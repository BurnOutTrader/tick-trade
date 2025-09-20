use std::{cmp::Ordering, collections::BinaryHeap, time::Duration};
use anyhow::Result;
use chrono::{DateTime, Utc};
use duckdb::Connection;
use tokio::time::sleep;

use standard_lib::market_data::base_data::{Tick, Candle, Bbo, OrderBook, Resolution};
use standard_lib::securities::symbols::Exchange;

use crate::queries::{
    get_ticks_in_range,
    get_candles_in_range,
    get_books_in_range,
    get_ticks_from_date_to_latest,
    get_candles_from_date_to_latest,
    get_latest_book,
};
use crate::duck::{latest_available, resolve_dataset_id};
use crate::models::DataKind;

/// ---- Helpers ----

#[inline]
fn ts_ns(dt: &DateTime<Utc>) -> i128 {
    (dt.timestamp() as i128) * 1_000_000_000i128 + (dt.timestamp_subsec_nanos() as i128)
}

/// Everything a source needs to implement to participate in the global replay.
trait ReplaySource {
    /// Earliest *next* timestamp this source can emit (None when exhausted).
    fn peek_ts(&self) -> Option<DateTime<Utc>>;
    /// Emit everything at `peek_ts()` into the hub and advance.
    async fn step(&mut self, hub: &EventHub) -> Result<()>;
    /// Human tag (for logs).
    fn name(&self) -> &str;
}

/// Your hub type (whatever module you use). Only methods used here must exist.
#[derive(Clone)]
pub struct EventHub {
    // keep your real fields
}
impl EventHub {
    pub fn publish_tick_batch(&self, _ts: DateTime<Utc>, _batch: &[Tick]) {}
    pub fn publish_candle_batch(&self, _ts: DateTime<Utc>, _batch: &[Candle]) {}
    pub fn publish_bbo_batch(&self, _ts: DateTime<Utc>, _batch: &[Bbo]) {}
    pub fn publish_book_batch(&self, _ts: DateTime<Utc>, _batch: &[OrderBook]) {}
}

/// One symbol-kind replayer that holds data in memory and feeds the hub step-by-step.
struct SymbolKindReplayer {
    tag: String, // e.g. "Rithmic MNQ Ticks"
    kind: DataKind,

    // Data buffered (already sorted) — we batch “all at same ts”
    ticks:   std::collections::VecDeque<Tick>,
    candles: std::collections::VecDeque<Candle>,
    bbo:     std::collections::VecDeque<Bbo>,
    books:   std::collections::VecDeque<OrderBook>,
}

impl SymbolKindReplayer {
    fn new_ticks(tag: String, rows: Vec<Tick>) -> Self {
        let mut rows = rows;
        rows.sort_by_key(|t| ts_ns(&t.time));
        Self { tag, kind: DataKind::Tick, ticks: rows.into(), candles: [].into(), bbo: [].into(), books: [].into() }
    }
    fn new_candles(tag: String, mut rows: Vec<Candle>) -> Self {
        // IMPORTANT: candles ordered by *end* time per your decision.
        rows.sort_by_key(|c| ts_ns(&c.time_end));
        Self { tag, kind: DataKind::Candle, ticks: [].into(), candles: rows.into(), bbo: [].into(), books: [].into() }
    }
    fn new_books(tag: String, mut rows: Vec<OrderBook>) -> Self {
        rows.sort_by_key(|b| ts_ns(&b.time));
        Self { tag, kind: DataKind::Book, ticks: [].into(), candles: [].into(), bbo: [].into(), books: rows.into() }
    }

    fn drain_eq_ts<T, G>(
        q: &mut std::collections::VecDeque<T>,
        ts: DateTime<Utc>,
        get_ts: G,
        out: &mut Vec<T>,
    ) where
        G: Fn(&T) -> DateTime<Utc>,
    {
        while let Some(front) = q.front() {
            if get_ts(front) == ts {
                out.push(q.pop_front().unwrap());
            } else {
                break;
            }
        }
    }
}

impl ReplaySource for SymbolKindReplayer {
    fn peek_ts(&self) -> Option<DateTime<Utc>> {
        match self.kind {
            DataKind::Tick   => self.ticks.front().map(|t| t.time),
            DataKind::Candle => self.candles.front().map(|c| c.time_end),
            DataKind::Book   => self.books.front().map(|b| b.time),
            DataKind::Bbo    => self.bbo.front().map(|q| q.time),
        }
    }

    async fn step(&mut self, hub: &EventHub) -> Result<()> {
        let Some(next_ts) = self.peek_ts() else { return Ok(()); };

        match self.kind {
            DataKind::Tick => {
                let mut batch = Vec::with_capacity(16);
                Self::drain_eq_ts(&mut self.ticks, next_ts, |t| t.time, &mut batch);
                if !batch.is_empty() { hub.publish_tick_batch(next_ts, &batch); }
            }
            DataKind::Candle => {
                let mut batch = Vec::with_capacity(16);
                Self::drain_eq_ts(&mut self.candles, next_ts, |c| c.time_end, &mut batch);
                if !batch.is_empty() { hub.publish_candle_batch(next_ts, &batch); }
            }
            DataKind::Book => {
                let mut batch = Vec::with_capacity(4);
                Self::drain_eq_ts(&mut self.books, next_ts, |b| b.time, &mut batch);
                if !batch.is_empty() { hub.publish_book_batch(next_ts, &batch); }
            }
            DataKind::Bbo => {
                let mut batch = Vec::with_capacity(16);
                Self::drain_eq_ts(&mut self.bbo, next_ts, |q| q.time, &mut batch);
                if !batch.is_empty() { hub.publish_bbo_batch(next_ts, &batch); }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str { &self.tag }
}

/// Min-heap entry (we invert with Reverse for BinaryHeap).
#[derive(Eq)]
struct QItem {
    ts: DateTime<Utc>,
    idx: usize, // index into sources vec
}
impl PartialEq for QItem { fn eq(&self, o: &Self) -> bool { self.ts == o.ts && self.idx == o.idx } }
impl Ord for QItem {
    fn cmp(&self, o: &Self) -> Ordering { // reverse for min-heap behavior via BinaryHeap
        o.ts.cmp(&self.ts).then_with(|| o.idx.cmp(&self.idx))
    }
}
impl PartialOrd for QItem {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) }
}

/// Coordinator: globally time-ordered replay across many sources
pub struct ReplayCoordinator {
    sources: Vec<Box<dyn ReplaySource + Send>>,
}

impl ReplayCoordinator {
    pub fn new() -> Self { Self { sources: Vec::new() } }
    pub fn add_source(&mut self, src: impl ReplaySource + Send + 'static) {
        self.sources.push(Box::new(src));
    }

    /// Run until all sources exhaust. For realism you can add a `pace` sleep between steps.
    pub async fn run(mut self, hub: EventHub, pace: Option<Duration>) -> Result<()> {
        let mut heap: BinaryHeap<QItem> = BinaryHeap::new();

        // seed heap
        for (i, s) in self.sources.iter().enumerate() {
            if let Some(ts) = s.peek_ts() {
                heap.push(QItem { ts, idx: i });
            }
        }

        while let Some(head) = heap.pop() {
            let target_ts = head.ts;

            // Collect all sources whose next event is at target_ts
            let mut batch_idxs = vec![head.idx];
            while let Some(next) = heap.peek() {
                if next.ts == target_ts {
                    let same = heap.pop().unwrap();
                    batch_idxs.push(same.idx);
                } else { break; }
            }

            // Step each of those sources once (emit their batch for this ts)
            for idx in batch_idxs {
                self.sources[idx].step(&hub).await?;
            }

            // Re-seed heap with updated peek_ts for each stepped source
            for idx in batch_idxs {
                if let Some(ts) = self.sources[idx].peek_ts() {
                    heap.push(QItem { ts, idx });
                }
            }

            if let Some(d) = pace { sleep(d).await; }
        }

        Ok(())
    }
}