use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc, RwLock};
use std::sync::Arc;

use standard_lib::engine_core::strategy_engine::StrategyEngine;
use standard_lib::engine_core::strategy_manager::Strategy;
use standard_lib::engine_core::public_classes::StrategyEvent;
use standard_lib::engine_core::data_events::{FeedKind, StrategyMode};

use standard_lib::market_data::base_data::{Bbo, Candle, OrderBook, Resolution, Tick};
use standard_lib::market_data::market_by_order::{BookInterest, BookTrigger, MboActionMask, L3Book};

use standard_lib::securities::symbols::{Exchange, SymbolId};
use standard_lib::universes::models::UniversePlan;
use standard_lib::universes::inbuilt::r#static::plan_one_symbol;

#[allow(dead_code)]
pub struct L3ExampleStrategy {
    tasks: DashMap<String, tokio::task::JoinHandle<()>>,
}


#[async_trait]
impl Strategy for L3ExampleStrategy {
    fn mode(&self) -> StrategyMode { StrategyMode::Live }

    fn backtest_start(&self) -> DateTime<Utc> { Utc::now() } // unused in Live
    fn backtest_end(&self)   -> DateTime<Utc> { Utc::now() } // unused in Live

    fn universe_plans(&self) -> Vec<UniversePlan> {
        vec![
            plan_one_symbol(SymbolId("MNQ".into()), Exchange::CME, "Databento".into())
        ]
    }

    // Ask for exactly what you want. If Databento MBO is wired, the engine will derive BBO/BOOK/TICKS.
    fn feeds(&self) -> FeedKind {
        FeedKind::MBO | FeedKind::BBO | FeedKind::BOOK // add FeedKind::TICKS / ::CANDLES if you want them too
    }

    async fn run(&self, ctx: &StrategyEngine, mut rx: mpsc::Receiver<StrategyEvent>) -> anyhow::Result<()> {
        while let Some(evt) = rx.recv().await {
            match evt {
                StrategyEvent::SymbolAdded { id, sec, mut feeds, .. } => {
                    let sym_key = id.to_string();

                    // 1) Get the canonical L3 book handle
                    let Some(book) = ctx.book_handle(&sym_key, sec.exchange) else {
                        // If None, likely your provider/engine hasn’t attached an MBO sink yet
                        tracing::warn!("no L3 book yet for {sym_key}");
                        continue;
                    };

                    // 2) Register for L3 nudges — choose actions and (optionally) timer cadence
                    let interest = BookInterest {
                        actions: MboActionMask::ADD
                            | MboActionMask::MODIFY
                            | MboActionMask::CANCEL
                            | MboActionMask::TRADE
                            | MboActionMask::FILL,
                        // e.g., also get a periodic nudge every 50ms (engine emits if configured)
                        timer_interval_ns: Some(50_000_000),
                    };
                    let rx_mbo = ctx.register_book_interest(&sym_key, interest);

                    // 3) Optionally also listen to BBO/Tick/OrderBook/Candles from the hub (derived or vendor)
                    let rx_bbo      = feeds.bbo_rx.take();   // already subscribed by engine based on FeedKind
                    let rx_tick     = feeds.tick_rx.take();
                    let rx_book     = feeds.book_rx.take();
                    let rx_cndl_1s  = feeds.candle_rx.take(); // if you also asked for CANDLES

                    let handle = self.spawn_symbol_task(
                        ctx.clone(),
                        sym_key.clone(),
                        sec.exchange,
                        rx_mbo,
                        book,
                        rx_bbo,
                        rx_tick,
                        rx_book,
                        rx_cndl_1s,
                    );
                    self.tasks.insert(sym_key, handle);
                }

                StrategyEvent::SymbolRemoved { id, .. } => {
                    if let Some((_, h)) = self.tasks.remove(&id) { h.abort(); }
                }
            }
        }

        // shutdown cleanup
        for kv in self.tasks.iter() { kv.value().abort(); }
        self.tasks.clear();
        Ok(())
    }
}

#[allow(dead_code)]
impl L3ExampleStrategy {
    pub fn new() -> Self { Self { tasks: DashMap::new() } }

    fn spawn_symbol_task(
        &self,
        _ctx: StrategyEngine,
        sym_key: String,
        _exch: Exchange,
        mut rx_mbo: mpsc::UnboundedReceiver<BookTrigger>,
        book: Arc<RwLock<L3Book>>,
        mut rx_bbo: Option<broadcast::Receiver<Bbo>>,
        mut rx_tick: Option<broadcast::Receiver<Tick>>,
        mut rx_book: Option<broadcast::Receiver<OrderBook>>,
        mut rx_cndl_1s: Option<broadcast::Receiver<Candle>>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // === L3 nudges (primary driver) ===
                    Some(trig) = rx_mbo.recv() => {
                        match trig {
                            BookTrigger::OnMbo { action, side, price, size, order_id, ts_event, .. } => {
                                // read current top-of-book after the mutation
                                let g = book.read().await;
                                let snap = g.snapshot(1);
                                if !snap.bids.is_empty() && !snap.asks.is_empty() {
                                    let (b0p,b0s) = snap.bids[0].clone();
                                    let (a0p,a0s) = snap.asks[0].clone();
                                    tracing::debug!(
                                        target: "l3",
                                        "L3 {} {:?} side={:?} px={} sz={} oid={:?} -> bid0 {}({}) | ask0 {}({}) @{}",
                                        sym_key, action, side, price, size, order_id,
                                        b0p, b0s, a0p, a0s, ts_event.unwrap_or_else(Utc::now)
                                    );
                                }
                                // (your alpha logic here)
                            }
                            BookTrigger::OnTimerNs {   .. } => {
                                // periodic check (e.g., compute imbalance every 50ms)
                                let g = book.read().await;
                                let depth5 = g.snapshot(5);
                                // example: simple imbalance metric
                                let bid_sum: rust_decimal::Decimal = depth5.bids.iter().map(|(_,s)| *s).sum();
                                let ask_sum: rust_decimal::Decimal = depth5.asks.iter().map(|(_,s)| *s).sum();
                                tracing::trace!(
                                    target: "l3",
                                    "L3-TIMER {} I={}",
                                    sym_key,
                                    (bid_sum - ask_sum)
                                );
                            }
                        }
                    }

                    // === Optional: BBO (derived or vendor) ===
                    res = async { if let Some(rx)=rx_bbo.as_mut() { rx.recv().await.ok() } else { None } } => {
                        if let Some(bbo) = res {
                            tracing::trace!(target: "bbo", "BBO {} {} bid:{}({}) ask:{}({})",
                                bbo.symbol, bbo.time, bbo.bid, bbo.bid_size, bbo.ask, bbo.ask_size);
                        }
                    }

                    // === Optional: raw ticks ===
                    res = async { if let Some(rx)=rx_tick.as_mut() { rx.recv().await.ok() } else { None } } => {
                        if let Some(tk) = res {
                            tracing::trace!(target: "tick", "TICK {} {} p:{} v:{} side:{:?}",
                                tk.symbol, tk.time, tk.price, tk.size, tk.side);
                        }
                    }

                    // === Optional: aggregated book snapshots (if you asked for FeedKind::BOOK) ===
                    res = async { if let Some(rx)=rx_book.as_mut() { rx.recv().await.ok() } else { None } } => {
                        if let Some(ob) = res {
                            if !(ob.bids.is_empty() || ob.asks.is_empty()) {
                                tracing::trace!(target: "book", "BOOK {} bid0:{:?} ask0:{:?} @{}",
                                    ob.symbol, ob.bids[0], ob.asks[0], ob.time);
                            }
                        }
                    }

                    // === Optional: 1s candles ===
                    res = async { if let Some(rx)=rx_cndl_1s.as_mut() { rx.recv().await.ok() } else { None } } => {
                        if let Some(c) = res {
                            if matches!(c.resolution, Resolution::Seconds(1)) {
                                tracing::trace!(target: "cndl", "1s {} {}-{} O:{} H:{} L:{} C:{} V:{}",
                                    c.symbol, c.time_start, c.time_end, c.open, c.high, c.low, c.close, c.volume);
                            }
                        }
                    }
                }
            }
        })
    }
}
