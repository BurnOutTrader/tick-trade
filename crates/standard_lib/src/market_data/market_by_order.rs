use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use chrono::{DateTime, TimeZone, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use crate::market_data::base_data::{Bbo, OrderBook, Side, Tick};
use crate::securities::symbols::Exchange;

bitflags::bitflags! {
    #[derive(Copy, Clone, PartialEq, Eq, Hash)]
    pub struct MboActionMask: u32 {
        const ADD    = 1<<0;
        const CANCEL = 1<<1;
        const MODIFY = 1<<2;
        const CLEAR  = 1<<3;
        const TRADE  = 1<<4;
        const FILL   = 1<<5;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BookTrigger {
    /// Fire when an MBO action of interest occurs (properties below).
    OnMbo {
        action: MboAction,
        side: Side,
        price: Decimal,
        size: u32,
        order_id: Option<u128>,
        // carry through all vendor timing/quality fields:
        ts_event: Option<DateTime<Utc>>,
        ts_recv:  Option<DateTime<Utc>>,
        venue_seq: Option<u32>,
        channel_id: Option<u8>,
        flags: Option<u8>,
    },
    /// Time-based nudge (fixed cadence) in **ns**; timestamps are from the book.
    OnTimerNs {
        now_event: Option<DateTime<Utc>>, // last ME time known
        now_recv:  Option<DateTime<Utc>>, // last capture/recv time known
        interval_ns: u64,
    },
}
/// Per-order L3 action (Databento: Add, Cancel, Modify, cleaR book, Trade, Fill).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MboAction {
    Add,
    Cancel,
    Modify,
    Clear,    // Databento uses 'R' for cleaR book
    Trade,    // trade without explicit order_id fill (venue style)
    Fill,     // explicit fill against order_id
    None,
}

impl MboAction {
    /// Map Databento action byte/char to `MboAction`.
    #[inline]
    pub fn from_db_char(c: u8) -> Self {
        match c {
            b'A' => MboAction::Add,
            b'C' => MboAction::Cancel,
            b'M' => MboAction::Modify,
            b'R' => MboAction::Clear,
            b'T' => MboAction::Trade,
            b'F' => MboAction::Fill,
            _    => MboAction::None,
        }
    }
}

/// Wrap the raw flags byte; add convenience methods as you confirm bit layout.
/// Keeping raw gives you forward-compatibility across venues.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MboFlags(pub u8);

impl MboFlags {
    #[inline] pub fn raw(&self) -> u8 { self.0 }

    // Example placeholders (adjust once you confirm bit semantics):
    // pub const END_OF_EVENT: u8 = 1 << 0;
    // #[inline] pub fn end_of_event(&self) -> bool { self.0 & Self::END_OF_EVENT != 0 }
}

/// Vendor-agnostic L3 event (MBO) — single order-centric mutation.
#[derive(Clone, Debug)]
pub struct MboEvent {
    // ---- Identity / routing ----
    pub symbol: String,
    pub exchange: Exchange,

    // ---- Event core ----
    pub action: MboAction,     // Add/Cancel/Modify/Clear/Trade/Fill/None
    pub side: Side,            // Bid/Ask/None (reuses your Side enum)
    pub price: Decimal,        // scaled from 1e-9 (Databento int64)
    pub size: u32,             // order quantity for this event
    pub order_id: Option<u128>,// venue order id (Databento u64; stored as u128)

    // ---- Sequencing / channels / quality ----
    pub channel_id: Option<u8>,// Databento incremental channel id
    pub venue_seq:  Option<u32>,// venue-assigned sequence (gap detection)
    pub publisher_id: Option<u16>, // dataset/venue id (Databento)
    pub instrument_id: Option<u32>, // numeric instrument id (Databento)
    pub flags: Option<MboFlags>, // raw flags byte
    pub ts_in_delta_ns: Option<i32>, // ME send time delta (ns before ts_recv)

    // ---- Timing ----
    /// Matching-engine-received time (preferred for causality).
    pub ts_event: Option<DateTime<Utc>>,
    /// Capture/server receive time (good for QoS/latency).
    pub ts_recv:  Option<DateTime<Utc>>,
}

impl MboEvent {
    /// Convenience: pick the best timestamp for ordering when both exist.
    #[inline]
    pub fn best_time(&self) -> DateTime<Utc> {
        self.ts_event.or(self.ts_recv).unwrap_or_else(Utc::now)
    }

    /// True if this event likely affects the book state (most do, except None).
    #[inline]
    pub fn is_book_mutation(&self) -> bool {
        !matches!(self.action, MboAction::None)
    }

    /// True if this event represents a trade/fill you might want to convert to `Tick`.
    #[inline]
    pub fn is_execution(&self) -> bool {
        matches!(self.action, MboAction::Trade | MboAction::Fill)
    }
}

// ---------- Databento utilities ----------

#[allow(dead_code)]
/// Convert Databento `int64` price with scale 1e-9 → Decimal.
#[inline]
pub fn dec_from_1e9(v: i64) -> Decimal {
    Decimal::from_i128_with_scale(v as i128, 9)
}

#[allow(dead_code)]
/// Convert nanoseconds since UNIX epoch → `DateTime<Utc>`.
#[inline]
pub fn utc_from_ns(ns: u64) -> DateTime<Utc> {
    let secs = (ns / 1_000_000_000) as i64;
    let nsec = (ns % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nsec).single().unwrap_or_else(Utc::now)
}

#[allow(dead_code)]
/// Minimal Databento MBO row to map from (mirror their fields).
/// Wire your actual decoder to produce this, or adapt this impl to your type.
#[derive(Clone, Debug)]
pub struct DbMboRow {
    pub ts_recv: u64,       // ns since epoch
    pub ts_event: u64,      // ns since epoch
    pub rtype: u8,          // should be 160 for MBO
    pub publisher_id: u16,
    pub instrument_id: u32,
    pub action: u8,         // 'A','C','M','R','T','F'
    pub side: u8,           // 'B','A', or 'N'
    pub price: i64,         // 1e-9 scale
    pub size: u32,
    pub channel_id: u8,
    pub order_id: u64,
    pub flags: u8,
    pub ts_in_delta: i32,   // ns before ts_recv
    pub sequence: u32,
}

#[allow(dead_code)]
/// Map a Databento MBO row → normalized `MboEvent`.
#[inline]
pub fn map_db_mbo_row(
    row: &DbMboRow,
    symbol: String,
    exchange: Exchange,
) -> MboEvent {
    let action = MboAction::from_db_char(row.action);
    let side = match row.side {
        b'B' => Side::Buy,
        b'A' => Side::Sell,
        _    => Side::None,
    };

    MboEvent {
        symbol,
        exchange,
        action,
        side,
        price: dec_from_1e9(row.price),
        size: row.size,
        order_id: Some(row.order_id as u128),

        channel_id: Some(row.channel_id),
        venue_seq: Some(row.sequence),
        publisher_id: Some(row.publisher_id),
        instrument_id: Some(row.instrument_id),
        flags: Some(MboFlags(row.flags)),
        ts_in_delta_ns: Some(row.ts_in_delta),

        ts_event: Some(utc_from_ns(row.ts_event)),
        ts_recv:  Some(utc_from_ns(row.ts_recv)),
    }
}

// ---------- Optional adapters into your existing types ----------

/// Convert executions (Trade/Fill) into your `Tick` type.
/// Returns `None` for non-execution actions.
#[inline]
pub fn tick_from_mbo(e: &MboEvent) -> Option<Tick> {
    if !e.is_execution() { return None; }

    Some(Tick {
        symbol:   e.symbol.clone(),
        exchange: e.exchange,
        price:    e.price,
        size:     Decimal::from(e.size),
        time:     e.best_time(),
        side:     e.side.clone(),             // aggressor side when present

        exec_id:  None,               // DB MBO row doesn't carry exec_id; add if available
        maker_order_id: e.order_id.map(|id| id.to_string()),
        taker_order_id: None,         // populate if you infer aggressor ID from venue feed
        venue_seq: e.venue_seq,

        ts_event: e.ts_event,
        ts_recv:  e.ts_recv,
    })
}



// ----- Internal model for L3Book -----

#[derive(Clone, Debug, Default)]
struct PriceLevel {
    total_size: u64,                       // aggregated size at this price
    orders: HashMap<u128, u32>,            // order_id -> size
}

impl PriceLevel {
    #[inline]
    fn add_order(&mut self, order_id: u128, sz: u32) {
        let prev = self.orders.insert(order_id, sz).unwrap_or(0);
        self.total_size = self.total_size.saturating_sub(prev as u64)
            .saturating_add(sz as u64);
    }

    #[inline]
    fn modify_order(&mut self, order_id: u128, new_sz: u32) {
        if let Some(prev) = self.orders.get_mut(&order_id) {
            let prev_sz = *prev as u64;
            *prev = new_sz;
            let new_sz_u64 = new_sz as u64;
            if new_sz_u64 >= prev_sz {
                self.total_size = self.total_size.saturating_add(new_sz_u64 - prev_sz);
            } else {
                self.total_size = self.total_size.saturating_sub(prev_sz - new_sz_u64);
            }
        } else {
            // If a modify arrives without prior add, treat as add (defensive).
            self.add_order(order_id, new_sz);
        }
    }

    #[inline]
    fn reduce_fill(&mut self, order_id: u128, by: u32) {
        if let Some(sz) = self.orders.get_mut(&order_id) {
            let prev = *sz as u64;
            let by_u64 = by as u64;
            let new_sz = prev.saturating_sub(by_u64);
            *sz = new_sz as u32;
            self.total_size = self.total_size.saturating_sub(by_u64);
            if *sz == 0 {
                self.orders.remove(&order_id);
            }
        }
    }

    #[inline]
    fn remove_order(&mut self, order_id: u128) {
        if let Some(prev) = self.orders.remove(&order_id) {
            self.total_size = self.total_size.saturating_sub(prev as u64);
        }
    }

    #[inline]
    fn reduce_aggregate(&mut self, by: u64) {
        // Used for trade messages without order_id (aggregate reduction).
        let by = by.min(self.total_size);
        self.total_size -= by;
        if self.total_size == 0 {
            self.orders.clear();
        }
    }

    #[inline]
    fn order_count(&self) -> u32 { self.orders.len() as u32 }

    #[inline]
    fn is_empty(&self) -> bool { self.total_size == 0 || self.orders.is_empty() }
}

#[derive(Clone, Debug)]
pub struct L3Book {
    pub symbol: String,
    pub exchange: Exchange,

    // asks: ascending by price
    asks: BTreeMap<Decimal, PriceLevel>,
    // bids: descending by price using Reverse
    bids: BTreeMap<Reverse<Decimal>, PriceLevel>,

    // order index for fast lookup on modify/cancel/fill
    // maps order_id -> (Side, price)
    index: HashMap<u128, (Side, Decimal)>,

    last_ts_event: Option<DateTime<Utc>>,
    last_ts_recv:  Option<DateTime<Utc>>,
}

impl L3Book {
    pub fn new(symbol: String, exchange: Exchange) -> Self {
        Self {
            symbol,
            exchange,
            asks: BTreeMap::new(),
            bids: BTreeMap::new(),
            index: HashMap::new(),
            last_ts_event: None,
            last_ts_recv:  None,
        }
    }

    #[inline]
    fn touch_time(&mut self, e: &MboEvent) {
        if let Some(te) = e.ts_event { self.last_ts_event = Some(te); }
        if let Some(tr) = e.ts_recv  { self.last_ts_recv  = Some(tr); }
    }

    pub fn apply(&mut self, e: &MboEvent) -> Option<Bbo> {
        self.touch_time(e); // <-- update last times first
        let before = self.current_top();
        match e.action {
            MboAction::Clear => {
                self.asks.clear();
                self.bids.clear();
                self.index.clear();
            }

            MboAction::Add => {
                if let Some(oid) = e.order_id {
                    self.add_order(e.side, e.price, oid, e.size);
                }
            }

            MboAction::Modify => {
                if let Some(oid) = e.order_id {
                    self.modify_order(e.side, e.price, oid, e.size);
                }
            }

            MboAction::Cancel => {
                if let Some(oid) = e.order_id {
                    self.cancel_order(oid);
                }
            }

            MboAction::Fill => {
                if let Some(oid) = e.order_id {
                    self.fill_order(oid, e.size);
                } else {
                    // Fallback: aggregate reduction on resting side if no order_id.
                    self.aggregate_trade(e.side, e.price, e.size);
                }
            }

            MboAction::Trade => {
                // Trades reduce the RESTING side (opposite aggressor).
                self.aggregate_trade(e.side, e.price, e.size);
            }

            MboAction::None => { /* ignore */ }
        }

        // Clean empty levels after mutation
        self.prune_empty_level(e.side, e.price);

        // after mutate:
        let after = self.current_top();
        if before != after {
            let (bid_px, bid_sz, bid_ct) = after.0;
            let (ask_px, ask_sz, ask_ct) = after.1;
            return Some(Bbo {
                symbol:   self.symbol.clone(),
                exchange: self.exchange,
                bid:      bid_px.unwrap_or_default(),
                bid_size: Decimal::from(bid_sz.unwrap_or(0)),
                ask:      ask_px.unwrap_or_default(),
                ask_size: Decimal::from(ask_sz.unwrap_or(0)),
                // use event times (prefer ME time), never Utc::now
                time:     e.ts_event.or(self.last_ts_event).or(self.last_ts_recv).unwrap(),
                bid_orders: bid_ct.map(|c| c as u32),
                ask_orders: ask_ct.map(|c| c as u32),
                venue_seq:  e.venue_seq,
                is_snapshot: Some(false),
            });
        }
        None
    }


    /// Materialize a price-aggregated snapshot up to `depth`.
    pub fn snapshot(&self, depth: usize) -> OrderBook {
        let bids = self.iter_bids().take(depth)
            .map(|(p, lvl)| (p, Decimal::from(lvl.total_size)))
            .collect::<Vec<_>>();

        let asks = self.iter_asks().take(depth)
            .map(|(p, lvl)| (p, Decimal::from(lvl.total_size)))
            .collect::<Vec<_>>();

        // Use "now" for snapshot time; if you prefer last event time, store it in the book.
        let time = Utc::now();

        OrderBook {
            symbol:   self.symbol.clone(),
            exchange: self.exchange,
            bids,
            asks,
            time,
        }
    }

    // ----- Internals -----

    #[inline]
    fn add_order(&mut self, side: Side, price: Decimal, oid: u128, size: u32) {
        match side {
            Side::Buy => {
                let lvl = self.bids.entry(Reverse(price)).or_default();
                lvl.add_order(oid, size);
                self.index.insert(oid, (Side::Buy, price));
            }
            Side::Sell => {
                let lvl = self.asks.entry(price).or_default();
                lvl.add_order(oid, size);
                self.index.insert(oid, (Side::Sell, price));
            }
            Side::None => { /* ignore */ }
        }
    }

    #[inline]
    fn modify_order(&mut self, side_hint: Side, new_price: Decimal, oid: u128, new_size: u32) {
        if let Some((side, old_price)) = self.index.get(&oid).cloned() {
            // If price changes, move order across levels.
            if old_price != new_price {
                self.remove_from_level(side, old_price, oid);
                self.add_order(side, new_price, oid, new_size);
            } else {
                // In-place size change.
                match side {
                    Side::Buy => {
                        if let Some(lvl) = self.bids.get_mut(&Reverse(old_price)) {
                            lvl.modify_order(oid, new_size);
                        } else {
                            // Level missing: treat as add
                            self.add_order(Side::Buy, old_price, oid, new_size);
                        }
                    }
                    Side::Sell => {
                        if let Some(lvl) = self.asks.get_mut(&old_price) {
                            lvl.modify_order(oid, new_size);
                        } else {
                            self.add_order(Side::Sell, old_price, oid, new_size);
                        }
                    }
                    Side::None => {}
                }
            }
        } else {
            // Missing index (out-of-order modify): fall back to side hint if provided
            if !matches!(side_hint, Side::None) {
                self.add_order(side_hint, new_price, oid, new_size);
            }
        }
    }

    #[inline]
    fn cancel_order(&mut self, oid: u128) {
        if let Some((side, price)) = self.index.remove(&oid) {
            self.remove_from_level(side, price, oid);
        }
    }

    #[inline]
    fn fill_order(&mut self, oid: u128, by: u32) {
        if let Some((side, price)) = self.index.get(&oid).cloned() {
            match side {
                Side::Buy => {
                    if let Some(lvl) = self.bids.get_mut(&Reverse(price)) {
                        lvl.reduce_fill(oid, by);
                        if lvl.orders.get(&oid).is_none() {
                            // fully filled → remove from index
                            self.index.remove(&oid);
                        }
                        if lvl.is_empty() {
                            self.bids.remove(&Reverse(price));
                        }
                    }
                }
                Side::Sell => {
                    if let Some(lvl) = self.asks.get_mut(&price) {
                        lvl.reduce_fill(oid, by);
                        if lvl.orders.get(&oid).is_none() {
                            self.index.remove(&oid);
                        }
                        if lvl.is_empty() {
                            self.asks.remove(&price);
                        }
                    }
                }
                Side::None => {}
            }
        }
    }

    /// Aggregate trade without an order_id.
    /// Reduces the RESTING side (opposite the aggressor) at `price` by `size`.
    #[inline]
    fn aggregate_trade(&mut self, aggressor: Side, price: Decimal, size: u32) {
        let reduce_by = size as u64;
        match aggressor {
            Side::Buy => { // buy aggressor consumes ASK
                if let Some(lvl) = self.asks.get_mut(&price) {
                    lvl.reduce_aggregate(reduce_by);
                    if lvl.is_empty() {
                        self.asks.remove(&price);
                    }
                }
            }
            Side::Sell => { // sell aggressor consumes BID
                if let Some(lvl) = self.bids.get_mut(&Reverse(price)) {
                    lvl.reduce_aggregate(reduce_by);
                    if lvl.is_empty() {
                        self.bids.remove(&Reverse(price));
                    }
                }
            }
            Side::None => { /* unknown; conservative: do nothing */ }
        }
    }

    #[inline]
    fn remove_from_level(&mut self, side: Side, price: Decimal, oid: u128) {
        match side {
            Side::Buy => {
                if let Some(lvl) = self.bids.get_mut(&Reverse(price)) {
                    lvl.remove_order(oid);
                    if lvl.is_empty() {
                        self.bids.remove(&Reverse(price));
                    }
                }
            }
            Side::Sell => {
                if let Some(lvl) = self.asks.get_mut(&price) {
                    lvl.remove_order(oid);
                    if lvl.is_empty() {
                        self.asks.remove(&price);
                    }
                }
            }
            Side::None => {}
        }
    }

    #[inline]
    fn prune_empty_level(&mut self, side_hint: Side, price: Decimal) {
        match side_hint {
            Side::Buy => {
                if let Some(lvl) = self.bids.get(&Reverse(price)) {
                    if lvl.is_empty() {
                        self.bids.remove(&Reverse(price));
                    }
                }
            }
            Side::Sell => {
                if let Some(lvl) = self.asks.get(&price) {
                    if lvl.is_empty() {
                        self.asks.remove(&price);
                    }
                }
            }
            Side::None => { /* skip */ }
        }
    }

    /// Returns ((bid_px, bid_size, bid_orders), (ask_px, ask_size, ask_orders))
    #[inline]
    fn current_top(&self) -> ( (Option<Decimal>, Option<u64>, Option<u32>),
                               (Option<Decimal>, Option<u64>, Option<u32>) )
    {
        let best_bid = self.bids.first_key_value().map(|(k, lvl)| (k.0, lvl.total_size, lvl.order_count()));
        let best_ask = self.asks.first_key_value().map(|(p, lvl)| (*p,  lvl.total_size, lvl.order_count()));
        (
            best_bid.map(|(p, sz, ct)| (Some(p), Some(sz), Some(ct))).unwrap_or((None, None, None)),
            best_ask.map(|(p, sz, ct)| (Some(p), Some(sz), Some(ct))).unwrap_or((None, None, None)),
        )
    }

    #[inline]
    fn iter_bids(&self) -> impl Iterator<Item = (Decimal, &PriceLevel)> {
        self.bids.iter().map(|(rev_p, lvl)| (rev_p.0, lvl))
    }

    #[inline]
    fn iter_asks(&self) -> impl Iterator<Item = (Decimal, &PriceLevel)> {
        self.asks.iter().map(|(p, lvl)| (*p, lvl))
    }
}

use tokio::sync::{mpsc, RwLock};
use crate::engine_core::data_events::FeedKind;
use crate::engine_core::event_hub::EventHub;

#[derive(Clone)]
pub struct BookInterest {
    pub actions: MboActionMask,   // which MBO actions to receive
    pub timer_interval_ns: Option<u64>, // also receive periodic timer nudges
}

struct Sub {
    mask: MboActionMask,
    tx: mpsc::UnboundedSender<BookTrigger>,
}

pub struct BookNudgeBus {
    // symbol -> list of subscribers with masks
    subs: DashMap<String, Vec<Sub>>,
}

impl BookNudgeBus {
    pub fn new() -> Self { Self { subs: DashMap::new() } }

    /// Register interest; returns a receiver for nudges.
    pub fn register(
        &self,
        symbol: &str,
        interest: BookInterest,
    ) -> mpsc::UnboundedReceiver<BookTrigger> {
        let (tx, rx) = mpsc::unbounded_channel();
        let sub = Sub { mask: interest.actions, tx };
        self.subs.entry(symbol.to_string())
            .or_default()
            .push(sub);
        rx
    }

    /// Push an MBO-triggered nudge to subscribers whose mask matches.
    pub fn emit_mbo(&self, symbol: &str, trig: BookTrigger) {
        if let Some(list) = self.subs.get(symbol) {
            let (act_mask, ok) = match &trig {
                BookTrigger::OnMbo { action, .. } => {
                    let m = match action {
                        MboAction::Add    => MboActionMask::ADD,
                        MboAction::Cancel => MboActionMask::CANCEL,
                        MboAction::Modify => MboActionMask::MODIFY,
                        MboAction::Clear  => MboActionMask::CLEAR,
                        MboAction::Trade  => MboActionMask::TRADE,
                        MboAction::Fill   => MboActionMask::FILL,
                        MboAction::None   => MboActionMask::empty(),
                    };
                    (m, true)
                }
                _ => (MboActionMask::empty(), false),
            };
            if ok {
                for s in list.iter() {
                    if s.mask.intersects(act_mask) {
                        let _ = s.tx.send(trig.clone());
                    }
                }
            }
        }
    }

    /// Push a timer nudge to **all** subs that asked for timers (provider handles cadence).
    pub fn emit_timer(&self, symbol: &str, trig: BookTrigger) {
        if let Some(list) = self.subs.get(symbol) {
            for s in list.iter() {
                // strategies choose to listen or not; we simply deliver timer nudges
                let _ = s.tx.send(trig.clone());
            }
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct SymXchg { sym: String, ex: Exchange }
impl SymXchg {
    fn new(sym: &str, ex: Exchange) -> Self { Self { sym: sym.to_string(), ex } }
}

pub struct BookService {
    // canonical live books per symbol+exchange
    books: DashMap<SymXchg, Arc<RwLock<L3Book>>>,
    // event hub + nudge bus for fan-out
    hub: Arc<EventHub>,
    nudges: Arc<BookNudgeBus>,
}

impl BookService {
    pub fn new(hub: Arc<EventHub>, nudges: Arc<BookNudgeBus>) -> Self {
        Self { books: DashMap::new(), hub, nudges }
    }

    /// Get/create the canonical book.
    pub fn ensure_book(&self, symbol: &str, exchange: Exchange) -> Arc<RwLock<L3Book>> {
        let key = SymXchg::new(symbol, exchange);
        self.books
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(L3Book::new(symbol.to_string(), exchange))))
            .clone()
    }

    /// Optional: let strategies pull the canonical book (no provider dependency).
    pub fn book_handle(&self, symbol: &str, exchange: Exchange) -> Option<Arc<RwLock<L3Book>>> {
        self.books.get(&SymXchg::new(symbol, exchange)).map(|e| e.clone())
    }

    /// Create a per-symbol raw MBO sink. Providers push raw MBO here.
    ///
    /// `kinds` controls which **derived** streams we publish (Tick/Bbo/Book).
    /// Strategies can independently register nudges via BookNudgeBus.
    pub fn make_mbo_sink(
        self: &Arc<Self>,
        symbol: String,
        exchange: Exchange,
        kinds: FeedKind,
        book_snapshot_depth: usize,        // e.g. 10 or 20
        snapshot_every: std::time::Duration // e.g. 250ms
    ) -> mpsc::UnboundedSender<MboEvent> {
        let (tx, mut rx) = mpsc::unbounded_channel::<MboEvent>();
        let this = Arc::clone(self);

        // ensure the canonical book exists
        let book = this.ensure_book(&symbol, exchange);

        // spawn one task per (symbol, exchange)
        tokio::spawn(async move {
            let mut last_snap_ns: i64 = 0;

            while let Some(evt) = rx.recv().await {
                // 1) Optionally publish Tick on executions
                if kinds.intersects(FeedKind::TICKS) && evt.is_execution() {
                    if let Some(t) = tick_from_mbo(&evt) {
                        this.hub.publish_tick(t);
                    }
                }

                // 2) Apply to canonical book; maybe publish BBO
                let bbo_opt = {
                    let mut g = book.write().await;
                    g.apply(&evt)
                };
                if kinds.intersects(FeedKind::BBO) {
                    if let Some(bbo) = bbo_opt { this.hub.publish_bbo(bbo); }
                }

                // 3) Throttled depth snapshots if requested
                if kinds.intersects(FeedKind::BOOK) {
                    let now_ns = evt.best_time().timestamp_nanos_opt().unwrap_or(0);
                    if now_ns - last_snap_ns >= snapshot_every.as_nanos() as i64 {
                        last_snap_ns = now_ns;
                        let snap = book.read().await.snapshot(book_snapshot_depth);
                        this.hub.publish_book(snap);
                    }
                }

                // 4) Always emit **filtered nudges** (strategies decide via mask)
                this.nudges.emit_mbo(&symbol, BookTrigger::OnMbo {
                    action: evt.action,
                    side: evt.side,
                    price: evt.price,
                    size: evt.size,
                    order_id: evt.order_id,
                    ts_event: evt.ts_event,
                    ts_recv:  evt.ts_recv,
                    venue_seq: evt.venue_seq,
                    channel_id: evt.channel_id,
                    flags: evt.flags.map(|f| f.raw()),
                });
            }
        });

        tx
    }
}