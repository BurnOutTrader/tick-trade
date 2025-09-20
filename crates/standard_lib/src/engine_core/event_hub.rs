use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use crate::market_data::base_data::{Bbo, Candle, OrderBook, Tick};

/// EventHub: in-process pub/sub for market data.
///
/// Provides two kinds of topics for each feed type:
/// - **Global topics** (`*_all`) publish every event for all symbols.
/// - **Per-symbol topics** (`per_symbol_*`) publish only a single symbol.
///
/// Each topic exposes:
/// - a **broadcast** channel (`tx`) for streaming (fan-out, lossy under backpressure),
/// - a **snapshot** channel (`snap`, `tokio::watch`) that always holds the latest value
///   (or `None` if nothing has been published yet).
///
/// Use `publish_*` to emit events, `subscribe_*` for streams, and `snapshot_*` to
/// receive the latest value immediately (then updates).
#[allow(dead_code)]
#[derive(Clone)]
pub struct EventHub {
    // Per-symbol topics (concurrent)
    per_symbol_tick:   DashMap<String, Sender<Tick>>,
    per_symbol_bbo:    DashMap<String, Sender<Bbo>>,
    per_symbol_book:   DashMap<String, Sender<OrderBook>>,
    per_symbol_candle: DashMap<String, Sender<Candle>>,
}

#[allow(dead_code)]
impl Default for EventHub {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHub {
    /// Create a new `EventHub` with empty topic maps and default buffers.
    ///
    /// Global topics are pre-created; per-symbol topics are created lazily on
    /// first publish/subscribe. Default broadcast buffer is `1024` messages
    /// per topic (see `Topic::new(1024)`).
    pub fn new() -> Self {
        Self {
            per_symbol_tick:   DashMap::new(),
            per_symbol_bbo:    DashMap::new(),
            per_symbol_book:   DashMap::new(),
            per_symbol_candle: DashMap::new(),
        }
    }

    // -------- publishing (sync fast path) --------

    /// Publish a `Tick` to global and per-symbol topics.
    ///
    /// Sends to:
    /// - global broadcast (`tick_all.tx`) and snapshot (`tick_all.snap`),
    /// - symbol’s broadcast and snapshot, creating the symbol topic if missing.
    ///
    /// Note: publishing is fire-and-forget; send errors (e.g., no receivers)
    /// are ignored on purpose.
    #[inline]
    pub fn publish_tick(&self, v: Tick) {
        if let Some(tx) = self.per_symbol_tick.get(&v.symbol) {
            tx.send(v).ok();
        }
    }

    #[inline]
    /// Publish a `Bbo` (best bid/offer) to global and per-symbol topics.
    pub fn publish_bbo(&self, v: Bbo) {
        if let Some(tx) = self.per_symbol_bbo.get(&v.symbol) {
            tx.send(v).ok();
        }
    }

    #[inline]
    /// Publish an `OrderBook` snapshot to global and per-symbol topics.
    pub fn publish_book(&self, v: OrderBook) {
        if let Some(tx) = self.per_symbol_book.get(&v.symbol) {
            tx.send(v).ok();
        }
    }

    #[inline]
    /// Publish a `Candle` to global and per-symbol topics.
    pub fn publish_candle(&self, v: Candle) {
        if let Some(tx) = self.per_symbol_candle.get(&v.symbol) {
            tx.send(v).ok();
        }
    }

    // -------- subscriptions --------
    /// Subscribe to **ticks for a single symbol** (streaming).
    ///
    /// The per-symbol topic is created lazily if it does not exist yet.
    pub fn subscribe_tick_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<Tick> {
        if let Some(tx) = self.per_symbol_tick.get(sym) {
            return tx. subscribe();
        }
        let (tx, rx) = broadcast::channel(1024);
        self.per_symbol_tick.insert(sym.to_string(), tx);
        rx
    }

    /// Subscribe to **BBO for a single symbol** (streaming).
    pub fn subscribe_bbo_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<Bbo> {
        if let Some(tx) = self.per_symbol_bbo.get(sym) {
            return tx. subscribe();
        }
        let (tx, rx) = broadcast::channel(1024);
        self.per_symbol_bbo.insert(sym.to_string(), tx);
        rx
    }

    /// Subscribe to **order book snapshots for a single symbol** (streaming).
    pub fn subscribe_book_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<OrderBook> {
        if let Some(tx) = self.per_symbol_book.get(sym) {
            return tx. subscribe();
        }
        let (tx, rx) = broadcast::channel(50);
        self.per_symbol_book.insert(sym.to_string(), tx);
        rx
    }

    /// Subscribe to **candles for a single symbol** (streaming).
    pub fn subscribe_candle_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<Candle> {
        if let Some(tx) = self.per_symbol_candle.get(sym) {
            return tx. subscribe();
        }
        let (tx, rx) = broadcast::channel(50);
        self.per_symbol_candle.insert(sym.to_string(), tx);
        rx
    }
}

