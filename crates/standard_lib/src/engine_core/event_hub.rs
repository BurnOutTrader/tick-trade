use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::{broadcast, watch};
use crate::market_data::base_data::{Bbo, Candle, OrderBook, Tick};

#[derive(Clone)]
struct Topic<T: Clone + Send + 'static> {
    tx: broadcast::Sender<T>,
    snap: watch::Sender<Option<T>>, // last value (snapshot), optional
}

impl<T: Clone + Send + 'static> Topic<T> {
    fn new(cap: usize) -> Self {
        let (tx, _) = broadcast::channel(cap);
        let (snap, _) = watch::channel(None);
        Self { tx, snap }
    }
}

fn mk_topic<T: Clone + Send + 'static>() -> Topic<T> {
    Topic::new(2048)
}

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
    // Global topics (all symbols)
    tick_all:   Topic<Tick>,
    candle_all: Topic<Candle>,
    bbo_all:    Topic<Bbo>,
    book_all:   Topic<OrderBook>,

    // Per-symbol topics (concurrent)
    per_symbol_tick:   Arc<DashMap<String, Topic<Tick>>>,
    per_symbol_bbo:    Arc<DashMap<String, Topic<Bbo>>>,
    per_symbol_book:   Arc<DashMap<String, Topic<OrderBook>>>,
    per_symbol_candle: Arc<DashMap<String, Topic<Candle>>>,
}

#[allow(dead_code)]
impl EventHub {
    /// Create a new `EventHub` with empty topic maps and default buffers.
    ///
    /// Global topics are pre-created; per-symbol topics are created lazily on
    /// first publish/subscribe. Default broadcast buffer is `1024` messages
    /// per topic (see `Topic::new(1024)`).
    pub fn new() -> Self {
        Self {
            tick_all: mk_topic::<Tick>(),
            candle_all: mk_topic::<Candle>(),
            bbo_all: mk_topic::<Bbo>(),
            book_all: mk_topic::<OrderBook>(),
            per_symbol_tick:   Arc::new(DashMap::new()),
            per_symbol_bbo:    Arc::new(DashMap::new()),
            per_symbol_book:   Arc::new(DashMap::new()),
            per_symbol_candle: Arc::new(DashMap::new()),
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
    pub fn publish_tick(&self, v: Tick) {
        let _ = self.tick_all.tx.send(v.clone());
        let _ = self.tick_all.snap.send(Some(v.clone()));
        self.publish_symbol_topic(&self.per_symbol_tick, &v.symbol.clone(), v);
    }

    /// Publish a `Bbo` (best bid/offer) to global and per-symbol topics.
    pub fn publish_bbo(&self, v: Bbo) {
        let _ = self.bbo_all.tx.send(v.clone());
        let _ = self.bbo_all.snap.send(Some(v.clone()));
        self.publish_symbol_topic(&self.per_symbol_bbo, &v.symbol.clone(), v);
    }

    /// Publish an `OrderBook` snapshot to global and per-symbol topics.
    pub fn publish_book(&self, v: OrderBook) {
        let _ = self.book_all.tx.send(v.clone());
        let _ = self.book_all.snap.send(Some(v.clone()));
        self.publish_symbol_topic(&self.per_symbol_book, &v.symbol.clone(), v);
    }

    /// Publish a `Candle` to global and per-symbol topics.
    pub fn publish_candle(&self, v: Candle) {
        let _ = self.candle_all.tx.send(v.clone());
        let _ = self.candle_all.snap.send(Some(v.clone()));
        self.publish_symbol_topic(&self.per_symbol_candle, &v.symbol.clone(), v);
    }

    /// Internal helper: publish to a per-symbol topic, creating it on demand.
    ///
    /// Uses DashMap’s `entry` API to initialize a `Topic<T>` with a broadcast
    /// buffer of `1024` messages the first time a symbol is seen.
    fn publish_symbol_topic<T: Clone + Send + 'static>(
        &self,
        map: &DashMap<String, Topic<T>>,
        key: &str,
        v: T,
    ) {
        let entry = map.entry(key.to_string()).or_insert_with(|| Topic::new(1024));
        let tx = entry.tx.clone();
        let snap = entry.snap.clone();
        let _ = tx.send(v.clone());
        let _ = snap.send(Some(v));
    }

    // -------- subscriptions --------

    /// Subscribe to **all ticks** for all symbols (streaming).
    ///
    /// Uses a `tokio::broadcast::Receiver<Tick>`. Receivers must keep up; if they
    /// lag behind, older messages may be dropped.
    pub fn subscribe_tick_all(&self) -> tokio::sync::broadcast::Receiver<Tick> {
        self.tick_all.tx.subscribe()
    }

    /// Subscribe to **all BBOs** for all symbols (streaming).
    pub fn subscribe_bbo_all(&self) -> tokio::sync::broadcast::Receiver<Bbo> {
        self.bbo_all.tx.subscribe()
    }

    /// Subscribe to **all order books** for all symbols (streaming).
    pub fn subscribe_book_all(&self) -> tokio::sync::broadcast::Receiver<OrderBook> {
        self.book_all.tx.subscribe()
    }

    /// Subscribe to **all candles** for all symbols (streaming).
    pub fn subscribe_candle_all(&self) -> tokio::sync::broadcast::Receiver<Candle> {
        self.candle_all.tx.subscribe()
    }

    /// Subscribe to **ticks for a single symbol** (streaming).
    ///
    /// The per-symbol topic is created lazily if it does not exist yet.
    pub fn subscribe_tick_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<Tick> {
        self.per_symbol_tick
            .entry(sym.to_string())
            .or_insert_with(|| Topic::new(1024))
            .tx
            .subscribe()
    }

    /// Subscribe to **BBO for a single symbol** (streaming).
    pub fn subscribe_bbo_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<Bbo> {
        self.per_symbol_bbo
            .entry(sym.to_string())
            .or_insert_with(|| Topic::new(1024))
            .tx
            .subscribe()
    }

    /// Subscribe to **order book snapshots for a single symbol** (streaming).
    pub fn subscribe_book_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<OrderBook> {
        self.per_symbol_book
            .entry(sym.to_string())
            .or_insert_with(|| Topic::new(1024))
            .tx
            .subscribe()
    }

    /// Subscribe to **candles for a single symbol** (streaming).
    pub fn subscribe_candle_symbol(&self, sym: &str) -> tokio::sync::broadcast::Receiver<Candle> {
        self.per_symbol_candle
            .entry(sym.to_string())
            .or_insert_with(|| Topic::new(1024))
            .tx
            .subscribe()
    }

    /// Get a **snapshot receiver** for the latest tick across all symbols.
    ///
    /// Returns a `tokio::watch::Receiver<Option<Tick>>`:
    /// - immediately yields the current latest value (or `None` if none yet),
    /// - then yields subsequent updates.
    pub fn snapshot_tick_all(&self) -> tokio::sync::watch::Receiver<Option<Tick>> {
        self.tick_all.snap.subscribe()
    }

    /// Get a snapshot receiver for the latest BBO across all symbols.
    pub fn snapshot_bbo_all(&self) -> tokio::sync::watch::Receiver<Option<Bbo>> {
        self.bbo_all.snap.subscribe()
    }

    /// Get a snapshot receiver for the latest order book across all symbols.
    pub fn snapshot_book_all(&self) -> tokio::sync::watch::Receiver<Option<OrderBook>> {
        self.book_all.snap.subscribe()
    }

    /// Get a snapshot receiver for the latest candle across all symbols.
    pub fn snapshot_candle_all(&self) -> tokio::sync::watch::Receiver<Option<Candle>> {
        self.candle_all.snap.subscribe()
    }
}

