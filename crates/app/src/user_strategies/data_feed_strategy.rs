use standard_lib::engine_core::strategy_engine::StrategyEngine;
use standard_lib::engine_core::data_events::{FeedKind, StrategyMode};
use standard_lib::market_data::base_data::{Bbo, Candle, OrderBook, Resolution, Tick};
use dashmap::DashMap;
use standard_lib::securities::symbols::{Exchange, SymbolId};
use standard_lib::universes::models::UniversePlan;
use standard_lib::universes::inbuilt::r#static::plan_one_symbol;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::{broadcast, mpsc};
use standard_lib::engine_core::public_classes::StrategyEvent;
use standard_lib::engine_core::strategy_manager::Strategy;

pub struct DataSubscriptionExampleStrategy {
    /// one JoinHandle per active symbol (no guards here—engine owns them)
    tasks: DashMap<String, tokio::task::JoinHandle<()>>,
}

impl DataSubscriptionExampleStrategy {
    pub fn new() -> Self {
        Self {
            tasks: DashMap::new(),
        }
    }

    fn spawn_symbol_task(
        &self,
        _context: &StrategyEngine,
        mut rx_book: Option<broadcast::Receiver<OrderBook>>,
        mut rx_cndl_1s: Option<broadcast::Receiver<Candle>>,
        mut rx_cndl_5m: Option<broadcast::Receiver<Candle>>, // we’ll optionally create this below
        mut rx_tick: Option<broadcast::Receiver<Tick>>,
        mut rx_bbo: Option<broadcast::Receiver<Bbo>>,
    ) -> tokio::task::JoinHandle<()> {
        tracing::info!("strategy: per-symbol task spawned");
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // 1s candles
                    res = async {
                        if let Some(rx) = rx_cndl_1s.as_mut() { rx.recv().await.ok() } else { None }
                    } => {
                        if let Some(cndl) = res {
                            if matches!(cndl.resolution, Resolution::Seconds(1)) {
                                tracing::info!(
                                    "Candle {}:  {} {}-{} O:{} H:{} L:{} C:{} V:{}",
                                    cndl.symbol, cndl.resolution, cndl.time_start, cndl.time_end,
                                    cndl.open, cndl.high, cndl.low, cndl.close, cndl.volume
                                );
                            }
                        }
                    }

                    // 5m candles (if we wired them)
                    res = async {
                        if let Some(rx) = rx_cndl_5m.as_mut() { rx.recv().await.ok() } else { None }
                    } => {
                        if let Some(cndl) = res {
                            if matches!(cndl.resolution, Resolution::Minutes(5)) {
                                tracing::info!(
                                    "5m {} {}-{} O:{} H:{} L:{} C:{} V:{}",
                                    cndl.symbol, cndl.time_start, cndl.time_end,
                                    cndl.open, cndl.high, cndl.low, cndl.close, cndl.volume
                                );
                            }
                        }
                    }

                    // Order book (top of book logging)
                    res = async {
                        if let Some(rx) = rx_book.as_mut() { rx.recv().await.ok() } else { None }
                    } => {
                        if let Some(book) = res {
                            if !(book.bids.is_empty() || book.asks.is_empty()) {
                                tracing::info!(
                                    "BOOK {} bid0:{:?} ask0:{:?} @{}",
                                    book.symbol, book.bids[0], book.asks[0], book.time
                                );
                            }
                        }
                    }

                    // Ticks
                    res = async {
                        if let Some(rx) = rx_tick.as_mut() { rx.recv().await.ok() } else { None }
                    } => {
                        if let Some(tk) = res {
                            tracing::info!("TICK {} {} p:{} v:{}", tk.symbol, tk.time, tk.price, tk.size);
                        }
                    }

                    // BBO
                    res = async {
                        if let Some(rx) = rx_bbo.as_mut() { rx.recv().await.ok() } else { None }
                    } => {
                        if let Some(qq) = res {
                            tracing::info!(
                                "BBO {} {} bid:{}({}) ask:{}({})",
                                qq.symbol, qq.time, qq.bid, qq.bid_size, qq.ask, qq.ask_size
                            );
                        }
                    }
                }
            }
        })
    }
}

#[async_trait]
impl Strategy for DataSubscriptionExampleStrategy {
    fn mode(&self) -> StrategyMode {
        StrategyMode::Live
    }

    fn backtest_start(&self) -> DateTime<Utc> {
        todo!()
    }

    fn backtest_end(&self) -> DateTime<Utc> {
        todo!()
    }

    fn universe_plans(&self) -> Vec<UniversePlan> {
        vec![plan_one_symbol(SymbolId("MNQ".to_string()), Exchange::CME, "Rithmic".to_string())]
    }

    fn feeds(&self) -> FeedKind {
        FeedKind::CANDLES | FeedKind::BBO
    }

    /// One long-lived runner per (strategy, mode). The engine holds guards and
    /// sends us StrategyEvents with ready-to-use receivers.
    async fn run(&self, ctx: &StrategyEngine, mut rx: mpsc::Receiver<StrategyEvent>) -> anyhow::Result<()> {
        // If the engine only subscribes 1s candles, we can roll them up to 5m here.
        // If the engine already provides 5m candles, skip the roll-up.
        // We’ll detect candle presence on each SymbolAdded.

        while let Some(evt) = rx.recv().await {
            match evt {
                StrategyEvent::SymbolAdded { id, sec, mut feeds, .. } => {
                    // ensure we at least have 1s candles, ticks, BBO/BOOK depending on your engine policy
                    // If you *don’t* have a 5m candle receiver, build one by rolling up 1s.
                    let mut rx_cndl_5m: Option<broadcast::Receiver<Candle>> = None;

                    if feeds.candle_rx.is_some() {
                        // Example: derive a 5m stream from whatever the engine provided (assume 1s)
                        // Requires your existing helper; adjust module path:
                        let sym = feeds.vcode.clone();
                        let hub = ctx.hub(); // Arc<EventHub>

                        // Source: a fresh candle receiver (we don't want to consume feeds.candle_rx twice)
                        let rx_cndl_src = hub.subscribe_candle_symbol(&sym);

                        standard_lib::market_data::consolidators::spawn_candles_to_candles(
                            Resolution::Minutes(5),
                            rx_cndl_src,
                            sym.clone(),
                            sec.exchange,
                            hub.clone(),
                            None,
                        );

                        // Our worker will listen to the consolidated 5m from the hub:
                        rx_cndl_5m = Some(hub.subscribe_candle_symbol(&sym));
                    }

                    // Spawn the per-symbol worker (no guards captured — engine keeps them)
                    let handle = self.spawn_symbol_task(
                        ctx,
                        feeds.book_rx.take(),
                        feeds.candle_rx.take(), // 1s (or whatever engine gave)
                        rx_cndl_5m,             // optional 5m
                        feeds.tick_rx.take(),
                        feeds.bbo_rx.take(),
                    );

                    self.tasks.insert(id.to_string(), handle);
                }

                StrategyEvent::SymbolRemoved { id, .. } => {
                    if let Some((_k, h)) = self.tasks.remove(&id).map(|kv| (kv.0, kv.1)) {
                        h.abort(); // receivers will close because engine dropped guards
                    }
                }
            }
        }

        // shutdown: abort any leftover workers
        for kv in self.tasks.iter() {
            kv.value().abort();
        }
        self.tasks.clear();

        Ok(())
    }
}