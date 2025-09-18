//! tick-trade — core runtime engine
//!
//! Goals:
//! 1) Run multiple strategies concurrently
//! 2) No duplicate live subscriptions (global dedupe via MdBus)
//! 3) Strategies share base feeds (tick/BBO/book)
//! 4) Simple, tidy strategy launch API
use std::sync::Arc;
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use tokio::sync::{broadcast, mpsc, RwLock};
use crate::engine_core::data_events::SubGuard;
use crate::engine_core::engine_ctx::EngineCtx;
use crate::market_data::base_data::{Bbo, Candle, OrderBook, Resolution, Tick};
use crate::engine_core::event_hub::EventHub;
use crate::engine_core::public_classes::{FeedKind, MarketDataRequest, StrategyMode};
use crate::market_data::market_by_order::{BookInterest, BookTrigger, L3Book};
use crate::securities::registry::{RegistryEvent, SecurityRegistry};
use crate::securities::security::Security;
use crate::securities::symbols::Exchange;

/// ========================================================
/// StrategyEngine
/// ========================================================
///
/// A **mode-scoped view** of [`EngineCtx`]. Everything here is already
/// bound to a specific [`StrategyMode`] (Live or Backtest), so strategies
/// don’t need to worry about which bus/clock to use.
///
/// ### What it does
/// - Routes subscriptions to the **right MdRouter** for this mode.
/// - Exposes ergonomic **feed receivers** (ticks, BBO, book, candles).
/// - Provides **RAII subscription** helpers that return a [`SubGuard`]
///   alongside the matching receiver (e.g., `base_feeds`, `tick_feed`).
/// - Exposes the **correct clock** for this mode (`clock()`).
/// - Can auto-hydrate missing `Security` via `ensure_security()` if a resolver is configured.
///
/// ### What it avoids
/// - No vendor-specific wiring in strategies (policies live in requests).
/// - No direct access to `live_bus` / `sim_bus`.
/// - No global mutable state; everything is scoped to this mode.
///
/// ### Typical use in a strategy
/// 1) Resolve a symbol to `Security` via [`resolve`]
/// 2) Acquire deduped subs via [`acquire`] or helpers like [`base_feeds`]
/// 3) Consume broadcast receivers (tick/BBO/book/candles) in your task
///
/// ### Threading model
/// - All helpers are **cheap** to clone and `Send + Sync`.
/// - Subscription dedupe happens under the hood via [`MdRouter`].
///
/// ### When *not* to use it
/// - System composition or cross-strategy orchestration belongs in the
///   engine layer, not inside individual strategies.
#[derive(Clone)]
pub struct StrategyEngine {
    pub(crate) inner: EngineCtx,
    pub(crate) mode: StrategyMode,
}

impl StrategyEngine {
    /// Returns the current mode (Live or Backtest).
    #[inline]
    pub fn mode(&self) -> StrategyMode { self.mode }

    /// Returns a handle to the global event hub (pub/sub).
    #[inline]
    pub fn hub(&self) -> Arc<EventHub> { self.inner.hub.clone() }

    pub fn book_handle(&self, symbol: &str, exchange: Exchange) ->  Option<Arc<RwLock<L3Book>>> {
        self.inner.bus.book_handle(symbol, exchange)
    }

    pub fn register_book_interest(
        &self,
        symbol: &str,
        interest: BookInterest,
    ) -> mpsc::UnboundedReceiver<BookTrigger> {
        self.inner.bus.register_book_interest(symbol, interest)
    }
    /// Returns the security registry for symbol lookup.
    #[inline]
    pub fn registry(&self) -> Arc<SecurityRegistry> { self.inner.registry.clone() }

    /// Subscribe to registry change events (read-only).
    #[inline]
    pub fn registry_events(&self) -> broadcast::Receiver<RegistryEvent> {
        self.inner.registry.subscribe()
    }

    #[allow(dead_code)]
    /// Register or update a security (only if your design allows strategies to onboard instruments).
    #[inline]
    fn register_security(&self, sec: Security) -> Arc<Security> {
        self.inner.registry.upsert(sec)
    }

    // --- Convenience helpers: subscribe to normalized streams ---
    #[inline] fn tick_rx(&self, sym: &str)   -> tokio::sync::broadcast::Receiver<Tick>      { self.inner.hub.subscribe_tick_symbol(sym) }
    #[inline] fn bbo_rx(&self, sym: &str)    -> tokio::sync::broadcast::Receiver<Bbo>       { self.inner.hub.subscribe_bbo_symbol(sym) }
    #[inline] fn book_rx(&self, sym: &str)   -> tokio::sync::broadcast::Receiver<OrderBook> { self.inner.hub.subscribe_book_symbol(sym) }
    #[inline] fn candle_rx(&self, sym: &str) -> tokio::sync::broadcast::Receiver<Candle>    { self.inner.hub.subscribe_candle_symbol(sym) }

    /// Subscribe shared **base feeds** (BBO + OrderBook).
    /// Returns an RAII guard + receivers.
    pub async fn base_feeds(
        &self,
        vendor: String,
        sec: &Arc<Security>,
        kinds: FeedKind,
        book_depth: Option<u8>,
    ) -> anyhow::Result<(SubGuard,tokio::sync::broadcast::Receiver<Bbo>, tokio::sync::broadcast::Receiver<OrderBook>)> {
        let guard = self.inner.bus.acquire(MarketDataRequest {
            symbol:sec.id.to_string(),
            exchange: sec.exchange,
            kinds,
            book_depth,
            resolution: None,
            vendor,
        }).await?;

        Ok((guard, self.bbo_rx(&sec.id.to_string(),), self.book_rx(&sec.id.to_string())))
    }

    /// Subscribe only to **ticks**.
    /// Returns an RAII guard + receiver.
    pub async fn tick_feed(
        &self,
        vendor: String,
        sec: &Arc<Security>,
    ) -> anyhow::Result<(SubGuard, tokio::sync::broadcast::Receiver<Tick>)>
    {
        let guard = self.inner.bus.acquire(MarketDataRequest {
            symbol:sec.id.to_string(),
            exchange: sec.exchange,
            kinds: FeedKind::TICKS,
            book_depth: None,
            resolution: None,
            vendor,
        }).await?;

        Ok((guard, self.tick_rx(&sec.id.to_string())))
    }

    pub async fn candle_feed(
        &self,
        vendor: String,
        sec: &Arc<Security>,
        res: Resolution,
    ) -> anyhow::Result<(SubGuard, tokio::sync::broadcast::Receiver<Candle>)> {
        let guard = self.inner.bus.acquire(MarketDataRequest {
            symbol:sec.id.to_string(),
            exchange: sec.exchange,
            kinds: FeedKind::CANDLES,
            book_depth: None,
            resolution: Some(res),
            vendor,
        }).await?;

        Ok((guard, self.candle_rx(&sec.id.to_string())))
    }

    #[inline]
    pub fn time_utc(&self) -> DateTime<Utc> {
        match self.mode {
            StrategyMode::Live     => Utc::now(),
            StrategyMode::Backtest => self.inner.sim_clock.now(), // ← the per-backtest clock
        }
    }

    #[inline]
    pub fn time_local(&self, tz: &Tz) -> DateTime<Tz> {
        tz.from_utc_datetime(&self.time_utc().naive_utc())
    }
}





