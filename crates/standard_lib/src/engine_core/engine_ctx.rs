use std::sync::Arc;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::broadcast;
use crate::engine_core::clock::SimClock;
use crate::engine_core::data_events::{MdRouter, StrategyMode};
use crate::engine_core::strategy_engine::StrategyEngine;
use crate::engine_core::event_hub::EventHub;
use crate::engine_core::provider_resolver::SecurityResolver;
use crate::reality_models::models::ModelCatalog;
use crate::securities::registry::{RegistryEvent, SecurityRegistry};
use crate::securities::security::Security;
use crate::securities::symbols::{Exchange, SymbolId};


/// The **engine context** is the root container for all shared runtime
/// services and clocks. It owns:
///
/// - [`EventHub`] — process-wide pub/sub of *normalized* data & events.
/// - [`SecurityRegistry`] — canonical lookup + metadata store for instruments.
/// - [`MdRouter`] — deduped market-data subscription manager (one per mode).
/// - **Clocks** — a wall clock for live, a controllable clock for backtests.
/// - **Resolver** — optional on-demand `Security` hydrator (e.g., via vendor).
///
/// `EngineCtx` is intentionally mode-agnostic. Strategies should **not**
/// access it directly; instead, call [`EngineCtx::for_mode`] to obtain a
/// [`StrategyEngine`] that routes helpers (acquire, tick/bbo/book/candle
/// receivers, etc.) to the correct bus/clock for that mode.
///
/// ### When to use `EngineCtx`
/// - Composition/bootstrap: building live/backtest contexts, registering
///   providers on the right `MdRouter`, seeding the registry, etc.
/// - Infrastructure code: wiring services that strategies will use through
///   a mode-scoped [`StrategyEngine`].
///
/// ### When *not* to use `EngineCtx`
/// - Inside strategy logic. Always pass `StrategyEngine` to strategies;
///   it prevents accidental cross-mode mistakes and keeps the API compact.
///
/// ### Live vs Backtest
/// - **Live**: `live_clock` is used; subscriptions go through `live_bus`.
/// - **Backtest**: `sim_clock` is used; subscriptions go through `sim_bus`.
///   (Historical replay/simulator plugs behind `MdRouter` for BT.)
///
/// ### Resolver semantics
/// If `resolver` is present, [`ensure_security`] can hydrate unknown
/// instruments on demand and upsert them into the registry. If absent,
/// `ensure_security` returns an error for unknown symbols.
///
/// ### Example: build a live context and a `StrategyEngine`
/// ```ignore
/// let registry = Arc::new(SecurityRegistry::new());
/// let ctx = EngineCtx::live(registry);
/// // Register a provider on the SAME bus the ctx owns:
/// ctx.bus.register_and_connect(my_provider).await?;
///
/// // Hand a mode-scoped engine to strategies:
/// let engine = ctx.for_mode(StrategyMode::Live);
/// ```
///
/// ### Example: ensure a symbol exists before subscribing
/// ```ignore
/// let sec = ctx.ensure_security(&SymbolId("MNQ".into()), Exchange::CME).await?;
/// let eng = ctx.for_mode(StrategyMode::Live);
/// let (_g, bbo_rx, book_rx) = eng.base_feeds(&sec, FeedKind::BBO | FeedKind::BOOK, Some(10)).await?;
/// ```
///
/// ### Pitfalls
/// - Do not construct separate `EventHub`s for the same live runtime — vendors,
///   routers, and strategies must share **one** hub per context.
/// - Register providers on the `bus`,
/// - If you add a resolver *after* constructing a context, assign it to
///   `ctx.resolver` before calling `ensure_security`.
#[derive(Clone)]
pub struct EngineCtx {
    /// Global pub/sub hub for normalized ticks, quotes, books, candles, etc.
    pub hub: Arc<EventHub>,

    /// Canonical security registry (symbol→Security metadata).
    pub registry: Arc<SecurityRegistry>,

    /// Controllable clock for simulations/backtests.
    pub sim_clock:  Arc<SimClock>,

    /// Live market-data router (connects to real providers, dedupes subs).
    pub bus: Arc<MdRouter>,

    /// Optional on-demand `Security` loader (e.g., vendor API/cache).
    pub resolvers: DashMap<String, Arc<dyn SecurityResolver>>,

    /// Default model catalog available to strategies/tools.
    pub default_models: Arc<ModelCatalog>,
}

impl EngineCtx {
    /// Resolve a [`SymbolId`] into a concrete [`Security`] that already exists
    /// in the registry (or can be loaded via the optional `resolver`).
    ///
    /// Returns an error if the symbol cannot be found or hydrated.
    #[inline]
    pub async fn resolve(&self, vendor: &str, id: &SymbolId, exchange: Exchange) -> anyhow::Result<Arc<Security>> {
        self.ensure_security(vendor, id, exchange).await?;
        self.registry
            .get(id, &exchange)
            .ok_or_else(|| anyhow::anyhow!("unknown symbol {}", id.0))
    }

    /// Ensure a `Security` exists in the registry; if missing and a `resolver`
    /// is configured, it will be used to load and upsert the instrument.
    ///
    /// - With a resolver: unknown symbols can be lazily fetched and cached.
    /// - Without a resolver: unknown symbols produce an error.
    pub async fn ensure_security(&self, vendor: &str, id: &SymbolId, exchange: Exchange) -> anyhow::Result<Arc<Security>> {
        if let Some(sec) = self.registry.get(id, &exchange) {
            return Ok(sec);
        }
        if let Some(resolver_kvp) = self.resolvers.get(vendor) {
            match resolver_kvp.value().resolve(id, exchange).await? {
                Some(sec) => Ok(self.registry.upsert(sec)),
                None => Err(anyhow::anyhow!("resolver returned none for {}", id.0)),
            }
        } else {
            Err(anyhow::anyhow!("unknown symbol {} (no resolver configured)", id.0))
        }
    }

    /// Produce a **mode-scoped** context for strategies.
    ///
    /// The returned [`StrategyEngine`] routes all helpers (`acquire`,
    /// `tick_rx`, `bbo_rx`, `book_rx`, `candle_rx`, etc.) to the correct
    /// bus/clock for `mode`, so strategy code stays mode-agnostic.
    #[inline]
    pub fn for_mode(&self, mode: StrategyMode) -> StrategyEngine {
        StrategyEngine { inner: self.clone(), mode }
    }

    /// Live context with an attached resolver for on-demand security hydration.
    pub fn live(
        registry: Arc<SecurityRegistry>,
    ) -> Self {
        let hub = Arc::new(EventHub::new());
        Self {
            hub: hub.clone(),
            registry,
            sim_clock: Arc::new(SimClock::new(chrono::Utc::now())), // seed for tools/tests if needed
            bus: Arc::new(MdRouter::new(hub.clone())),
            resolvers: DashMap::new(),
            default_models: Arc::new(Default::default()),
        }
    }

    /// Backtest context with an attached resolver for on-demand security hydration.
    pub fn backtest(registry: Arc<SecurityRegistry>, start: DateTime<Utc>) -> Self {
        let hub = Arc::new(EventHub::new());
        Self {
            hub: hub.clone(),
            registry,
            sim_clock:  Arc::new(SimClock::new(start)),
            bus: Arc::new(MdRouter::new(hub.clone())),
            resolvers: DashMap::new(),
            default_models: Arc::new(Default::default()),
        }
    }

    pub fn add_resolver(&self, vendor: String, resolver: Arc<dyn SecurityResolver>) {
        self.resolvers.insert(vendor, resolver);
    }

    /// Subscribe to registry change events (add/update/remove).
    pub fn registry_events(&self) -> broadcast::Receiver<RegistryEvent> {
        self.registry.subscribe()
    }
}