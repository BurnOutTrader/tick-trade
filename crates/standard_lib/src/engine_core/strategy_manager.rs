use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use chrono::{DateTime, Utc};
use crate::engine_core::data_events::{FeedKind, StrategyMode, SubGuard};
use crate::engine_core::engine_ctx::EngineCtx;
use crate::engine_core::strategy_engine::StrategyEngine;
use crate::engine_core::public_classes::{Feeds, StrategyEvent};
use crate::market_data::base_data::Resolution;
use crate::securities::registry::SecurityRegistry;
use crate::securities::symbols::SymbolId;
use crate::universes::models::{SelectionDiff, SelectionSchedule, UniverseHandle, UniversePlan};
use uuid::Uuid;
use std::hash::Hash;
use crate::engine_core::api_traits::MarketDataProvider;

/// **Strategy Trait**
///
/// Contract every trading strategy must implement in order to run inside
/// the engine. The engine drives strategies in a consistent way across
/// **live trading** and **backtests**, abstracting away vendors and feeds.
///
/// ### Core responsibilities
/// - Declare **which mode** the strategy runs in (`Live` or `Backtest`).
/// - Provide **universe plans** (rules for symbol selection and cadence).
/// - Specify **feed kinds** (e.g. ticks, book, candles) that should be
///   delivered for each selected symbol.
/// - In backtests: declare the **start/end dates** of the run.
/// - Implement the long-running `run()` method, which consumes events
///   and contains the strategy’s business logic.
///
/// ### Lifecycle
/// 1. The engine registers the strategy via [`StrategyManager::add_strategy_instance`].
/// 2. Universe plans are scheduled; on each `step()` the engine computes
///    add/remove diffs.
/// 3. For each added symbol, the engine:
///    - Subscribes to vendor feeds (deduped via [`MdRouter`]).
///    - Installs guards into the runtime.
///    - Sends a `StrategyEvent::SymbolAdded` containing fresh receivers.
/// 4. The strategy’s `run()` task receives events (`SymbolAdded`, `SymbolRemoved`,
///    and custom ones) on its mailbox channel and reacts accordingly.
/// 5. On symbol removal, guards are dropped and upstream unsubscribes occur.
///
/// ### Required methods
/// - [`mode`] — Returns the execution mode (`Live` or `Backtest`).
/// - [`backtest_start`] / [`backtest_end`] — Define bounds for simulation runs.
///   (Ignored in live mode).
/// - [`universe_plans`] — List of `UniversePlan`s (Static, Manual, EveryDayAtHour, etc.)
///   that decide what symbols the strategy trades.
/// - [`feeds`] — Bitmask of [`FeedKind`]s to automatically subscribe for each symbol.
/// - [`run`] — The main async loop. Receives [`StrategyEvent`] messages and drives
///   strategy logic. Should not return until shutdown.
///
/// ### Threading model
/// - The engine spawns **one task per strategy instance** (`run()`).
/// - Per-symbol work may be fanned out into additional tasks, but the
///   strategy is responsible for managing them.
/// - All data delivery is via broadcast receivers scoped to the strategy’s
///   universe.
///
/// ### Example
/// ```ignore
/// #[async_trait::async_trait]
/// impl Strategy for MyStrat {
///     fn mode(&self) -> StrategyMode { StrategyMode::Live }
///     fn backtest_start(&self) -> DateTime<Utc> { Utc.ymd(2024, 1, 1).and_hms(0,0,0) }
///     fn backtest_end(&self) -> DateTime<Utc> { Utc.ymd(2024, 12, 31).and_hms(0,0,0) }
///     fn universe_plans(&self) -> Vec<UniversePlan> { vec![plan_one_symbol("MNQ", Exchange::CME)] }
///     fn feeds(&self) -> FeedKind { FeedKind::TICKS | FeedKind::BBO }
///     async fn run(&self, engine: &StrategyEngine, mut rx: mpsc::Receiver<StrategyEvent>) -> anyhow::Result<()> {
///         while let Some(event) = rx.recv().await {
///             match event {
///                 StrategyEvent::SymbolAdded { id, feeds, .. } => { /* attach to feeds */ }
///                 StrategyEvent::SymbolRemoved { id, .. } => { /* cleanup */ }
///                 _ => {}
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Strategy: Send + Sync + 'static {
    fn mode(&self) -> StrategyMode;
    fn backtest_start(&self) -> DateTime<chrono::Utc>;
    fn backtest_end(&self) -> DateTime<chrono::Utc>;
    /// The engine will apply these plans to your universe.
    fn universe_plans(&self) -> Vec<UniversePlan>;
    /// The engine will automatically subscribe these feeds on symbol updates and return the receivers via a StrategyMessage::SymbolAdded.
    fn feeds(&self) -> FeedKind;
    /// One runner per (strategy, mode). Never returns until shutdown.
    async fn run(&self, engine: &StrategyEngine, rx: mpsc::Receiver<StrategyEvent>) -> anyhow::Result<()>;
    fn set_warmup_complete(&self) {}
}


/// Orchestrates strategies across one **Live** context and any number of **Backtest** runs.
///
/// # What it owns
/// - One [`EngineCtx`] per run keyed by [`CtxKey`] (`Live` or `Backtest(RunId)`).
/// - Optional backtest metadata for each run ([`BtMeta`]).
/// - Static strategy configs ([`StrategyEntry`]) and active per-strategy runtimes
///   ([`StratRuntime`]) which hold mailboxes and engine-owned subscription guards.
/// - A canonical [`SecurityRegistry`] used to seed new contexts.
///
/// # Responsibilities
/// - **Provider wiring (Live):** `register_live_provider` ensures a single live
///   context exists, attaches/updates an optional [`SecurityResolver`], and
///   connects the provider to that context’s `MdRouter`.
/// - **Strategy lifecycle:** `add_strategy_instance` creates/reuses the correct
///   context (Live or a fresh Backtest run), spawns the strategy runner pinned to
///   that context, and records the runtime mailbox/guards.
/// - **Universe scheduling:** `step()` evaluates each strategy’s compiled
///   universe `PlanState`s on the *context’s* clock (wall for live, sim for BT),
///   diffs symbol membership, and calls `apply_diff` to start/stop vendor
///   subscriptions accordingly.
/// - **Guard management:** Subscriptions are deduplicated by `MdRouter` and kept
///   alive by **engine-owned** [`SubGuard`]s stored in the runtime’s `per_symbol`.
///   Strategies only receive broadcast receivers; they do not own the vendor guards.
///
/// # Concurrency model
/// - The manager runs on one task (your main event loop) and spawns one runner
///   task per `(strategy, ctx)` with a bounded mailbox (`mpsc::Sender<StrategyEvent>`).
/// - Feed subscriptions fan out via the shared [`EventHub`] inside the target
///   context; strategies receive cheap `broadcast::Receiver`s.
///
/// # Typical Live boot sequence
/// 1) `let mut sm = StrategyManager::new();`
/// 2) `let hub = sm.ensure_live_ctx(None).hub.clone();` (build provider with this hub)
/// 3) `sm.register_live_provider(provider, Some(resolver)).await?;`
/// 4) `sm.add_strategy_instance("MyStrat", strat, None).await?;`
/// 5) Drive: `loop { sm.step().await?; }`
///
/// # Invariants
/// - Exactly one *Live* context (shared hub) regardless of provider count.
/// - Each backtest gets its own context (own sim clock and MdRouter).
/// - Strategies must be mode-consistent with their assigned context.
/// - If a resolver is later provided for an existing Live context, it is attached.
pub struct StrategyManager {
    // one EngineCtx per Live or Backtest run
    ctxs: HashMap<CtxKey, EngineCtx>,
    // optional BT metadata per run
    bt_meta: HashMap<CtxKey, BtMeta>,

    // registered strategies (static config)
    strategies: Vec<StrategyEntry>,

    // running mailboxes/guards by (strategy, mode, ctxKey)
    runtimes: HashMap<(String, StrategyMode, CtxKey), StratRuntime>,

    // a canonical SecurityRegistry to seed contexts (or you can pass per add)
    registry: Arc<SecurityRegistry>,
}

impl StrategyManager {
    /// Create an empty manager with a fresh, canonical [`SecurityRegistry`].
    ///
    /// This registry is cloned into any new contexts created later (Live or
    /// Backtest). If you need a pre-seeded registry, upsert into
    /// `manager.registry` before adding strategies/providers.
    pub fn new() -> Self {
        let registry = Arc::new(SecurityRegistry::new());
        let mut ctxs = HashMap::new();
        ctxs.insert(CtxKey::Live, EngineCtx::live(registry.clone()));
        Self {
            ctxs,
            bt_meta: HashMap::new(),
            strategies: Vec::new(),
            runtimes: HashMap::new(),
            registry,
        }
    }

    /// Register and connect a **live** market data provider against the shared
    /// Live context. Creates the Live context on demand if missing.
    ///
    /// - If `resolver` is provided and the Live context already exists, the
    ///   resolver is **attached/updated** (`ctx.resolver = Some(...)`).
    /// - The provider is `ensure_connected()` by the underlying `MdRouter`
    ///   and becomes available for subsequent `acquire()` calls.
    ///
    /// # Errors
    /// Returns an error if the provider cannot connect or register.
    pub async fn register_provider(
        &mut self,
        provider: Arc<dyn MarketDataProvider>,
    ) -> anyhow::Result<()> {
        // ensure/create LIVE context first
        let ctx = self.ctxs.entry(CtxKey::Live).or_insert_with(|| {
            EngineCtx::live(self.registry.clone())
        });
        ctx.add_resolver(provider.md_vendor_name().to_string(), provider.clone().resolver());

        ctx.bus.register_and_connect(provider).await?;
        Ok(())
    }

    /// Ensure the **Live** context exists and return a reference to it.
    ///
    /// Use this to obtain the **one** hub you should pass to your provider’s
    /// constructor so that vendor → hub → strategies all share the same bus.
    ///
    /// If `resolver` is provided and a new context is created, the context
    /// will be initialized with it.
    pub fn get_live_ctx(&self) -> &EngineCtx {
        self.ctxs.get(&CtxKey::Live).expect("Live context must exist")
    }

    /// Register a strategy instance and spawn its runner bound to the correct
    /// context, based on `strat.mode()`:
    ///
    /// - **Live:** Reuses the shared Live context (providers should be registered
    ///   beforehand via `register_live_provider`).
    /// - **Backtest:** Creates a **new** context keyed by a fresh [`RunId`],
    ///   initializes its sim clock at `strat.backtest_start()`, and records
    ///   the backtest window in [`BtMeta`].
    ///
    /// Returns the [`CtxKey`] of the context the strategy was bound to.
    ///
    /// # Notes
    /// - The strategy is handed a `StrategyEngine` view of the selected
    ///   context (routes subs to the correct `MdRouter`, exposes the right clock).
    /// - Universe plans are compiled immediately; actual selection happens in `step()`.
    pub async fn add_strategy_instance(
        &mut self,
        name: &str,
        strat: Arc<dyn Strategy>,
    ) -> anyhow::Result<CtxKey> {
        let mode = strat.mode();
        let (ctx_key, ctx_ref) = match mode {
            StrategyMode::Live => {
                // Ensure live context exists (providers should be registered separately)
                let ctx = self.ctxs.entry(CtxKey::Live).or_insert_with(|| {
                    EngineCtx::live(self.registry.clone())
                });
                (CtxKey::Live, ctx.clone())
            }
            StrategyMode::Backtest => {
                let run = RunId::new();
                let key = CtxKey::Backtest(run);
                let start = strat.backtest_start();
                let end   = strat.backtest_end();

                let ctx = self.ctxs.entry(key.clone()).or_insert_with(|| {
                    EngineCtx::backtest(self.registry.clone(), start)
                });
                self.bt_meta.insert(key.clone(), BtMeta { id: run, start, end });
                (key, ctx.clone())
            }
        };

        // Prepare strategy entry config
        let entry = StrategyEntry {
            name: name.to_string(),
            mode,
            strat: strat.clone(),
            plans: compile_plans(strat.universe_plans()),
            feeds: strat.feeds(),
            ctx_key: ctx_key.clone(),
        };
        self.strategies.push(entry);

        // Spawn runner pinned to this context
        let (tx, rx) = mpsc::channel::<StrategyEvent>(1024);
        let ctxm = ctx_ref.for_mode(mode);
        let sname = name.to_string();
        let handle = tokio::spawn(async move {
            if let Err(e) = strat.run(&ctxm, rx).await {
                tracing::error!(strategy=%sname, ?mode, error=%e, "strategy runner exited with error");
            }
        });

        self.runtimes.insert(
            (name.to_string(), mode, ctx_key.clone()),
            StratRuntime { tx, handle, per_symbol: HashMap::new() }
        );

        Ok(ctx_key)
    }

    /// Advance all strategies using their **context’s** notion of time:
    /// - Live: `system clock`
    /// - Backtest: simulated clock from `EngineCtx.sim_clock`
    ///
    /// For each strategy:
    /// - Evaluate due universe plans (`PlanState`) and compute add/remove diffs.
    /// - `apply_diff` will:
    ///   - Resolve securities via `ensure_security` (uses resolver when present).
    ///   - Acquire vendor subscriptions per the strategy’s `feeds` bitmask
    ///     (`FeedKind`), holding **engine-owned** guards in the runtime.
    ///   - Send fresh broadcast receivers to the strategy via `StrategyEvent::SymbolAdded`.
    ///
    /// Idempotent per tick; does nothing if no plans are due or no diffs occur.
    pub async fn step(&mut self) -> anyhow::Result<()> {
        // Build a work queue of diffs per (strategy, mode, ctx_key)
        let mut work: Vec<(String, StrategyMode, CtxKey, SelectionDiff, FeedKind)> = Vec::new();

        for entry in &mut self.strategies {
            // find the right clock (ctx is chosen by ctx_key)
            let ctx = self.ctxs.get(&entry.ctx_key).expect("ctx must exist");
            let now_mode = match entry.mode {
                StrategyMode::Live => Utc::now(),
                StrategyMode::Backtest => ctx.sim_clock.now(),
            };

            for plan in &mut entry.plans {
                if !Self::due(plan, now_mode) { continue; }

                let diff = plan.handle.run_selection(now_mode);
                if !diff.added.is_empty() || !diff.removed.is_empty() {
                    work.push((
                        entry.name.clone(),
                        entry.mode,
                        entry.ctx_key.clone(),
                        diff,
                        entry.feeds.clone(),
                    ));
                }
                plan.next_at = Self::schedule_next(plan, now_mode);
            }
        }

        // Apply diffs using the correct StrategyEngine per entry.ctx_key
        for (sname, mode, ctx_key, diff, feeds) in work {
            let ctx = self.ctxs.get(&ctx_key).expect("ctx must exist").clone();
            let ctxm = ctx.for_mode(mode);
            self.apply_diff(&ctxm, &sname, mode, ctx_key, diff, feeds).await;
        }

        Ok(())
    }

    /// Apply a single universe diff for a specific `(strategy, mode, ctx)` triple.
    ///
    /// - Acquires only the feeds requested by the strategy’s `feeds` bitmask:
    ///   - `BBO|BOOK` → base order-book feed (with optional depth).
    ///   - `TICKS`    → trades/ticks.
    ///   - `CANDLES`  → time-bar stream (policy chooses resolution; 1m by default).
    /// - Stores the returned [`SubGuard`]s in the runtime’s `per_symbol` map to
    ///   keep vendor subscriptions alive as long as the symbol is selected.
    /// - Sends `StrategyEvent::SymbolAdded` to the runner with **fresh**
    ///   broadcast receivers tapping the shared [`EventHub`].
    /// - On removal, drops the guards (allowing upstream unsubscribe) and
    ///   sends `StrategyEvent::SymbolRemoved`.
    ///
    /// Safe to call repeatedly; `MdRouter` handles dedupe/refcounting.
    async fn apply_diff(
        &mut self,
        ctxm: &StrategyEngine,
        sname: &str,
        mode: StrategyMode,
        ctx_key: CtxKey,
        diff: SelectionDiff,
        mask: FeedKind,
    ) {
        let Some(rt) = self.runtimes.get_mut(&(sname.to_string(), mode, ctx_key)) else { return; };

        let want_base  = mask.intersects(FeedKind::BBO | FeedKind::BOOK);
        let want_ticks = mask.contains(FeedKind::TICKS);
        let want_cndl  = mask.contains(FeedKind::CANDLES);
        let candle_res: Option<Resolution> = Some(Resolution::Seconds(1));

        for (vendor, id, exchange) in diff.added {
            let Ok(sec) = ctxm.inner.ensure_security(&vendor, &id, exchange).await else { continue; };
            let v = match sec.vendor("RITHMIC") {
                Some(v) => v.clone(),
                None => { tracing::warn!(?id, ?exchange, "no RITHMIC mapping"); continue; }
            };
            tracing::info!(
                strategy=%sname,
                symbol=?id,
                exchange=?exchange,
                preferred_vendor=%vendor,
                "acquiring feeds for symbol"
            );

            // Base
            let (base_guard, bbo_rx, book_rx) = if want_base {
                match ctxm.base_feeds(vendor.clone(), &sec, FeedKind::BBO | FeedKind::BOOK, Some(10)).await {
                    Ok((g, bbo, book)) => (Some(g), Some(bbo), Some(book)),
                    Err(e) => { tracing::warn!(?id, ?exchange, error=%e, "base_feeds failed"); (None, None, None) }
                }
            } else { (None, None, None) };

            // Ticks
            let (tick_guard, tick_rx) = if want_ticks {
                match ctxm.tick_feed(vendor.clone(), &sec).await {
                    Ok((g, rx)) => (Some(g), Some(rx)),
                    Err(e) => { tracing::warn!(?id, ?exchange, vendor=%vendor, error=%e, "tick_feed failed"); (None, None) }
                }
            } else { (None, None) };

            // Candles
            let (candle_guard, candle_rx) = if want_cndl {
                if let Some(res) = candle_res {
                    match ctxm.candle_feed(vendor.clone(), &sec, res).await {
                        Ok((g, rx)) => (Some(g), Some(rx)),
                        Err(e) => { tracing::warn!(?id, ?exchange, vendor=%vendor, ?res, error=%e, "candle_feed failed"); (None, None) }
                    }
                } else { (None, None) }
            } else { (None, None) };

            rt.per_symbol.insert(
                id.clone(),
                GuardSet { base: base_guard, tick: tick_guard, candle: candle_guard },
            );

            let feeds = Feeds { vcode: v.code.clone(), tick_rx, bbo_rx, book_rx, candle_rx };
            let _ = rt.tx.send(StrategyEvent::SymbolAdded { id: id.to_string(), exchange, sec: sec.clone(), feeds }).await;
        }

        for (_vendor,id, exchange) in diff.removed {
            let _ = rt.tx.send(StrategyEvent::SymbolRemoved { id: id.to_string(), exchange }).await;
            rt.per_symbol.remove(&id);
        }
    }

    // ---- scheduling helpers ----
    /// Returns `true` if a plan is due to run at `now_mode`.
    ///
    /// Semantics:
    /// - `Never` → only runs once (when `next_at` is `None`).
    /// - Others  → due when `now_mode >= next_at` (or immediately if `None`).
    #[inline]
    fn due(plan: &PlanState, now_mode: DateTime<Utc>) -> bool {
        match plan.schedule {
            SelectionSchedule::Never => plan.next_at.is_none(),
            _ => match plan.next_at { None => true, Some(t) => now_mode >= t },
        }
    }

    /// Compute next planned run time for a plan evaluated at `now_mode`.
    ///
    /// - `Every(d)`           → `now + d`
    /// - `OnBar(res)`         → next UTC bar boundary for `res`
    /// - `EveryDayAtHour{..}` → next local-time occurrence (DST-safe)
    /// - `Manual`/`Never`     → `None`
    fn schedule_next(plan: &PlanState, now_mode: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match plan.schedule {
            SelectionSchedule::Every(d) => Some(now_mode + d),
            SelectionSchedule::OnBar(res) => Some(Self::next_bar_boundary(now_mode, res)),
            SelectionSchedule::EveryDayAtHour { hour, time_zone } => Some(Self::next_daily_at(hour, time_zone, now_mode)),
            SelectionSchedule::Manual | SelectionSchedule::Never => None,
        }
    }


    /// DST-safe “next HH:00” in a given `tz`, returned in UTC.
    fn next_daily_at(hour: u8, tz: chrono_tz::Tz, now_utc: DateTime<Utc>) -> DateTime<Utc> {
        use chrono::{Datelike, LocalResult, TimeZone};
        let now_local = now_utc.with_timezone(&tz);
        let (y, m, d) = (now_local.year(), now_local.month(), now_local.day());
        let pick = |res: LocalResult<DateTime<_>>| match res {
            LocalResult::Single(t) => (t > now_local).then_some(t),
            LocalResult::Ambiguous(a, b) => [a, b].into_iter().filter(|t| *t > now_local).min(),
            LocalResult::None => None,
        };
        if let Some(t) = pick(tz.with_ymd_and_hms(y, m, d, hour as u32, 0, 0)) { return t.with_timezone(&Utc); }
        let tmr = now_local + chrono::Duration::days(1);
        let (y2, m2, d2) = (tmr.year(), tmr.month(), tmr.day());
        match tz.with_ymd_and_hms(y2, m2, d2, hour as u32, 0, 0) {
            LocalResult::Single(t) => t.with_timezone(&Utc),
            LocalResult::Ambiguous(a, b) => a.min(b).with_timezone(&Utc),
            LocalResult::None => tz.with_ymd_and_hms(y2, m2, d2, (hour as u32).saturating_add(1), 0, 0).earliest().unwrap().with_timezone(&Utc),
        }
    }

    /// Next UTC bar boundary for common time-based resolutions.
    fn next_bar_boundary(now: DateTime<Utc>, res: Resolution) -> DateTime<Utc> {
        use chrono::TimeZone;
        let secs = now.timestamp();
        let step = match res {
            Resolution::Seconds(s) => s as i64,
            Resolution::Minutes(m) => (m as i64) * 60,
            Resolution::Hours(h)   => (h as i64) * 3600,
            Resolution::Daily      => 24*3600,
            Resolution::Weekly     => 7*24*3600,
            Resolution::Ticks | Resolution::Quote | Resolution::TickBars(_) => 1,
        };
        let next = secs - secs.rem_euclid(step) + step;
        Utc.timestamp_opt(next, 0).single().unwrap_or(now)
    }

}


/// helper: turn UniversePlan -> PlanState
fn compile_plans(plans: Vec<UniversePlan>) -> Vec<PlanState> {
    plans
        .into_iter()
        .map(|p| PlanState {
            schedule: p.schedule,
            handle: UniverseHandle {
                name: p.name,
                exch: p.universe.exchange(),
                last: Default::default(),
                inner: p.universe,
            },
            next_at: None, // run on next step()
        })
        .collect()
}


/// Immutable configuration captured when a strategy is registered with the manager.
///
/// A `StrategyEntry` does **not** hold any live subscriptions or task state itself;
/// that belongs in the [`StratRuntime`]. Instead, it describes *what* the
/// strategy wants and *where* it runs:
///
/// - `name`: Identifier used for logs and runtime keys.
/// - `mode`: Live or Backtest — decides which clock/bus to use.
/// - `strat`: The actual strategy instance (`Arc<dyn Strategy>`). The manager
///   spawns its `run()` task using this.
/// - `plans`: Compiled universe plans (`UniversePlan → PlanState`) that govern
///   symbol selection cadence.
/// - `feeds`: Bitmask of [`FeedKind`]s the engine should manage for each
///   selected symbol (e.g. `TICKS | BBO | BOOK`).
/// - `ctx_key`: Which context this entry is bound to (`Live` or a specific
///   backtest run).
struct StrategyEntry {
    name: String,
    mode: StrategyMode,
    #[allow(dead_code)]
    strat: Arc<dyn Strategy>,
    plans: Vec<PlanState>,
    feeds: FeedKind,
    ctx_key: CtxKey,
}

/// Engine-owned vendor subscription guards for one symbol.
///
/// The manager inserts a `GuardSet` into the strategy’s [`StratRuntime`]
/// whenever a symbol is added by a universe plan. Each field corresponds to
/// a different feed type:
///
/// - `base`: Guard for **BBO/BOOK** feeds (quote/orderbook).
/// - `tick`: Guard for **TICKS** (trade prints).
/// - `candle`: Guard for **CANDLES** (time-bar stream).
///
/// If a field is `None`, that feed is not currently subscribed. Dropping a
/// `GuardSet` (or one of its fields) automatically triggers unsubscribe
/// upstream via the guard’s `Drop` impl. Strategies only receive broadcast
/// receivers — they never own these guards directly.
#[allow(dead_code)]
struct GuardSet {
    base: Option<SubGuard>,   // BBO|BOOK
    tick: Option<SubGuard>,   // TICKS
    candle: Option<SubGuard>, // CANDLES
}
/// Per-strategy runtime state:
/// - Mailbox to the runner (`tx`)
/// - JoinHandle of the runner task
/// - Engine-owned guard set per selected symbol
struct StratRuntime {
    tx: mpsc::Sender<StrategyEvent>,
    #[allow(dead_code)]
    handle: tokio::task::JoinHandle<()>,
    per_symbol: HashMap<SymbolId, GuardSet>,
}

/// A compiled universe plan with scheduling state.
/// `next_at = None` means “run on the next `step()`”.
struct PlanState {
    schedule: SelectionSchedule,
    handle: UniverseHandle,
    next_at: Option<DateTime<Utc>>,
}

/// Unique identifier for a backtest run.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct RunId(Uuid);

impl RunId {
    /// Generate a new unique run ID.
    pub fn new() -> Self { Self(Uuid::new_v4()) }
}

/// Key that identifies an engine context owned by the manager:
/// - `Live`  → single global live context
/// - `Backtest(RunId)` → isolated backtest context
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum CtxKey {
    Live,
    Backtest(RunId),
}

/// Metadata describing a backtest run (window and ID).
#[derive(Clone, Copy, Debug)]
pub struct BtMeta {
    pub id: RunId,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}
