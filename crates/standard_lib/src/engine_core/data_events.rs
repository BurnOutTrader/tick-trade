//! Market-data routing and subscription deduplication.
//!
//! This module wires providers into a single router (`MdRouter`) that:
//! - registers vendors,
//! - picks the appropriate provider for a request (`VendorPolicy`),
//! - deduplicates identical subscriptions keyed by `SubKey`,
//! - and exposes RAII-style lifetime management via `SubGuard`.
//!
//! The router is concurrency-safe and cheap to clone. It keeps a ref-counted
//! table of active subscriptions and automatically unsubscribes from a vendor
//! once the last `SubGuard` is dropped.

use std::sync::{Arc, Weak};
use std::{hash::{Hash, Hasher}, sync::atomic::{AtomicU32, Ordering}};
use dashmap::DashMap;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tokio::sync::{mpsc, RwLock};
use crate::engine_core::api_traits::{HistoricalDataProvider, HistoricalRequest, HistoryHandle, MarketDataProvider, HistoryEvent};
use crate::engine_core::event_hub::EventHub;
pub use crate::engine_core::public_classes::{FeedKind, MarketDataRequest, StrategyMode};
use crate::market_data::base_data::Resolution;
use crate::market_data::market_by_order::{BookInterest, BookNudgeBus, BookService, BookTrigger, L3Book, MboEvent};
use crate::securities::symbols::Exchange;

/// Provider-facing opaque handle for a single vendor subscription
/// (ticks/BBO/book/candles/etc.). The router never assumes what the
/// handle does—only that it can be uniquely identified and
/// unsubscribed when the last user releases it.
///
/// Implemented by each `MarketDataProvider`.
#[allow(dead_code)]
pub trait SubscriptionHandle: Send + Sync {
    fn id(&self) -> u64;
    fn unsubscribe(&self) -> anyhow::Result<()>;

    // Optional capability: default = not supported.
    fn as_streams_mbo(&self) -> Option<&dyn StreamsMbo> { None }
}

pub trait StreamsMbo: Send + Sync {
    fn attach_mbo_sink(&self, symbol: &str, sink: mpsc::UnboundedSender<MboEvent>);
}

pub struct DbMboHandle {
    sinks: dashmap::DashMap<String, mpsc::UnboundedSender<MboEvent>>,
    // … network/decoder internals …
}

impl StreamsMbo for DbMboHandle {
    fn attach_mbo_sink(&self, symbol: &str, sink: mpsc::UnboundedSender<MboEvent>) {
        self.sinks.insert(symbol.to_string(), sink);
    }
}

impl SubscriptionHandle for DbMboHandle {
    fn id(&self) -> u64 { /* … */ 0 }
    fn unsubscribe(&self) -> anyhow::Result<()> { /* … */ Ok(()) }

    // Advertise the capability.
    fn as_streams_mbo(&self) -> Option<&dyn StreamsMbo> { Some(self) }
}


/// Canonical identity of a *normalized* market-data subscription.
/// Keys dedupe requests across strategies/components so that
/// multiple callers share one vendor subscription under the hood.
///
/// Two keys are equal if and only if all of the following match:
/// - `vendor` (e.g., `"RITHMIC"`)
/// - `symbol` (vendor code, e.g., `"MNQZ25"`)
/// - `exchange` (string hint as carried in `MarketDataRequest`)
/// - `kinds` (bitflags for TICKS/BBO/BOOK/CANDLES/…)
/// - `resolution` (e.g., `Some(Resolution::Seconds(1))`)
/// - `depth` (e.g., book levels)
///
/// Hash composition mirrors equality to guarantee correct sharing in
/// the `DashMap`.
#[derive(Clone, Debug, Eq)]
pub(crate) struct SubKey {
    vendor: String, // "rithmic"
    symbol: String,
    exchange: String,
    kinds: FeedKind,
    resolution: Option<Resolution>,
    depth: Option<u8>,
}
impl PartialEq for SubKey { fn eq(&self, o:&Self)->bool {
    self.vendor==o.vendor && self.symbol==o.symbol && self.exchange==o.exchange &&
        self.kinds==o.kinds && self.resolution==o.resolution && self.depth==o.depth
}}
impl Hash for SubKey {
    fn hash<H:Hasher>(&self, h:&mut H){
        self.vendor.hash(h); self.symbol.hash(h); self.exchange.hash(h);
        self.kinds.bits().hash(h); self.resolution.hash(h); self.depth.hash(h);
    }
}

/// Internal entry stored in the active-subscription table.
///
/// `refcnt` counts live `SubGuard`s; when it reaches zero we remove the
/// map entry and call `unsubscribe()` on the provider handle.
struct SubEntry {
    refcnt: AtomicU32,
    handle: Arc<dyn SubscriptionHandle>,
}

/// ========================================================
/// MdRouter
/// ========================================================
///
/// **Market-data router** that:
/// - Registers vendor providers
/// - Deduplicates identical subscription requests (by `SubKey`).
/// - Manages reference counting and **RAII** unsubscription via
///   lightweight `SubGuard`s.
///
/// ### Concurrency & safety
/// - Internals are behind `DashMap` and `Arc`; the router is `Clone`.
/// - `acquire()` is async only for provider I/O; hot paths are lock-free
///   aside from map lookups.
/// - Unsubscribe happens off the drop path on a Tokio task to avoid
///   blocking destructors.
///
/// ### Backtest
/// - Live mode is implemented. Backtest mode can be wired to a simulator
///   later; currently returns a stub error.
#[derive(Clone)]
pub struct MdRouter {
    pub(crate) inner: Arc<MdRouterInner>,
    hub: Arc<EventHub>,
    book_svc: Arc<BookService>,
}

pub(crate) struct MdRouterInner {
    /// Registered providers by **name** (must be unique).
    providers: DashMap<String, Arc<dyn MarketDataProvider>>,
    /// Active dedup table: `SubKey` → refcount + provider handle.
    active:    DashMap<SubKey, Arc<SubEntry>>,
    hist_providers: DashMap<String, Arc<dyn HistoricalDataProvider>>,
    book_nudges: Arc<BookNudgeBus>,
}

impl MdRouter {
    /// Create an empty router (no providers yet).
    pub fn new(live_hub: Arc<EventHub>) -> Self {
        let nudges = Arc::new(BookNudgeBus::new());
        let book_svc = Arc::new(BookService::new(live_hub.clone(), nudges.clone()));

        Self {
            inner: Arc::new(MdRouterInner {
                providers: DashMap::new(),
                active: DashMap::new(),
                hist_providers: DashMap::new(),
                book_nudges: nudges,
            }),
            hub: live_hub,
            book_svc,
        }
    }

    /// Convenience for strategies: register for nudges without touching BookNudgeBus directly.
    pub fn register_book_interest(
        &self,
        symbol: &str,
        interest: BookInterest,
    ) -> mpsc::UnboundedReceiver<BookTrigger> {
        self.inner.book_nudges.register(symbol, interest)
    }

    /// Register a historical provider under its `name()`.
    pub fn register_hist(&self, p: Arc<dyn HistoricalDataProvider>) {
        self.inner.hist_providers.insert(p.name().to_string(), p);
    }

    /// Register & connect a historical provider (idempotent connect).
    pub async fn register_and_connect_hist(
        &self,
        p: Arc<dyn HistoricalDataProvider>
    ) -> anyhow::Result<()> {
        p.clone().ensure_connected().await?;
        self.register_hist(p);
        Ok(())
    }

    /// Register a provider under its `name()`. Overwrites any existing one
    /// with the same name.
    pub fn register(&self, p: Arc<dyn MarketDataProvider>) {
        self.inner.providers.insert(p.md_vendor_name().to_string(), p);
    }

    /// Register a provider and ensure it is connected/logged-in.
    ///
    /// Providers should make `ensure_connected()` idempotent; calling this multiple
    /// times is safe. The provider is inserted under its `name()` for lookups
    /// performed by `pick_provider()`.
    ///
    /// Returns an error if the provider fails to connect.
    pub async fn register_and_connect(&self, provider: Arc<dyn MarketDataProvider>) -> anyhow::Result<()> {
        provider.clone().ensure_connected().await?;
        tracing::info!(provider=%provider.md_vendor_name(), "registered & connected market-data provider");
        self.register(provider);
        Ok(())
    }

    pub async fn fetch_history(
        &self,
        req: HistoricalRequest
    ) -> anyhow::Result<(Arc<dyn HistoryHandle>, mpsc::Receiver<HistoryEvent>)> {
        let p = self.pick_hist_provider(&req.vendor)?;
        p.clone().ensure_connected().await?;
        p.fetch(req).await
    }

    /// Select a provider according to the requested `VendorPolicy`.
    ///
    /// - `Exact(name)`: return the provider registered under `name`, or error if missing.
    /// - `Prefer(list)`: return the first registered provider found in `list`;
    ///   if none match, fall back to **any** registered provider.
    /// - `Any`: return the first registered provider (iteration order).
    ///
    /// Errors if the router has no providers registered.
    fn pick_provider(&self, vendor: &str) -> anyhow::Result<Arc<dyn MarketDataProvider>> {
        self.inner.providers.get(vendor).map(|v| v.clone())
            .ok_or_else(|| anyhow::anyhow!("unknown provider {vendor}"))
    }

    fn pick_hist_provider(
        &self,
        vendor: &str
    ) -> anyhow::Result<Arc<dyn HistoricalDataProvider>> {
        self.inner.hist_providers
            .get(vendor)
            .map(|v| v.clone())
            .ok_or_else(|| anyhow::anyhow!("unknown historical provider {vendor}"))
    }

    pub fn event_hub(&self) -> Arc<EventHub> { self.hub.clone() } // if needed
    pub fn book_nudges(&self) -> Arc<BookNudgeBus> { self.inner.book_nudges.clone() }

    // You can expose BookService helpers if you want:
    pub fn book_handle(&self, symbol: &str, exchange: Exchange) -> Option<Arc<RwLock<L3Book>>> {
        self.book_service().book_handle(symbol, exchange)
    }
    fn book_service(&self) -> Arc<BookService> {
        // store BookService in MdRouter similar to EventHub/nudges:
        // MdRouter { hub, inner: ..., book_svc: Arc<BookService> }
        self.book_svc.clone()
    }

    pub async fn acquire(&self, req: MarketDataRequest) -> anyhow::Result<SubGuard> {
        let provider = self.pick_provider(&req.vendor)?;
        provider.clone().ensure_connected().await?;

        let key = SubKey {
            vendor: provider.md_vendor_name().to_string(),
            symbol: req.symbol.clone(),
            exchange: req.exchange.to_string(),
            kinds: req.kinds,
            resolution: req.resolution.clone(),
            depth: req.book_depth,
        };

        if let Some(e) = self.inner.active.get(&key) {
            e.refcnt.fetch_add(1, Ordering::AcqRel);
            return Ok(SubGuard { router: Arc::downgrade(&self.inner), key, entry: e.clone() });
        }

        // 1) Open vendor subscription exactly as requested
        let handle = provider.clone().subscribe(&req).await?;
        tracing::info!(vendor=%provider.md_vendor_name(), handle_id=%handle.id(), symbol=%req.symbol, "provider subscription created");

        // 2) If the handle can stream raw MBO, attach an engine sink **when L3-related kinds are requested**
        let wants_l3 = req.kinds.intersects(FeedKind::MBO | FeedKind::TICKS | FeedKind::BBO | FeedKind::BOOK);
        if wants_l3 {
            if let Some(s) = handle.as_streams_mbo() {
                // Engine maintains canonical book(s), and will only publish the derived
                // streams indicated by req.kinds (TICKS/BBO/BOOK bits).
                let sink = self.book_svc.make_mbo_sink(
                    req.symbol.clone(),
                    req.exchange,
                    req.kinds,
                    req.book_depth.unwrap_or(10) as usize,          // default depth for snapshots
                    std::time::Duration::from_millis(250),         // default snapshot cadence
                );
                s.attach_mbo_sink(&req.symbol, sink);
            }
        }

        let entry = Arc::new(SubEntry { refcnt: AtomicU32::new(1), handle });
        self.inner.active.insert(key.clone(), entry.clone());
        Ok(SubGuard { router: Arc::downgrade(&self.inner), key, entry })
    }
}

/// RAII guard that keeps a deduped vendor subscription alive.
///
/// Cloning a `SubGuard` shares the same underlying `SubEntry`. When the
/// **last** guard is dropped, the router:
/// 1) removes the `SubKey` from the `active` map, and
/// 2) unsubscribes from the provider on a background Tokio task.
///
/// The guard holds only a `Weak` pointer to the router internals so it will
/// not keep the router alive during shutdown.
pub struct SubGuard {
    router: Weak<MdRouterInner>,
    key: SubKey,
    #[allow(dead_code)]
    // keep the entry Arc alive to prevent a race removing the map entry
    entry: Arc<SubEntry>,
}

impl Drop for SubGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.router.upgrade() {
            release_inner(&inner, &self.key);
        }
    }
}

/// Decrement refcount for `key` and, if it reaches zero, remove the active
/// entry and trigger provider `unsubscribe()` on a background task.
///
/// This function is called from `Drop` of `SubGuard` and must remain non-blocking.
fn release_inner(inner: &MdRouterInner, key: &SubKey) {
    // fast path: decrement
    if let Some(entry) = inner.active.get(key) {
        let prev = entry.refcnt.fetch_sub(1, Ordering::AcqRel);
        if prev > 1 {
            return; // still in use
        }
    } else {
        return; // already gone
    }

    // last guard — remove and unsubscribe
    let handle_opt = inner.active.remove(key).map(|(_, e)| e.handle.clone());
    if let Some(handle) = handle_opt {
        // do vendor async cleanup off the drop path
        tokio::spawn(async move {
            let _ = handle.unsubscribe();
        });
    }
}

#[inline]
pub fn dec_to_nano(d: rust_decimal::Decimal, scale: i64) -> i64 {
    (d * rust_decimal::Decimal::from_i64(scale).unwrap()).round().to_i64().unwrap()
}

