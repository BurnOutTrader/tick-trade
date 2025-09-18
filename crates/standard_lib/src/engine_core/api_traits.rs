use std::sync::Arc;
use ahash::AHashMap;
use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::Tz;
use tokio::sync::mpsc;
use uuid::Uuid;
use crate::engine_core::public_classes::{FeedKind, MarketDataRequest};
use crate::market_data::base_data::{Bbo, Candle, OrderBook, Resolution, Tick};
use crate::engine_core::data_events::{SubscriptionHandle};
use crate::engine_core::provider_resolver::SecurityResolver;
use crate::reality_models::models::ModelCatalog;
use crate::securities::futures_helpers::parse_expiry_from_contract_code;
use crate::securities::market_hours::MarketHours;
use crate::securities::security::{PricingSpecs, Security, SymbolProperties, VendorSymbol};
use crate::securities::symbols::{Exchange, SecurityType, SymbolId};

/// A venue adapter that can establish/maintain connectivity and open
/// live market-data subscriptions. Implementations are typically thin
/// wrappers over vendor SDKs (e.g., Rithmic, FIX/ITCH/WebSocket).
///
/// ### Responsibilities
/// - **Connection lifecycle** via [`ensure_connected`](Self::ensure_connected)
///   (idempotent).
/// - **Subscription acquisition** via [`subscribe`](Self::subscribe)
///   returning an RAII [`SubscriptionHandle`] that will unsubscribe
///   when dropped.
/// - **Feature detection** to allow routers to adapt requests.
///
/// ### Concurrency
/// - Methods are `async` and the trait is `Send + Sync`.
/// - Callers generally pass `Arc<dyn MarketDataProvider>`.
///
/// ### Idempotency
/// - `ensure_connected` **must** be safe to call multiple times.
///   If sessions are already up, it should be a no-op.
///
/// ### Errors
/// - Use `anyhow::Result` to surface vendor/logical failures.
///
/// ### Example
/// ```ignore
/// let provider: Arc<dyn MarketDataProvider> = Arc::new(MyVenue::new(...));
/// provider.clone().ensure_connected().await?;
///
/// let handle = provider.clone()
///     .subscribe(&MarketDataRequest {
///         vendor: VendorPolicy::Exact("RITHMIC".into()),
///         symbol: "MNQZ25".into(),
///         exchange: "CME".into(),
///         kinds: FeedKind::BBO | FeedKind::BOOK,
///         resolution: None,
///         book_depth: Some(10),
///     })
///     .await?;
/// // While `handle` is alive, the feed stays subscribed.
/// ```
#[async_trait::async_trait]
pub trait MarketDataProvider: Send + Sync {
    /// Human-readable provider name (e.g., `"RITHMIC"`).
    fn md_vendor_name(&self) -> &str;
    /// Ensure network sessions/logins are established.
    ///
    /// **Idempotent**: if already connected, this should be a no-op.
    /// Implementations may spawn background tasks (heartbeats, handlers).
    async fn ensure_connected(self: Arc<Self>) -> anyhow::Result<()>;

    /// Open (or attach to) a market-data subscription.
    ///
    /// Implementations can dedupe internally, but the engine’s
    /// [`MdRouter`] typically handles deduplication and reference counting.
    ///
    /// Returns an RAII [`SubscriptionHandle`]. Dropping it should tear
    /// down the vendor subscription when the last reference disappears.
    async fn subscribe(self: Arc<Self>, req: &MarketDataRequest) -> anyhow::Result<Arc<dyn SubscriptionHandle>>;

    /// Capability probe: does this provider support the requested feed kinds?
    /// Defaults to `true` (opt-in stricter providers may override).
    fn supports(&self, _kinds: FeedKind) -> bool { true }
    /// Capability probe for candle/aggregation resolution.
    /// Defaults to `true`; override for venues with limited bar sizes.
    fn supports_resolution(&self, _res: &Option<Resolution>) -> bool { true }

    fn resolver(self: Arc<Self>) -> Arc<dyn SecurityResolver>;
}

/// Extract a futures **root** from a vendor symbol code.
///
/// Heuristic:
/// - Uppercase the input.
/// - Strip **up to two trailing digits** (year suffix).
/// - If there was at least one digit removed and the prior char is an
///   alphabetic **month code** (F,G,H, …, Z), strip that as well.
/// - Return everything **before** that boundary.
///
/// This produces:
/// - `"ESZ25"` → `"ES"`
/// - `"MNQZ5"` → `"MNQ"`
/// - `"CL"`    → `"CL"` (no change)
///
/// The function is intentionally fast and allocation-light; it does not
/// validate that the remaining characters are a real exchange root.
///
/// ### Notes
/// - Works for common futures codes with 1–2 digit years.
/// - If you need 3–4 digit year formats, extend the heuristic.
///
/// ### Example
/// ```
/// assert_eq!(extract_root("ESZ25"), "ES");
/// assert_eq!(extract_root("MNQZ5"), "MNQ");
/// assert_eq!(extract_root("cl"), "CL");
pub(crate) fn extract_root(code: &str) -> String {
    let up = code.to_ascii_uppercase();
    let b = up.as_bytes();
    let mut i = b.len();
    let mut digits = 0;
    while i > 0 && b[i-1].is_ascii_digit() && digits < 2 { i -= 1; digits += 1; }
    if i > 0 && digits > 0 && b[i-1].is_ascii_alphabetic() { i -= 1; }
    code[..i].to_string()
}

/// ========================================================
/// FuturesUniverseProvider
/// ========================================================
///
/// A “static facts” provider for futures universes:
/// - **Contract discovery** (all listed/dated contracts).
/// - **Front-month selection** for a root.
/// - **Pricing specs** (tick size, multiplier, currency, etc.).
/// - **Security construction** from provider facts.
///
/// Implementations may query exchange reference data, vendor product
/// catalogs, or internal metadata stores.
///
/// ### Typical Flow
/// 1. `list_contracts(exch)` to map roots → all dated contracts.
/// 2. `front_month_contract(root, exch)` to determine the default month.
/// 3. `pricing_specs(root, exch)` to get tick/multiplier/ccy.
/// 4. `build_security(...)` to assemble a canonical [`Security`].
///
/// ### Concurrency
/// - Methods are `async`; trait is `Send + Sync`.
/// - Call sites typically hold `Arc<Self>`.
#[async_trait::async_trait]
pub trait FuturesUniverseProvider: Send + Sync  {
    /// Return all currently listed (future-dated) contracts per **root** for `exch`.
    ///
    /// Map key is a **root** string (e.g., `"ES"`, `"MNQ"`). Value is a vector of
    /// `(SymbolId, NaiveDate, Tz)` where the date is the contract’s expiration
    /// (or first notice/last trade—implementation-defined; document your choice),
    /// and `Tz` expresses the venue’s primary time zone for that contract.
    async fn list_contracts(self: &Arc<Self>, exch: Exchange)
                            -> AHashMap<String, Vec<(SymbolId, NaiveDate, Tz)>>;


    /// Return the best-effort **front month** contract code for a `root` on `exch`.
    ///
    /// Implementations should encode their roll policy (volume/open interest,
    /// exchange guidance, or calendar proximity).
    async fn front_month_contract(self: &Arc<Self>, root: String, exch: Exchange) -> Option<String>;

    /// Static pricing facts for a **root** on `exch` (tick size, multiplier, quote ccy, …).
    ///
    /// May optionally include a canonical expiration for the root if the product
    /// is single-month; otherwise, the expiration is determined at the contract level.
    async fn pricing_specs(self: &Arc<Self>, root: String, exch: Exchange) -> Option<PricingSpecs>;


    /// Construct a canonical [`Security`] from provider facts.
    ///
    /// Default behavior:
    /// - Derive `root` via [`extract_root`].
    /// - Fetch `pricing_specs`. If missing, return `Ok(None)`.
    /// - If only a root was provided (e.g., `"ES"`), resolve a front-month
    ///   via [`front_month_contract`]. Otherwise, use the given code.
    /// - Attach vendor symbol mapping and selected models from `models`.
    /// - Embed `hours` (already computed or supplied by the caller).
    ///
    /// Override if a venue requires special handling (e.g., non-standard
    /// codes, synthetic/continuous symbols, or multi-leg products).
    ///
    /// ### Returns
    /// - `Ok(Some(Security))` on success
    /// - `Ok(None)` if the provider lacks sufficient facts to build it
    /// - `Err(_)` for hard failures (I/O, parse, invariant breaks)
    async fn build_security(
        self: &Arc<Self>,
        id: &SymbolId,                 // "ESU5" or "ES"
        exch: Exchange,
        models: Arc<ModelCatalog>,     // where you get Fee/Slippage/Fill/BP/Vol/Settlement
        hours: MarketHours,            // pass precomputed hours (or from a provider)
        vendor: String,                  // e.g., "RITHMIC"
    ) -> anyhow::Result<Option<Security>> {
        let code = &id.0;
        let root = extract_root(code);

        // 1) pricing facts
        let Some(pricing) = self.pricing_specs(root.clone(), exch).await else {
            return Ok(None);
        };

        // 2) expiry (prefer listed; fallback to code parse)
        let id_string = id.to_string();
        let root = extract_root(&id_string);
        let spec = match self.pricing_specs(id_string, exch).await {
            None => return Err(anyhow::anyhow!("no pricing_specs for {:?}", id)),
            Some(s) => s
        };

        // If only a root was given, resolve front month
        let (final_code, final_expiry) = if spec.expiration_date.is_none() && code.eq_ignore_ascii_case(&root) {
            let Some(front) = self.front_month_contract(root.clone(), exch).await else {
                return Ok(None);
            };
            let exp = parse_expiry_from_contract_code(&front);
            (front, exp)
        } else {
            (code.clone(), spec.expiration_date)
        };

        // 3) assemble
        let props = SymbolProperties {
            description: root.clone(),
            pricing,
            expiry: final_expiry,
            is_continuous: final_expiry.is_none(),
            underlying: Some(SymbolId(root.clone())),
            vendor_symbols: vec![VendorSymbol {
                vendor: vendor.into(),
                code: final_code.clone(),
                exchange_hint: Some(exch),
            }],
        };

        let model_set = models.select(SecurityType::Future, None, None);
        let sec = Security {
            id: SymbolId(final_code),
            security_type: SecurityType::Future,
            exchange: exch,
            props: Arc::new(props),
            hours: Arc::new(hours),
            fee_model: model_set.fee.clone(),
            slippage_model: model_set.slip.clone(),
            fill_model: model_set.fill.clone(),
            bp_model: model_set.bp.clone(),
            vol_model: model_set.vol.clone(),
            settlement_model: model_set.settle.clone(),
            cache: Arc::default(),
        };

        Ok(Some(sec))
    }
}


pub trait EquityUniverseProvider {
}

/// What a backtest wants to pull.
#[derive(Clone, Debug)]
pub struct HistoricalRequest {
    pub vendor: String,
    pub symbol:     String,           // vendor code ("MNQZ25")
    pub exchange:   Exchange,           // hint (e.g., "CME")
    pub r_type:      RequestType,         // TICKS | CANDLES | (BBO/BOOK optional later)
    pub resolution: Option<Resolution>, // required for CANDLES
    pub start:      DateTime<Utc>,
    pub end:        DateTime<Utc>,

}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Copy)]
pub enum RequestType {
    Tick,
    TickBar,
    Candle,
    Bbo,
    Book,
}

/// Streamed payloads from a provider.
#[derive(Debug)]
pub enum HistoryEvent {
    Tick(Tick),
    Candle(Candle),
    // Optional future:
    Bbo(Bbo),
    OrderBook(OrderBook),
    EndOfStream,          // provider finished successfully
    Error(anyhow::Error), // provider aborted
}

/// Provider-facing opaque handle (used by the router for cancellation).
pub trait HistoryHandle: Send + Sync {
    fn cancel(&self);        // idempotent
    fn id(&self) -> Uuid;     // stable id for logs
}

/// Providers implement this to supply historical data.
///
/// Contract:
/// - `fetch()` returns immediately with a handle and an `mpsc::Receiver<HistoryEvent>`.
/// - The provider streams results (possibly chunked) to the receiver.
/// - Cancellation via `handle.cancel()` should stop the stream ASAP.
#[async_trait::async_trait]
pub trait HistoricalDataProvider: Send + Sync {
    fn name(&self) -> &str;

    /// Optional: connect/login; should be idempotent.
    async fn ensure_connected(self: Arc<Self>) -> anyhow::Result<()>;

    /// Start a historical fetch stream.
    async fn fetch(
        self: Arc<Self>,
        req: HistoricalRequest,
    ) -> anyhow::Result<(Arc<dyn HistoryHandle>, mpsc::Receiver<HistoryEvent>)>;

    /// Feature flags help the router pick/shape requests.
    fn supports(&self, _kinds: FeedKind) -> bool { true }
    fn supports_resolution(&self, _res: &Option<Resolution>) -> bool { true }

    fn resolver(self: Arc<Self>) -> Arc<dyn SecurityResolver>;
}