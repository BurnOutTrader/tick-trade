use std::sync::Arc;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;
use crate::securities::market_hours::MarketHours;
use crate::securities::symbols::{Currency, Exchange, SecurityType, SymbolId};
use crate::reality_models::traits::{FeeModel, SlippageModel, FillModel, BuyingPowerModel, VolatilityModel, SettlementModel};

#[derive(Debug, Clone)]
pub struct PricingSpecs {
    pub tick_size: Decimal,
    pub value_per_tick: Decimal,
    pub lot_size: Decimal,
    pub contract_multiplier: Decimal,
    pub quote_ccy: Currency,
    pub expiration_date: Option<NaiveDate>
}

/// Static instrument facts (no runtime state).
#[derive(Debug, Clone)]
pub struct SymbolProperties {
    /// Human description/root (e.g., "Micro E-mini Nasdaq-100")
    pub description: String,
    /// Pricing/tick/multiplier/quote currency
    pub pricing: PricingSpecs,
    /// Expiration date for dated contracts; `None` for perpetual/continuous
    pub expiry: Option<chrono::NaiveDate>,
    /// True for continuous/front-month synthetic symbols
    pub is_continuous: bool,
    /// Optional underlying canonical id (for derivatives)
    pub underlying: Option<SymbolId>,
    /// Vendor mappings for routing to providers/venues
    pub vendor_symbols: Vec<VendorSymbol>,
}

/// Lightweight runtime cache/state for a security (prices, OI, etc.)
#[derive(Debug, Default)]
pub struct SecurityCache {
    // Add rolling fields as needed; keep lean to avoid cloning costs.
    // Example placeholders:
    // pub last_price: Option<Decimal>,
    // pub last_bid: Option<Decimal>,
    // pub last_ask: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VendorSymbol {
    pub vendor: String,
    pub code: String,
    pub exchange_hint: Option<Exchange>,
}

/// Handle that composes facts (props), calendar (hours), models, and small runtime cache.
#[derive(Clone)]
pub struct Security {
    pub id: SymbolId,
    pub security_type: SecurityType,
    pub exchange: Exchange,

    /// Static instrument properties (no Options in the handle itself)
    pub props: Arc<SymbolProperties>,

    /// Trading calendar for this instrument
    pub hours: Arc<MarketHours>,

    /// Pluggable models (always present; defaulted by an initializer/factory)
    pub fee_model: Arc<dyn FeeModel>,
    pub slippage_model: Arc<dyn SlippageModel>,
    pub fill_model: Arc<dyn FillModel>,
    pub bp_model: Arc<dyn BuyingPowerModel>,
    pub vol_model: Arc<dyn VolatilityModel>,
    pub settlement_model: Arc<dyn SettlementModel>,

    /// Small runtime cache/state
    pub cache: Arc<SecurityCache>,
}

impl Security {
    /// Find a vendor mapping by provider name (case-insensitive).
    #[inline]
    pub fn vendor(&self, name: &str) -> Option<&VendorSymbol> {
        self.props
            .vendor_symbols
            .iter()
            .find(|v| v.vendor.eq_ignore_ascii_case(name))
    }

    /// Convenience forwarders for frequently used facts
    #[inline] pub fn description(&self) -> &str { &self.props.description }
    #[inline] pub fn pricing(&self) -> &PricingSpecs { &self.props.pricing }
    #[inline] pub fn expiry(&self) -> Option<chrono::NaiveDate> { self.props.expiry }
    #[inline] pub fn is_continuous(&self) -> bool { self.props.is_continuous }
    #[inline] pub fn underlying(&self) -> Option<SymbolId> { self.props.underlying.clone() }
}
