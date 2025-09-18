use rust_decimal::Decimal;
use chrono::{DateTime, Utc};
use rust_decimal_macros::dec;
use crate::reality_models::models::ModelSet;
use crate::reality_models::traits::{BuyingPowerModel, FeeModel, FillModel, SettlementModel, SlippageModel, VolatilityModel};
use crate::securities::security::PricingSpecs;
use crate::securities::symbols::Currency;

// One global default you can reference (or build all entries explicitly)
lazy_static::lazy_static! {
    static ref DEFAULT_FUTURES: ModelSet = ModelSet::default();
}

//Default models
pub struct DefaultFeeModel;

impl FeeModel for DefaultFeeModel { fn estimate(&self, _:Decimal, _:Decimal) ->Decimal{dec!(0)} }

pub struct DefaultSlippage;

impl SlippageModel for DefaultSlippage { fn slip(&self, p:Decimal, _:Decimal) ->Decimal{p} }

pub struct DefaultFill;

impl FillModel for DefaultFill { fn fill_price(&self, m:Decimal, o:Option<Decimal>, _:Decimal) ->Decimal{ o.unwrap_or(m) } }

pub struct DefaultFuturesBP;

impl BuyingPowerModel for DefaultFuturesBP {
    fn initial_margin(&self, px:Decimal, qty:Decimal, _:&PricingSpecs)->Decimal{ (px*qty).abs()*dec!(0.05) }
    fn buying_power(&self, _p:Currency, _s:Currency)->Decimal{ dec!(1_000_000) }
}

pub struct DefaultVol;

impl VolatilityModel for DefaultVol { fn update(&self, _:Decimal, _:DateTime<Utc>){} }

pub struct DefaultSettle;

impl SettlementModel for DefaultSettle { fn settles_intraday(&self) ->bool{ true } }