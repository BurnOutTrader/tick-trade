use std::str::FromStr;
use std::sync::Arc;
use ahash::AHashMap;
use async_trait::async_trait;
use chrono::NaiveDate;
use chrono_tz::Tz;
use prost::Message;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tracing::{error, info, warn};
use crate::websocket::api_base::RithmicApiClient;
use standard_lib::engine_core::api_traits::FuturesUniverseProvider;
use standard_lib::securities::futures_helpers;
use standard_lib::securities::security::PricingSpecs;
use standard_lib::securities::symbols::{Currency, Exchange, SymbolId};
use crate::websocket::rithmic_proto_objects::rti::{ResponseProductCodes, ResponseReferenceData};
use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;

fn windows_tz_to_iana(s: &str) -> Option<&'static str> {
    match s {
        // Extend this map as needed
        "Central Standard Time"  => Some("America/Chicago"),
        "Eastern Standard Time"  => Some("America/New_York"),
        "Mountain Standard Time" => Some("America/Denver"),
        "Pacific Standard Time"  => Some("America/Los_Angeles"),
        "UTC"                    => Some("UTC"),
        _ => None,
    }
}

#[async_trait]
impl FuturesUniverseProvider for RithmicApiClient {
    async fn list_contracts(
        self: &Arc<Self>,
        exch: Exchange,
    ) -> AHashMap<String, Vec<(SymbolId, NaiveDate, Tz)>> {
        let mut out: AHashMap<String, Vec<(SymbolId, NaiveDate, Tz)>> = AHashMap::new();
        let mut tz_by_product: AHashMap<String, Tz> = AHashMap::new();

        match self.request_product_codes(Some(exch.to_string()), true).await {
            Ok(chunks) => {
                for payload in chunks {
                    // Decode
                    let mut rd = &payload[..];
                    let msg = match ResponseProductCodes::decode(&mut rd) {
                        Ok(m) => {
                            info!("ResponseProductCodes: {:?}", m);
                            m
                        }
                        Err(e) => {
                            warn!("decode ResponseProductCodes: {e}");
                            continue;
                        }
                    };

                    // Pull fields
                    let product_code = msg.product_code.as_deref().unwrap_or_default();
                    if product_code.is_empty() {
                        // Without a product code we can't group this row; skip
                        continue;
                    }
                    // Ensure key exists
                    out.entry(product_code.to_string()).or_default();

                    // Map/store timezone for this product if present (use first valid one)
                    if let Some(raw_tz) = msg.timezone_time_of_interest.as_deref() {
                        let mapped = windows_tz_to_iana(raw_tz).unwrap_or(raw_tz);
                        if let Ok(tz) = Tz::from_str(mapped) {
                            tz_by_product.entry(product_code.to_string()).or_insert(tz);
                        } else {
                            warn!(
                            "skip: invalid tz (raw='{}', mapped='{}') product='{}'",
                            raw_tz, mapped, product_code
                        );
                        }
                    }

                    // If row has no symbol, it's a product-only line; keep the key and continue
                    let sym_name = msg.symbol_name.as_deref().unwrap_or_default();
                    if sym_name.is_empty() {
                        warn!("skip: empty symbol_name for product='{}'", product_code);
                        continue;
                    }

                    // Parse expiry from the vendor symbol
                    let expiry = match futures_helpers::parse_expiry_from_contract_code(sym_name) {
                        Some(d) => d,
                        None => {
                            warn!(
                            "skip: cannot parse expiry from symbol='{}' (product='{}')",
                            sym_name, product_code
                        );
                            continue;
                        }
                    };

                    // Pick a timezone for this tuple (fallback to UTC if none recorded)
                    let tz = tz_by_product
                        .get(product_code)
                        .cloned()
                        .unwrap_or(chrono_tz::UTC);

                    // Push tuple
                    if let Some(vec) = out.get_mut(product_code) {
                        vec.push((SymbolId(sym_name.to_string()), expiry, tz));
                    }
                }

                // Sort & dedup each product’s list
                for (_prod, vec) in out.iter_mut() {
                    vec.sort_by(|a, b| {
                        // (SymbolId, NaiveDate, Tz)
                        let (sa, da, _ta) = a;
                        let (sb, db, _tb) = b;
                        da.cmp(db).then_with(|| sa.0.cmp(&sb.0))
                    });
                    vec.dedup_by(|a, b| a.0 == b.0); // dedup by SymbolId
                }

                // SECOND PASS: for products that are still empty, try front month
                let empties: Vec<String> = out
                    .iter()
                    .filter(|(_k, v)| v.is_empty())
                    .map(|(k, _)| k.clone())
                    .collect();

                for prod in empties {
                    match self.front_month_contract(prod.clone(), exch).await {
                        Some(front_code) => {
                            if let Some(expiry) = futures_helpers::parse_expiry_from_contract_code(&front_code) {
                                let tz = tz_by_product
                                    .get(&prod)
                                    .cloned()
                                    .unwrap_or(chrono_tz::UTC);
                                // Push into map now (no outstanding mutable borrow across await)
                                out.entry(prod.clone())
                                    .or_default()
                                    .push((SymbolId(front_code.clone()), expiry, tz));
                            } else {
                                warn!(
                                "skip: cannot parse expiry from front month code='{}' product='{}'",
                                front_code, prod
                            );
                            }
                        }
                        None => {
                            warn!("skip: no front month contract for product='{}'", prod);
                        }
                    }
                }

                out
            }
            Err(e) => {
                error!("request_product_codes failed: {e}");
                out
            }
        }
    }

    async fn front_month_contract(
        self: &Arc<Self>,
        root: String,
        exch: Exchange
    ) -> Option<String> {
        // Reference Data (Template 15) returns trading_symbol (e.g., "MNQU5") directly.
        let chunks = self
            .request_reference_data(SysInfraType::TickerPlant, root.clone(), exch.to_string())
            .await
            .ok()?;

        for payload in chunks {
            let mut rd = &payload[..];
            match ResponseReferenceData::decode(&mut rd) {
                Ok(m) => {
                    // Prefer trading_symbol. Fallbacks: exchange_symbol, then symbol.
                    if let Some(ts) = m.trading_symbol.filter(|s| !s.is_empty()) {
                        return Some(strip_exch_suffix(ts));
                    }
                    if let Some(es) = m.exchange_symbol.filter(|s| !s.is_empty()) {
                        return Some(strip_exch_suffix(es));
                    }
                    if let Some(s) = m.symbol.filter(|s| !s.is_empty()) {
                        return Some(strip_exch_suffix(s));
                    }
                }
                Err(e) => {
                    tracing::warn!("decode ResponseReferenceData: {e}");
                }
            }
        }
        None
    }

    async fn pricing_specs(
        self: &Arc<Self>,
        root: String,
        exch: Exchange,
    ) -> Option<PricingSpecs> {
        // 1) Pull reference data for this product root on the given exchange
        let chunks = self
            .request_reference_data(SysInfraType::TickerPlant, root.clone(), exch.to_string())
            .await
            .ok()?;

        // 2) Choose the first suitable row (prefer one that matches product_code == root)
        let mut refd: Option<ResponseReferenceData> = None;
        for payload in chunks {
            let mut rd = &payload[..];
            if let Ok(m) = ResponseReferenceData::decode(&mut rd) {
                let pc = m.product_code.as_deref().unwrap_or("").trim();
                if pc.eq_ignore_ascii_case(&root) || pc.is_empty() {
                    refd = Some(m);
                    break;
                }
            }
        }
        let m = refd?;

        // 3) Map to PricingSpecs
        let ccy = m.currency.as_deref().unwrap_or("USD");
        let quote_ccy = Currency::from_str(ccy).unwrap_or(Currency::USD);
        let raw_expiry = match m.expiration_date.as_deref() {
            None => {
                warn!("missing expiration_date for root='{}' exch='{}'", root, exch);
                return None;
            }
            Some(ex) => ex,
        };

        let expiration_date = match NaiveDate::parse_from_str(raw_expiry, "%Y%m%d") {
            Ok(d) => d,
            Err(e) => {
                warn!(
            "bad expiration_date='{}' for root='{}' exch='{}': {}",
            raw_expiry, root, exch, e
        );
                return None;
            }
        };
        // tick size and $/tick
        let tick_size = Decimal::from_f64(m.min_qprice_change.unwrap_or(0.0)).unwrap_or(Decimal::ZERO);
        let single_point_value = Decimal::from_f64(m.single_point_value.unwrap_or(0.0)).unwrap_or(Decimal::ZERO);
        let value_per_tick = (tick_size * single_point_value).normalize();

        // For index futures, contract multiplier is commonly the $/point
        let contract_multiplier = single_point_value;



        Some(PricingSpecs {
            expiration_date: Some(expiration_date),
            tick_size,
            value_per_tick,
            lot_size: Decimal::ONE,
            contract_multiplier,
            quote_ccy,
        })
    }
}

/// Remove trailing ".CME" or similar, if present.
fn strip_exch_suffix(mut s: String) -> String {
    if let Some(dot_idx) = s.rfind('.') {
        let suffix = &s[dot_idx + 1..];
        if (2..=4).contains(&suffix.len()) && suffix.chars().all(|c| c.is_ascii_alphabetic()) {
            s.truncate(dot_idx);
        }
    }
    s
}