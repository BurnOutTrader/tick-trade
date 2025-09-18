use std::sync::Arc;
use ahash::AHashSet;
use async_trait::async_trait;
use crate::engine_core::api_traits::{extract_root, FuturesUniverseProvider};
use crate::reality_models::models::ModelCatalog;
use crate::securities::security::{PricingSpecs, Security, SymbolProperties, VendorSymbol};
use crate::securities::symbols::{exchange_market_type, Exchange, MarketType, SecurityType, SymbolId};
use chrono::NaiveDate;
use chrono_tz::Tz;
use dashmap::DashMap;
use crate::securities::futures_helpers::parse_expiry_from_contract_code;
use crate::securities::market_hours::{hours_for_exchange, MarketHours};
use crate::securities::registry::SecurityRegistry;

/// Optional pluggable resolver that can hydrate a `Security` on-demand when a
/// universe selects a new `SymbolId` that's not yet in the registry.
#[async_trait]
pub trait SecurityResolver: Send + Sync {
    /// Return a fully-formed Security for `id` (or `Ok(None)` if unknown).
    async fn resolve(&self, id: &SymbolId, _exchange: Exchange) -> anyhow::Result<Option<Security>>;
    /// Optional batch hint for prefetching. Default: no-op.
    async fn prefetch(&self, _ids: &[SymbolId], _exchange: Exchange) -> anyhow::Result<()> { Ok(()) }
}

pub struct ProviderResolver<U: FuturesUniverseProvider> {
    pub vendor: String,
    pub uni:    Arc<U>,
    pub models: Arc<ModelCatalog>,

    // ---- new: warm caches to avoid repeated provider hits ----
    // All contracts grouped by root, loaded once per exchange
    contracts_by_root: DashMap<String, Vec<(SymbolId, NaiveDate, Tz)>>,
    // Pricing facts per (root, exchange)
    pricing_by_root: DashMap<String, PricingSpecs>,
}

impl<U: FuturesUniverseProvider> ProviderResolver<U> {
    pub fn new(
        vendor: String,
        uni: Arc<U>,
    ) -> Self {
        Self {
            vendor,
            uni,
            models: Arc::new(ModelCatalog::default()),
            contracts_by_root: DashMap::new(),
            pricing_by_root: DashMap::new(),
        }
    }

    /// Call this **once** at startup (or per exchange) to load everything.
    /// Super important: this performs the expensive calls just once.
    pub async fn prefetch_all(&self, exch: Exchange) -> anyhow::Result<()> {
        // 1) Load every contract once (expensive)
        let all = self.uni.list_contracts(exch).await; // all roots for exch
        for (root, contracts) in all {
            self.contracts_by_root.insert(root.clone(), contracts);
        }

        // 2) For every root we now know about, fetch pricing (your cheaper reference_data path)
        //    NOTE: pricing_specs() is implemented by you using request_reference_data(...)
        let mut unique_roots = AHashSet::new();
        for r in self.contracts_by_root.iter() {
            unique_roots.insert(r.key().clone());
        }
        for root in unique_roots {
            if let Some(ps) = self.uni.pricing_specs(root.clone(), exch).await {
                self.pricing_by_root.insert(root.clone(), ps);
            } else {
                tracing::warn!(%root, exch=?exch, "missing pricing_specs during prefetch");
            }
        }

        Ok(())
    }

    /// Build **all** securities for this exchange from the warm caches.
    /// Skips anything already in the registry. Returns how many were added.
    pub async fn bulk_build_all(&self, exchange: Exchange, registry: &SecurityRegistry) -> anyhow::Result<usize> {
        let mut added = 0usize;

        // Snapshot keys to avoid holding dashmap ref across awaits
        let roots: Vec<String> = self.contracts_by_root.iter().map(|kv| kv.key().clone()).collect();

        for root in roots {
            // 0) Skip roots with no pricing (cannot build)
            let Some(pricing) = self.pricing_by_root.get(&root).map(|v| v.clone()) else {
                tracing::warn!(%root, "no pricing found; skipping root");
                continue;
            };

            // 1) Market hours for this root (once)
            let hrs  = hours_for_exchange(exchange);

            // 2) Build a **continuous** (root-only) synthetic if you want it
            //    (id == root). Only insert if missing.
            let cont_id = SymbolId(root.clone());
            if registry.get(&cont_id, &exchange).is_none() {
                if let Some(sec) = self.build_from_parts(&cont_id, &root, exchange, None, &pricing, &hrs) {
                    registry.upsert(sec);
                    added += 1;
                }
            }

            // 3) All dated contracts for this root (preloaded)
            if let Some(contracts) = self.contracts_by_root.get(&root) {
                for (sid, expiry, _tz) in contracts.iter() {
                    if registry.get(sid, &exchange).is_some() { continue; }
                    if let Some(sec) = self.build_from_parts(sid, &root, exchange, Some(*expiry), &pricing, &hrs) {
                        registry.upsert(sec);
                        added += 1;
                    }
                }
            }
        }

        Ok(added)
    }

    /// Helper: build a `Security` entirely from cached parts — **no** provider calls.
    fn build_from_parts(
        &self,
        id: &SymbolId,
        root: &str,
        exchange: Exchange,
        expiry: Option<NaiveDate>,
        pricing: &PricingSpecs,
        hours: &MarketHours,
    ) -> Option<Security> {
        // SymbolProperties from cached pricing
        let props = SymbolProperties {
            description: root.to_string(),
            pricing: pricing.clone(),
            expiry,
            is_continuous: expiry.is_none(),
            underlying: Some(SymbolId(root.to_string())),
            vendor_symbols: vec![VendorSymbol {
                vendor: self.vendor.clone(),
                code: id.0.clone(),
                exchange_hint: Some(exchange),
            }],
        };

        // Models from catalog
        let model_set = self.models.select(SecurityType::Future, None, None);

        Some(Security {
            id: id.clone(),
            security_type: SecurityType::Future,
            exchange,
            props: Arc::new(props),
            hours: Arc::new(hours.clone()),
            fee_model: model_set.fee.clone(),
            slippage_model: model_set.slip.clone(),
            fill_model: model_set.fill.clone(),
            bp_model: model_set.bp.clone(),
            vol_model: model_set.vol.clone(),
            settlement_model: model_set.settle.clone(),
            cache: Arc::default(),
        })
    }
}

#[async_trait]
impl<U> SecurityResolver for ProviderResolver<U>
where
    U: FuturesUniverseProvider + Send + Sync + 'static,
{
    async fn resolve(&self, id: &SymbolId, exchange: Exchange) -> anyhow::Result<Option<Security>> {
        // Fast path using caches when available; fallback to the provider impl.
        let (root, expiry) = match exchange_market_type(exchange) {
            MarketType::Futures => {
                // Try to get expiry from cached contract list
                let root = extract_root(&id.0);
                let expiry: Option<NaiveDate> = self
                    .contracts_by_root
                    .get(&root)
                    .and_then(|entry| {
                        entry
                            .value()                         // &Vec<(SymbolId, NaiveDate, Tz)>
                            .iter()
                            .find(|(sid, _, _)| sid.0.eq_ignore_ascii_case(&id.0))
                            .map(|(_, d, _)| *d)             // <-- copy the NaiveDate out
                    })
                    .or_else(|| parse_expiry_from_contract_code(&id.0));

                (root, expiry)
            }
        };

        if let Some(pricing) = self.pricing_by_root.get(&root) {
            let hrs = hours_for_exchange(exchange);

            let sec = self.build_from_parts(id, &root,exchange, expiry, &pricing, &hrs);
            return Ok(sec);
        }

        // If not warmed, fall back to the provider’s generic path
        self.uni
            .build_security(id, exchange, self.models.clone(), hours_for_exchange(exchange), self.vendor.clone())
            .await
    }

    async fn prefetch(&self, _ids: &[SymbolId], exchange: Exchange) -> anyhow::Result<()> {
        // We ignore the ids and prefetch the entire exchange once.
        // If you want to respect `_ids`, you can filter to those roots instead.
        self.prefetch_all(exchange).await
    }
}