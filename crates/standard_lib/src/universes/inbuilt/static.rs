use std::sync::Arc;
use ahash::{AHashMap, AHashSet};
use chrono::{DateTime, Utc};
use crate::securities::symbols::{Exchange, SymbolId};
use crate::universes::models::{SelectionSchedule, Universe, UniversePlan};


pub struct OneSymbolUniverse {
    pub vendor: String,
    pub id: SymbolId,
    pub exchange: Exchange,
}
impl Universe for OneSymbolUniverse {
    fn select(&self, _asof: DateTime<Utc>) -> AHashMap<String, AHashSet<SymbolId>> {
        let set = [self.id.clone()].into_iter().collect::<AHashSet<_>>();
        [(self.vendor.clone(), set)].into_iter().collect()
    }
    fn name(&self) -> &'static str { "OneSymbol" }

    fn exchange(&self) -> Exchange {
        self.exchange
    }
}

pub fn plan_one_symbol(id: SymbolId, exchange: Exchange, vendor :String) -> UniversePlan {
    UniversePlan {
        name: "OneSymbol",
        universe: Arc::new(OneSymbolUniverse { id, exchange, vendor }),
        schedule: SelectionSchedule::Never, // run once on first step
    }
}

/// A universe that always returns a fixed set of symbols.
pub struct StaticSetUniverse {
    pub vendor: String,
    pub ids: Vec<SymbolId>,
    pub exchange: Exchange,
}
impl Universe for StaticSetUniverse {
    fn select(&self, _asof: DateTime<Utc>) -> AHashMap<String, AHashSet<SymbolId>> {
        let set: AHashSet<SymbolId> = self.ids.iter().cloned().collect();
        std::iter::once((self.vendor.clone(), set)).collect()
    }
    fn name(&self) -> &'static str { "StaticSet" }

    fn exchange(&self) -> Exchange {
        self.exchange
    }
}


// Build a fixed-set static plan
pub fn plan_static_set(ids: Vec<SymbolId>, exchange: Exchange, vendor: String) -> UniversePlan {
    UniversePlan {
        name: "StaticSet",
        universe: Arc::new(StaticSetUniverse { ids, exchange, vendor}),
        schedule: SelectionSchedule::Never, // ← runs once, never again
    }
}
