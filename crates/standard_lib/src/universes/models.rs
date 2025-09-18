use std::sync::Arc;
use ahash::{AHashMap, AHashSet};
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use crate::market_data::base_data::Resolution;
use crate::securities::symbols::{Exchange, SymbolId};

#[derive(Clone, Copy, Debug)]
pub enum SelectionSchedule {
    /// Run every fixed wall-clock duration (live) or sim time step (backtest)
    Every(Duration),

    /// Run when a bar boundary is hit for this resolution (e.g. 1m, 5m, 1h)
    OnBar(Resolution),

    /// Every day at a specific hour (e.g. 16, 8) in the specified timezone(Tz)
    EveryDayAtHour{hour: u8, time_zone: Tz},

    /// Manual (you’ll call Engine::poke_strategy if you want)
    Manual,

    /// Never change subscriptions after initial selection.
    Never,
}

#[derive(Clone)]
pub struct UniversePlan {
    pub name: &'static str,
    pub schedule: SelectionSchedule,
    pub universe: Arc<dyn Universe>,
}

#[derive(Debug, Clone)]
pub struct SelectionDiff { pub added: Vec<(String, SymbolId,Exchange)>, pub removed: Vec<(String, SymbolId, Exchange)> }

impl SelectionDiff {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

pub trait Universe: Send + Sync {
    fn select(&self, asof: DateTime<Utc>) -> AHashMap<String, AHashSet<SymbolId>>;
    fn name(&self) -> &'static str;
    fn exchange(&self) -> Exchange;
}

pub struct UniverseHandle {
    pub name: &'static str,
    pub exch: Exchange,
    pub last: AHashMap<String, AHashSet<SymbolId>>,
    pub inner: Arc<dyn Universe>
}
impl UniverseHandle {
    pub fn run_selection(&mut self, asof: DateTime<Utc>) -> SelectionDiff {
        let next: AHashMap<String, AHashSet<SymbolId>> = self.inner.select(asof);

        let mut added:   Vec<(String, SymbolId, Exchange)> = Vec::new();
        let mut removed: Vec<(String, SymbolId, Exchange)> = Vec::new();

        // Vendors present in `next`
        for (vendor, next_set) in &next {
            let empty = AHashSet::new();
            let last_set = self.last.get(vendor).unwrap_or(&empty);

            // newly added for this vendor
            for id in next_set.difference(last_set) {
                added.push((vendor.clone(), id.clone(), self.exch));
            }
            // removed for this vendor (still present vendor key, but fewer ids)
            for id in last_set.difference(next_set) {
                removed.push((vendor.clone(), id.clone(), self.exch));
            }
        }

        // Vendors that disappeared entirely since last tick
        for (vendor, last_set) in &self.last {
            if !next.contains_key(vendor) {
                for id in last_set {
                    removed.push((vendor.clone(), id.clone(), self.exch));
                }
            }
        }

        // Commit new snapshot
        self.last = next;

        SelectionDiff { added, removed }
    }
}

pub struct UniverseEngine { pub universes: std::collections::BTreeMap<&'static str, UniverseHandle> }
impl UniverseEngine {
    pub fn new() -> Self { Self { universes: std::collections::BTreeMap::new() } }
    pub fn add(&mut self, u: UniverseHandle) { self.universes.insert(u.name, u); }
    pub fn step(&mut self, asof: DateTime<Utc>) -> Vec<(&'static str, SelectionDiff)> {
        let mut out = Vec::new();
        for (name, u) in self.universes.iter_mut() {
            let diff = u.run_selection(asof);
            if !diff.is_empty() {
                out.push((*name, diff));
            }
        }
        out
    }
}