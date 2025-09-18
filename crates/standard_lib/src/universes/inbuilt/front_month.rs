//! Front-month futures universe
//!
//! Picks, for each root (e.g., "ES", "NQ", "CL"), the nearest non-expired
//! contract on a given `Exchange`. It is *purely registry-driven*: it never
//! calls vendors and never blocks.
//!
//! Requirements:
//! - `SecurityRegistry` exposes `fn snapshot(&self) -> Vec<Arc<Security>>`
//!   that returns a cheap snapshot of known instruments.
//! - `Security` has `security_type`, `exchange`, `expiry: Option<NaiveDate>`,
//!   and `is_continuous: bool`.
//!
//! Strategy wiring:
//!   let uni = FrontMonthUniverse::new(reg.clone(), Exchange::CME)
//!       .with_roots(["ES","NQ","YM"])           // optional: restrict to these
//!       .include_continuous(false);             // default false
//!
//!   let plan = UniversePlan {
//!       name: "FrontMonth",
//!       schedule: SelectionSchedule::Every(chrono::Duration::minutes(1)),
//!       universe: Arc::new(uni),
//!   };

use std::collections::HashMap;
use std::sync::Arc;
use chrono::{NaiveDate, Utc, DateTime};
use ahash::{AHashMap, AHashSet};
use crate::securities::registry::SecurityRegistry;
use crate::securities::security::Security;
use crate::securities::symbols::{Exchange, SecurityType, SymbolId};
use crate::universes::models::Universe;

#[allow(dead_code)]
/// Utility: CME month-code letters so we can derive a "root" from a canonical
/// symbol like "MNQZ25". If your `SymbolId` encodes roots differently,
/// swap this logic for a proper parser.
const MONTH_CODES: [char; 12] = ['F','G','H','J','K','M','N','Q','U','V','X','Z'];

#[allow(dead_code)]
/// Extract a "root" from a canonical futures symbol string by scanning for the
/// last month-code letter and taking everything *before* it.
/// Examples:
///   "ESZ25"   -> Some("ES")
///   "MNQZ5"   -> Some("MNQ")
///   "GCJ2026" -> Some("GC")
fn derive_root_from_symbol(sym: &str) -> Option<String> {
    let chars: Vec<char> = sym.chars().collect();
    for (i, c) in chars.iter().enumerate().rev() {
        if MONTH_CODES.contains(c) {
            // ensure there are digits after the month code (2 or 4)
            let tail: String = chars.iter().skip(i + 1).collect();
            let digits: String = tail.chars().take_while(|c| c.is_ascii_digit()).collect();
            if digits.len() == 2 || digits.len() == 4 {
                return Some(chars.iter().take(i).collect());
            }
        }
    }
    None
}

#[allow(dead_code)]
/// Front-month universe (registry-driven).
#[derive(Clone)]
pub struct FrontMonthUniverse {
    vendor: String,
    registry: Arc<SecurityRegistry>,
    exchange: Exchange,
    roots: Option<AHashSet<String>>,
    include_continuous: bool,
    name: &'static str,
}

#[allow(dead_code)]
impl FrontMonthUniverse {
    pub fn new(vendor: String, registry: Arc<SecurityRegistry>, exchange: Exchange) -> Self {
        Self {
            vendor,
            registry,
            exchange,
            roots: None,
            include_continuous: false,
            name: "FrontMonth",
        }
    }

    /// Restrict the universe to a fixed set of product roots (e.g., ["ES","NQ"]).
    pub fn with_roots<I, S>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut set = AHashSet::new();
        for s in iter { set.insert(s.into()); }
        self.roots = Some(set);
        self
    }

    /// Whether to include continuous contracts in selection (default: false).
    pub fn include_continuous(mut self, yes: bool) -> Self {
        self.include_continuous = yes;
        self
    }

    /// Override the universe name (useful if you instantiate multiple variants).
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// Decide if a security is eligible for front-month consideration.
    fn eligible(&self, sec: &Security, today: NaiveDate, root_filter: &Option<AHashSet<String>>) -> bool {
        if sec.security_type != SecurityType::Future { return false; }
        if sec.exchange != self.exchange { return false; }
        if !self.include_continuous && sec.is_continuous() { return false; }

        // Need an expiry in the future or today
        let Some(exp) = sec.expiry() else { return false; };
        if exp < today { return false; }

        // If filtering by roots, derive a root from the canonical id
        if let Some(filter) = root_filter {
            let rid = &sec.id.0;
            match derive_root_from_symbol(rid) {
                Some(root) => filter.contains(&root),
                None => false,
            }
        } else {
            true
        }
    }
}

impl Universe for FrontMonthUniverse {
    fn select(&self, asof: DateTime<Utc>) -> AHashMap<String, AHashSet<SymbolId>> {
        let today = asof.date_naive();

        // You said `SecurityRegistry::snapshot()` returns `Vec<Arc<Security>>`
        let all: Vec<Arc<Security>> = self.registry.snapshot();

        // Track, per inferred root, the *front-month* (earliest expiry) security.
        // Store just what we need to avoid borrow juggling.
        let mut best: HashMap<String, (SymbolId, NaiveDate)> = HashMap::new();

        for sec in &all {
            if !self.eligible(sec, today, &self.roots) { continue; }

            let Some(root) = derive_root_from_symbol(&sec.id.0) else { continue; };
            let Some(exp)  = sec.expiry() else { continue; };

            use std::collections::hash_map::Entry;
            match best.entry(root) {
                Entry::Vacant(v) => {
                    v.insert((sec.id.clone(), exp));
                }
                Entry::Occupied(mut o) => {
                    if exp < o.get().1 {
                        o.insert((sec.id.clone(), exp));
                    }
                }
            }
        }

        // Build the vendor→set map (single vendor here; extend if you support many).
        let set: AHashSet<SymbolId> = best.into_values().map(|(id, _)| id).collect();
        std::iter::once((self.vendor.clone(), set)).collect()
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn exchange(&self) -> Exchange {
        self.exchange
    }
}