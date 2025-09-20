use chrono::{NaiveDate, DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::str::FromStr;
use strum_macros::Display;
use rust_decimal::Decimal;

// ---------- Disambiguation key for same-timestamp events ----------
// Store time in microseconds to align with Parquet/Arrow, plus a tie-breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventKey {
    pub ts_utc_us: i64,   // UTC micros since epoch
    pub tie: u32,         // sequence within same ts (venue_seq or synthetic)
}

// ---------- Core records (storage-friendly shapes) ----------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickRow {
    pub provider: String,         // "rithmic", "databento", etc.
    pub symbol_id: String,        // canonical
    pub exchange: String,         // short code
    pub price: f64,               // use f64 on disk; convert to Decimal in calc path
    pub size: f64,
    pub side: u8,                 // 0 None, 1 Buy, 2 Sell
    pub key_ts_utc_us: i64,       // sort first
    pub key_tie: u32,             // then tie-breaker
    // optional light metadata for ordering/audit
    pub venue_seq: Option<u32>,
    pub exec_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandleRow {
    pub provider: String,
    pub symbol_id: String,
    pub exchange: String,
    pub res: String,            // normalized textual (e.g. "S1", "M5", "D", "W", "TBAR_1000")
    pub time_start_us: i64,
    pub time_end_us: i64,
    pub open: f64, pub high: f64, pub low: f64, pub close: f64,
    pub volume: f64,
    pub ask_volume: f64,
    pub bid_volume: f64,
    pub num_trades: u64,
}

// Simple BBO snapshot
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BboRow {
    pub provider: String,
    pub symbol_id: String,
    pub exchange: String,
    pub key_ts_utc_us: i64,
    pub bid: f64, pub bid_size: f64,
    pub ask: f64, pub ask_size: f64,
    pub bid_orders: Option<u32>,
    pub ask_orders: Option<u32>,
    pub venue_seq: Option<u32>,
    pub is_snapshot: Option<bool>,
}

// ---------- Catalog entities ----------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Provider { pub provider: String, pub version: Option<String> }

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolMeta {
    pub symbol_id: String,
    pub security: String,
    pub exchange: String,
    pub currency: String,
    pub root: Option<String>,          // for futures
    pub continuous_of: Option<String>, // if this is a continuous symbol
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UniverseMember {
    pub universe: String,      // e.g. "CME_MICROS"
    pub symbol_id: String,
    pub provider: String,
}