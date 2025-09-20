use std::path::{Path, PathBuf};
use chrono::NaiveDate;
use duckdb::Connection;
use standard_lib::market_data::base_data::Resolution;
use crate::catalog::{get_or_create_dataset_id, get_or_create_provider_id, get_or_create_symbol_id};
use crate::models::DataKind;


pub fn dataset_id_ticks(conn: &Connection, provider: &str, symbol: &str) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::Tick, None)
}

pub fn dataset_id_bbo(conn: &Connection, provider: &str, symbol: &str) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::Bbo, None)
}

pub fn dataset_id_books(conn: &Connection, provider: &str, symbol: &str) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::Book, None)
}

pub fn dataset_id_candles(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    res: Resolution,
) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::Candle, Some(res))
}

/// Build a HIVE-like partition dir:
///   <root>/provider=<p>/symbol=<s>/kind=<k>/day=YYYY-MM-DD
pub fn partition_dir_for_day(
    root: &Path,
    provider: &str,
    symbol: &str,
    kind_key: &str,       // "tick" | "bbo" | "candle" | "book"
    day: NaiveDate,
) -> PathBuf {
    root.join(format!("provider={provider}"))
        .join(format!("symbol={symbol}"))
        .join(format!("kind={kind_key}"))
        .join(format!("day={day}"))
}

