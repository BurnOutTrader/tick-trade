use std::{fs, path::{Path, PathBuf}};
use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use crate::models::{BboRow, CandleRow, DataKind, TickRow};
use crate::duck::upsert_partition;
use standard_lib::market_data::base_data::{OrderBook, Resolution};
use crate::catalog::ensure_dataset;
use crate::helpers::partition_dir_for_day;
use crate::parquet::{write_bbo_zstd, write_candles_zstd, write_ticks_zstd};
// ------------------------------
// Shared time utils
// ------------------------------

// ------------------------------
// TICKS
// ------------------------------

pub fn persist_ticks_partition_zstd(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    day: NaiveDate,
    rows_vec: &[TickRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_ticks_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Tick, None)?;

    let dir = partition_dir_for_day(data_root, provider, symbol, "tick", day);
    fs::create_dir_all(&dir)?;
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fname = format!("ticks-{}-{}.parquet", ts_ns, nanoid::nanoid!(6));
    let out_path = dir.join(fname);

    write_ticks_zstd(&out_path, rows_vec, zstd_level)?;

    let rows  = rows_vec.len() as i64;
    let bytes = fs::metadata(&out_path)?.len() as i64;

    // bounds from ns fields
    let (mut min_ns, mut max_ns) = (i64::MAX, i64::MIN);
    let (mut min_seq, mut max_seq): (Option<i64>, Option<i64>) = (None, None);

    for r in rows_vec {
        let ns = r.key_ts_utc_ns;
        if ns < min_ns { min_ns = ns; }
        if ns > max_ns { max_ns = ns; }

        if let Some(vs) = r.venue_seq {
            let v = vs as i64;
            min_seq = Some(min_seq.map_or(v, |m| m.min(v)));
            max_seq = Some(max_seq.map_or(v, |m| m.max(v)));
        }
    }

    let min_ts = ns_to_dt(min_ns);
    let max_ts = ns_to_dt(max_ns);

    upsert_partition(
        conn, dataset_id,
        out_path.to_string_lossy().as_ref(),
        "parquet", rows, bytes,
        min_ts, max_ts,
        min_seq, max_seq,
        Some(day),
    )?;

    Ok(out_path)
}

// ------------------------------
// CANDLES (use time_end for max)
// ------------------------------

pub fn persist_candles_partition_zstd(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    resolution: Resolution,
    day: NaiveDate,
    rows_vec: &[CandleRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_candles_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Candle, Some(resolution))?;

    let dir = partition_dir_for_day(data_root, provider, symbol, "candle", day);
    fs::create_dir_all(&dir)?;
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fname = format!("candles-{}-{}.parquet", ts_ns, nanoid::nanoid!(6));
    let out_path = dir.join(fname);

    write_candles_zstd(&out_path, rows_vec, zstd_level)?;

    let rows  = rows_vec.len() as i64;
    let bytes = fs::metadata(&out_path)?.len() as i64;

    let (mut min_ns, mut max_ns) = (i64::MAX, i64::MIN);
    for r in rows_vec {
        if r.time_start_ns < min_ns { min_ns = r.time_start_ns; }
        if r.time_end_ns   > max_ns { max_ns = r.time_end_ns; } // **end governs max**
    }
    let min_ts = ns_to_dt(min_ns);
    let max_ts = ns_to_dt(max_ns);

    upsert_partition(
        conn, dataset_id,
        out_path.to_string_lossy().as_ref(),
        "parquet", rows, bytes,
        min_ts, max_ts,
        None, None,
        Some(day),
    )?;

    Ok(out_path)
}

// ------------------------------
// BBO
// ------------------------------

pub fn persist_bbo_partition_zstd(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    day: NaiveDate,
    rows_vec: &[BboRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_bbo_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Bbo, None)?;

    let dir = partition_dir_for_day(data_root, provider, symbol, "bbo", day);
    fs::create_dir_all(&dir)?;
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fname = format!("bbo-{}-{}.parquet", ts_ns, nanoid::nanoid!(6));
    let out_path = dir.join(fname);

    write_bbo_zstd(&out_path, rows_vec, zstd_level)?;

    let rows  = rows_vec.len() as i64;
    let bytes = fs::metadata(&out_path)?.len() as i64;

    let (mut min_ns, mut max_ns) = (i64::MAX, i64::MIN);
    for r in rows_vec {
        let ns = r.key_ts_utc_ns;
        if ns < min_ns { min_ns = ns; }
        if ns > max_ns { max_ns = ns; }
    }
    let min_ts = ns_to_dt(min_ns);
    let max_ts = ns_to_dt(max_ns);

    upsert_partition(
        conn, dataset_id,
        out_path.to_string_lossy().as_ref(),
        "parquet", rows, bytes,
        min_ts, max_ts,
        None, None,
        Some(day),
    )?;

    Ok(out_path)
}

// ------------------------------
// ORDER BOOKS (JSON ladders)
// ------------------------------
//
// If you already added `write_orderbooks_partition(...)` that writes and
// registers, you can keep that. If you prefer symmetry with the others,
// here is a similar writer that uses DuckDB COPY over a temp table.
//
// If you keep your existing `write_orderbooks_partition`, you can skip this.

pub fn persist_books_partition_duckdb(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    day: NaiveDate,
    snapshots: &[OrderBook],   // uses your JSON ladder approach inside
    data_root: &Path,
    zstd_level: i32,           // not used directly by COPY, included for parity
) -> Result<PathBuf> {
    if snapshots.is_empty() {
        return Err(anyhow!("persist_books_partition_duckdb: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Book, None)?;

    let dir = partition_dir_for_day(data_root, provider, symbol, "book", day);
    fs::create_dir_all(&dir)?;
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fname = format!("books-{}-{}.parquet", ts_ns, nanoid::nanoid!(6));
    let out_path = dir.join(fname);

    // temp table & insert rows (as in your earlier write helper)
    conn.execute_batch(
        r#"
        create temp table if not exists _tmp_books_ingest (
            symbol     TEXT,
            exchange   TEXT,
            time       TIMESTAMP,
            bids_json  TEXT,
            asks_json  TEXT
        );
        delete from _tmp_books_ingest;
        "#,
    )?;

    let mut ins = conn.prepare(
        "insert into _tmp_books_ingest(symbol, exchange, time, bids_json, asks_json)
         values (?, ?, to_timestamp(?), ?, ?)",
    )?;

    let mut rows = 0i64;
    let mut min_ts: Option<DateTime<Utc>> = None;
    let mut max_ts: Option<DateTime<Utc>> = None;

    for ob in snapshots {
        // JSON-encode as compact [[px,sz],...] with Decimal to string
        let bids_arr: Vec<_> = ob.bids.iter().map(|(p,s)| serde_json::json!([p.to_string(), s.to_string()])).collect();
        let asks_arr: Vec<_> = ob.asks.iter().map(|(p,s)| serde_json::json!([p.to_string(), s.to_string()])).collect();
        let bids_json = serde_json::to_string(&bids_arr)?;
        let asks_json = serde_json::to_string(&asks_arr)?;
        let exch_str = format!("{:?}", ob.exchange);

        ins.execute(duckdb::params![
            &ob.symbol,
            &exch_str,
            ob.time.to_rfc3339(),
            &bids_json,
            &asks_json
        ])?;

        rows += 1;
        min_ts = Some(min_ts.map_or(ob.time, |t| t.min(ob.time)));
        max_ts = Some(max_ts.map_or(ob.time, |t| t.max(ob.time)));
    }

    // COPY with zstd compression
    let out_str = format!("'{}'", out_path.to_string_lossy().replace('\'', "''"));
    let sql = format!("copy _tmp_books_ingest to {} (format parquet, compression 'zstd');", out_str);
    conn.execute_batch(&sql)?;

    let md = fs::metadata(&out_path)?;
    let bytes = md.len() as i64;

    upsert_partition(
        conn,
        dataset_id,
        out_path.to_string_lossy().as_ref(),
        "parquet",
        rows,
        bytes,
        min_ts.unwrap(),
        max_ts.unwrap(),
        None,
        None,
        Some(day),
    )?;

    Ok(out_path)
}

#[inline]
fn ns_to_dt(ns: i64) -> chrono::DateTime<chrono::Utc> {
    let secs  = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)
        .expect("valid ns timestamp")
}