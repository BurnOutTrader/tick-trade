use std::{fs, path::{Path, PathBuf}};
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, Utc};
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
    day: chrono::NaiveDate,
    snapshots: &[OrderBook],
    data_root: &std::path::Path,
    zstd_level: i32, // optional; see PRAGMA notes below
) -> anyhow::Result<std::path::PathBuf> {
    use std::{fs, path::PathBuf};
    use chrono::Utc;

    if snapshots.is_empty() {
        anyhow::bail!("persist_books_partition_duckdb: empty batch");
    }

    // 1) dataset
    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Book, None)?;

    // 2) unique path
    let dir = partition_dir_for_day(data_root, provider, symbol, "book", day);
    fs::create_dir_all(&dir)?;
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fname = format!("books-{}-{}.parquet", ts_ns, nanoid::nanoid!(6));
    let out_path: PathBuf = dir.join(fname);

    // 3) staging table with BIGINT ns time
    conn.execute_batch(
        r#"
        create temp table if not exists _tmp_books_ingest (
            symbol     TEXT,
            exchange   TEXT,
            time_ns    BIGINT,    -- epoch ns, NOT TIMESTAMP
            bids_json  TEXT,
            asks_json  TEXT
        );
        delete from _tmp_books_ingest;
        "#,
    )?;

    let mut ins = conn.prepare(
        "insert into _tmp_books_ingest(symbol, exchange, time_ns, bids_json, asks_json)
         values (?, ?, ?, ?, ?)",
    )?;

    let mut rows = 0i64;
    let mut min_ns: Option<i64> = None;
    let mut max_ns: Option<i64> = None;

    for ob in snapshots {
        // encode ladders as compact [[price,size], ...] strings
        let bids_arr: Vec<_> = ob.bids.iter()
            .map(|(p,s)| serde_json::json!([p.to_string(), s.to_string()]))
            .collect();
        let asks_arr: Vec<_> = ob.asks.iter()
            .map(|(p,s)| serde_json::json!([p.to_string(), s.to_string()]))
            .collect();
        let bids_json = serde_json::to_string(&bids_arr)?;
        let asks_json = serde_json::to_string(&asks_arr)?;
        let exch_str = format!("{:?}", ob.exchange);

        // epoch ns (i128 -> i64 clamp: your times should be safe within i64 epoch ns range)
        let t_ns = ob.time.timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("invalid timestamp in OrderBook"))? as i64;

        ins.execute(duckdb::params![
            &ob.symbol,
            &exch_str,
            t_ns,
            &bids_json,
            &asks_json
        ])?;

        rows += 1;

        let t_ns = t_ns;
        min_ns = Some(min_ns.map_or(t_ns, |m| m.min(t_ns)));
        max_ns = Some(max_ns.map_or(t_ns, |m| m.max(t_ns)));
    }

    // 4) COPY to Parquet with zstd (optionally set level if supported)
    // Some DuckDB builds expose these pragmas; harmless to skip if not supported.
    // If you see an error, remove the two PRAGMA lines below.
    let out_str = out_path.to_string_lossy().replace('\'', "''");
    conn.execute_batch(&format!(
        r#"
        PRAGMA parquet_compression='zstd';
        PRAGMA parquet_zstd_compression_level={level};

        copy _tmp_books_ingest to '{path}'
        (format parquet, compression 'zstd');
        "#,
        level = zstd_level,
        path  = out_str,
    ))?;

    // 5) file stats
    let md = fs::metadata(&out_path)?;
    let bytes = md.len() as i64;

    // 6) catalog upsert (convert ns -> DateTime<Utc> using your helper)
    let min_ts = ns_to_dt(min_ns.expect("min_ns"));
    let max_ts = ns_to_dt(max_ns.expect("max_ns"));

    upsert_partition(
        conn,
        dataset_id,
        out_path.to_string_lossy().as_ref(),
        "parquet",
        rows,
        bytes,
        min_ts,
        max_ts,
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