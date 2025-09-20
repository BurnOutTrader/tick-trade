use std::{fs, path::{Path, PathBuf}};
use std::collections::BTreeMap;
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, Utc, DateTime};
use duckdb::Connection;
use standard_lib::market_data::base_data::{OrderBook, Resolution};
use crate::models::{BboRow, CandleRow, DataKind};
use crate::duck::{upsert_provider, upsert_symbol, upsert_dataset, upsert_partition};
use chrono::Datelike;
use crate::models::TickRow;
use crate::parquet::write_ticks_zstd;
use crate::perist::{persist_bbo_partition_zstd, persist_books_partition_duckdb, persist_candles_partition_zstd};
// your row type with *_ns fields

// --- tiny local helpers ---

#[inline]
fn ns_to_dt(ns: i64) -> DateTime<Utc> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .expect("valid UTC timestamp from ns")
}

// data_root/{provider}/{symbol}/tick/YYYY/MM/DD/
fn partition_dir_for_day(
    data_root: &Path,
    provider: &str,
    symbol: &str,
    kind_dir: &str,
    day: NaiveDate,
) -> PathBuf {
    data_root
        .join(provider)
        .join(symbol)
        .join(kind_dir)
        .join(format!("{:04}", day.year()))
        .join(format!("{:02}", day.month()))
        .join(format!("{:02}", day.day()))
}

// --- the one call you make ---

/// Ingest a batch of ticks for a single UTC day:
/// - writes Parquet (ZSTD)
/// - updates catalog (`providers`, `symbols`, `datasets`, `partitions`)
/// - returns the file path written
pub fn ingest_ticks_day(
    conn: &Connection,
    data_root: &Path,
    provider: &str,
    symbol: &str,
    day: NaiveDate,
    rows: &[TickRow],
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows.is_empty() {
        return Err(anyhow!("ingest_ticks_day: empty batch"));
    }

    // 1) ensure catalog ids
    let provider_id = upsert_provider(conn, provider)?;
    let symbol_id   = upsert_symbol(conn, provider_id, symbol)?;
    let dataset_id  = upsert_dataset(conn, provider_id, symbol_id, DataKind::Tick, None)?;

    // 2) build unique output path under day partition
    let dir = partition_dir_for_day(data_root, provider, symbol, "tick", day);
    fs::create_dir_all(&dir)?;
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let fname = format!("ticks-{}-{}.parquet", ts_ns, nanoid::nanoid!(6));
    let out_path = dir.join(fname);

    // 3) write parquet
    write_ticks_zstd(&out_path, rows, zstd_level)?;

    // 4) stats for catalog (nanoseconds → DateTime<Utc>)
    let rows_cnt = rows.len() as i64;
    let bytes = fs::metadata(&out_path)?.len() as i64;

    // TickRow must have nanosecond sort key: `key_ts_utc_ns`
    let mut min_ns = i64::MAX;
    let mut max_ns = i64::MIN;

    // Optional monotonic sequence bounds (if present)
    let mut min_seq: Option<i64> = None;
    let mut max_seq: Option<i64> = None;

    for r in rows {
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

    // 5) upsert partition row (idempotent / widening)
    upsert_partition(
        conn,
        dataset_id,
        out_path.to_string_lossy().as_ref(),
        "parquet",
        rows_cnt,
        bytes,
        min_ts,
        max_ts,
        min_seq,
        max_seq,
        Some(day),
    )?;

    Ok(out_path)
}

#[inline]
fn ns_to_utc_day(ns: i64) -> Result<NaiveDate> {
    let secs  = ns.div_euclid(1_000_000_000);
    let nsec  = (ns.rem_euclid(1_000_000_000)) as u32;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nsec)
        .ok_or_else(|| anyhow!("invalid ns timestamp: {ns}"))?;
    Ok(dt.date_naive())
}

/// Group a slice by UTC day derived from the provided extractor.
fn group_by_day<T, F>(rows: &[T], ts_ns: F) -> Result<BTreeMap<NaiveDate, Vec<&T>>>
where
    F: Fn(&T) -> i64,
{
    let mut buckets: BTreeMap<NaiveDate, Vec<&T>> = BTreeMap::new();
    for r in rows {
        let d = ns_to_utc_day(ts_ns(r))?;
        buckets.entry(d).or_default().push(r);
    }
    Ok(buckets)
}

/// One-call ingestion for **candles** (uses candle **end time** for day bucketing).
pub fn ingest_candles(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str, // carried inside rows but passed for symmetry with ticks API if you have one
    resolution: Resolution, // which candle dataset
    all_rows: &[CandleRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<Vec<PathBuf>> {
    // Bucket by UTC day from time_end_ns (policy choice).
    let buckets = group_by_day(all_rows, |c| c.time_end_ns)?;
    let mut out_paths = Vec::with_capacity(buckets.len());

    for (day, rows) in buckets {
        // rows is Vec<&CandleRow> → make a contiguous slice view
        // persist_* expects &[CandleRow]; collect into a small Vec<&>→Vec<CandleRow> clone-free?
        // We must pass owned slice; do a lightweight copy if you want (these are on-disk types).
        // If you prefer zero-copy, change persist_* to accept iter of &CandleRow.
        let mut scratch: Vec<CandleRow> = Vec::with_capacity(rows.len());
        for r in rows { scratch.push(r.clone()); }

        let p = persist_candles_partition_zstd(
            conn, provider, symbol, resolution, day, &scratch, data_root, zstd_level
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}

/// One-call ingestion for **BBO** snapshots.
pub fn ingest_bbo(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    _exchange: &str,
    all_rows: &[BboRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<Vec<PathBuf>> {
    let buckets = group_by_day(all_rows, |q| q.key_ts_utc_ns)?;
    let mut out_paths = Vec::with_capacity(buckets.len());

    for (day, rows) in buckets {
        let mut scratch: Vec<BboRow> = Vec::with_capacity(rows.len());
        for r in rows { scratch.push(r.clone()); }

        let p = persist_bbo_partition_zstd(
            conn, provider, symbol, day, &scratch, data_root, zstd_level
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}

/// One-call ingestion for **order-book** snapshots.
/// (This uses your DuckDB temp-table + COPY writer.)
pub fn ingest_books(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    all_snaps: &[OrderBook],
    data_root: &Path,
    zstd_level: i32,
) -> Result<Vec<PathBuf>> {
    // Bucket by UTC day using OrderBook.time
    let mut buckets: BTreeMap<NaiveDate, Vec<&OrderBook>> = BTreeMap::new();
    for ob in all_snaps {
        buckets.entry(ob.time.date_naive()).or_default().push(ob);
    }

    let mut out_paths = Vec::with_capacity(buckets.len());
    for (day, snaps) in buckets {
        // persist_* expects &[OrderBook]; build a scratch Vec<OrderBook> (OrderBook is small)
        let mut scratch: Vec<OrderBook> = Vec::with_capacity(snaps.len());
        for s in snaps { scratch.push(s.clone()); }

        let p = persist_books_partition_duckdb(
            conn, provider, symbol, day, &scratch, data_root, zstd_level
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}