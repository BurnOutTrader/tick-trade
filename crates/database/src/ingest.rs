use anyhow::{Context, Result};
use chrono::NaiveDate;
use duckdb::Connection;
use std::path::Path;
use super::paths::{Layout, Partitioning};
use super::append::append_merge_parquet;

/// Append a day’s worth of *partial* data (e.g., we had 50%, now add the rest) for Ticks/BBO.
/// `key_cols` must uniquely identify a row for the target dataset.
/// `order_by` sets final on-disk order (usually begins with your time column).
///
/// `new_batch_parquet` can be a freshly downloaded chunk you saved in a temp file.
///
/// Example keys:
///  - Ticks:   ["time","venue_seq","exec_id"]
///  - BBO:     ["time","venue_seq"]
///  - Candles: ["time_start","resolution"]  (if vendor bar feed can repeat, include a sequence)
pub fn append_day(
    conn: &Connection,
    layout: &Layout,
    provider: &str,
    symbol: &str,
    data_kind: &str,
    resolution: &str,
    day: NaiveDate,
    key_cols: &[&str],
    order_by: &[&str],
    new_batch_parquet: &Path,
) -> Result<()> {
    let target = layout.file_for(provider, symbol, data_kind, resolution, day, Partitioning::Daily);
    super::paths::Layout::ensure_parent_dirs(&target)?;
    append_merge_parquet(conn, &target, new_batch_parquet, key_cols, order_by)
}

/// Same idea for yearly partitions (e.g., daily bars).
pub fn append_year(
    conn: &Connection,
    layout: &Layout,
    provider: &str,
    symbol: &str,
    data_kind: &str,
    resolution: &str,
    year: i32,
    key_cols: &[&str],
    order_by: &[&str],
    new_batch_parquet: &Path,
) -> Result<()> {
    let day = NaiveDate::from_ymd_opt(year, 1, 1).context("bad year")?;
    let target = layout.file_for(provider, symbol, data_kind, resolution, day, Partitioning::Yearly);
    super::paths::Layout::ensure_parent_dirs(&target)?;
    append_merge_parquet(conn, &target, new_batch_parquet, key_cols, order_by)
}

/// And for single-file datasets (e.g., weekly bars).
pub fn append_single(
    conn: &Connection,
    layout: &Layout,
    provider: &str,
    symbol: &str,
    data_kind: &str,
    resolution: &str,
    key_cols: &[&str],
    order_by: &[&str],
    new_batch_parquet: &Path,
) -> Result<()> {
    let day = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let target = layout.file_for(provider, symbol, data_kind, resolution, day, Partitioning::Single);
    super::paths::Layout::ensure_parent_dirs(&target)?;
    append_merge_parquet(conn, &target, new_batch_parquet, key_cols, order_by)
}