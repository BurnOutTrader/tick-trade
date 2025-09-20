use anyhow::{Context, Result};
use duckdb::{Connection};
use tempfile::NamedTempFile;
use std::path::{Path, PathBuf};
use std::fs;

/// Atomic “append” that never loses existing data:
/// 1) We read existing parquet + new batch parquet.
/// 2) UNION ALL and de-dup by a composite key (your choice).
/// 3) Sort by a stable ordering (e.g., ts, venue_seq, exec_id).
/// 4) Write to tmp with max compression.
/// 5) Atomic replace.
///
/// Notes:
/// • If `existing_path` doesn’t exist, we just sort/dedup the new batch and move it in.
/// • `key_cols` MUST uniquely identify a row (e.g. ["time","venue_seq","exec_id"] for ticks).
/// • `order_by` defines on-disk order (improves scan locality).
pub fn append_merge_parquet(
    conn: &Connection,
    existing_path: &Path,
    new_batch_parquet: &Path,
    key_cols: &[&str],
    order_by: &[&str],
) -> Result<()> {
    // Ensure dirs for final location exist
    if let Some(parent) = existing_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let key_list = key_cols.join(", ");
    let order_list = if order_by.is_empty() {
        key_list.clone()
    } else {
        order_by.join(", ")
    };

    // When file doesn't exist yet, we can just normalize + move.
    let exists = existing_path.exists();

    // Build SQL that:
    //  - Reads existing (if any) and new batch
    //  - UNION ALL, then pick first row per key via ROW_NUMBER
    //  - ORDER BY desired order
    //
    // DuckDB quirk: read_parquet() over a missing file errors, so we branch in Rust.
    let select_union = if exists {
        format!(
            "WITH existing AS (SELECT * FROM read_parquet('{}')),
                  incoming AS (SELECT * FROM read_parquet('{}')),
                  unioned AS (SELECT * FROM existing UNION ALL SELECT * FROM incoming),
                  ranked AS (
                    SELECT u.*,
                           ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {keys}) AS rn
                    FROM unioned u
                  )
             SELECT * FROM ranked WHERE rn=1 ORDER BY {order_by}",
            existing_path.display(),
            new_batch_parquet.display(),
            keys = key_list,
            order_by = order_list
        )
    } else {
        format!(
            "WITH incoming AS (SELECT * FROM read_parquet('{}')),
                  ranked AS (
                    SELECT i.*,
                           ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {keys}) AS rn
                    FROM incoming i
                  )
             SELECT * FROM ranked WHERE rn=1 ORDER BY {order_by}",
            new_batch_parquet.display(),
            keys = key_list,
            order_by = order_list
        )
    };

    // Write to a temp parquet with max compression.
    // We use ZSTD + small row groups to compress hard (tune ROW_GROUP_SIZE for your workload).
    let tmp = NamedTempFile::new_in(
        existing_path.parent().unwrap_or_else(|| Path::new(".")))?;
    let tmp_path: PathBuf = tmp.path().to_path_buf();
    drop(tmp); // DuckDB will create/overwrite; we just reserved a path on same filesystem.

    let copy_sql = format!(
        "COPY ({select_union})
         TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE  {row_group},
                  PARQUET_VERSION '2.6', STRING_STATS TRUE)",
        tmp_path.display(),
        select_union = select_union,
        // Heavier compression usually likes moderately sized row groups;
        // 256k–1M rows is common for market data; pick what fits your memory profile.
        row_group = 512_000
    );

    conn.execute_batch(&copy_sql)
        .with_context(|| "DuckDB COPY to tmp parquet failed")?;

    // Atomic replace: rename tmp -> final.
    // On Windows, std::fs::rename will replace if same volume; on POSIX it’s atomic.
    fs::rename(&tmp_path, existing_path)
        .or_else(|_| {
            // Fallback: remove and rename (rarely needed)
            if existing_path.exists() { let _ = fs::remove_file(existing_path); }
            fs::rename(&tmp_path, existing_path)
        })
        .with_context(|| format!("Failed to move merged parquet into {}", existing_path.display()))?;

    Ok(())
}