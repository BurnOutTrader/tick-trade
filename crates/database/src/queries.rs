use ahash::AHashMap;
use chrono::{DateTime, Utc};
use duckdb::{params, Connection};
use standard_lib::market_data::base_data::Resolution;
use crate::layout::Layout;
use crate::models::DataKind;

/// Return earliest timestamp available for a single (provider, kind, symbol, exchange, res).
/// Uses partition pruning + Parquet stats; reads no payloads when min/max suffice.
pub fn earliest_event_ts(
    conn: &Connection,
    layout: &Layout,
    provider: &str,
    kind: DataKind,
    symbol: &str,
    exchange: &str,
    res: Resolution,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let glob = layout.glob_for(provider, kind, symbol, &exchange, res);

    // NOTE:
    // - HIVE_PARTITIONING=1 makes DuckDB expose partition keys as columns.
    // - We still filter on those columns for super-cheap file pruning.
    // - The time column inside parquet must be a TIMESTAMP (UTC). If it’s int,
    //   cast it in the query or write it as timestamp when producing parquet.
    let sql = r#"
        SELECT min(time) AS min_ts
        FROM read_parquet(?, HIVE_PARTITIONING=1, UNION_BY_NAME=1)
        WHERE provider = ? AND kind = ? AND symbol = ? AND exchange = ? AND res = ?
    "#;

    let res_s = format!("{res:?}"); // “Ticks”, “Minutes5”, etc., as in layout
    let kind_s = format!("{kind:?}");

    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(params![glob, provider, kind_s, symbol, exchange, res_s])?;

    if let Some(row) = rows.next()? {
        let min_ts: Option<String> = row.get(0)?;
        // DuckDB returns RFC3339 string for TIMESTAMP by default in the driver; parse back:
        let out = match min_ts {
            Some(s) => Some(DateTime::parse_from_rfc3339(&s)?.with_timezone(&Utc)),
            None => None,
        };
        Ok(out)
    } else {
        Ok(None)
    }
}

/// Batch version: earliest timestamp per symbol (same provider/kind/exchange/res).
pub fn earliest_per_symbol(
    conn: &Connection,
    layout: &Layout,
    provider: &str,
    kind: DataKind,
    symbols: &[String],
    exchange: &str,
    res: Resolution,
) -> anyhow::Result<AHashMap<String, Option<DateTime<Utc>>>> {
    let mut out = AHashMap::with_capacity(symbols.len());
    for s in symbols {
        let v = earliest_event_ts(conn, layout, provider, kind, s, exchange, res)?;
        out.insert(s.clone(), v);
    }
    Ok(out)
}

/// Earliest across many symbols (useful for “universe” queries).
pub fn earliest_any(
    conn: &Connection,
    layout: &Layout,
    provider: &str,
    kind: DataKind,
    symbols: &[String],
    exchange: &str,
    res: Resolution,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    // union all with parameterized table functions is awkward; simplest path:
    let mut best: Option<DateTime<Utc>> = None;
    for s in symbols {
        if let Some(ts) = earliest_event_ts(conn, layout, provider, kind, s, exchange, res)? {
            best = match best {
                None => Some(ts),
                Some(b) if ts < b => Some(ts),
                Some(b) => Some(b),
            };
        }
    }
    Ok(best)
}