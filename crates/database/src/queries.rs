use std::str::FromStr;
use ahash::AHashMap;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use duckdb::{params, Connection};
use rust_decimal::Decimal;
use standard_lib::market_data::base_data::{Candle, Resolution, Side, Tick};
use standard_lib::securities::symbols::Exchange;
use crate::duck::{latest_available, resolve_dataset_id};
use crate::layout::Layout;
use crate::models::{DataKind, SeqBound};

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

// ---------- helpers ----------

#[inline]
fn epoch_ns_to_dt(ns: i64) -> DateTime<Utc> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nanos)
        .expect("valid timestamp from parquet epoch ns")
}

/// Escape a path for embedding in a DuckDB string literal.
#[inline]
fn duck_string(s: &str) -> String {
    // escape single quotes for SQL literal
    format!("'{}'", s.replace('\'', "''"))
}

/// Build a read_parquet() invocation over an explicit list of files.
fn build_read_parquet_list(paths: &[String]) -> anyhow::Result<String> {
    if paths.is_empty() {
        return Err(anyhow::anyhow!("no partition paths provided"));
    }
    // If duck_string takes &str, use p.as_str()
    let list = paths
        .iter()
        .map(|p| duck_string(p.as_str()))
        .collect::<Vec<String>>()
        .join(", ");

    Ok(format!("read_parquet([{}])", list))
}

/// Find all partition files overlapping a time window.
fn partition_paths_for_range(
    conn: &Connection,
    dataset_id: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Vec<String>> {
    let mut q = conn.prepare(
        r#"
        select path
          from partitions
         where dataset_id = ?
           and max_ts >= ?
           and min_ts <  ?
         order by min_ts asc
        "#,
    )?;
    let mut rows = q.query(params![dataset_id, start.to_rfc3339(), end.to_rfc3339()])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let p: String = r.get(0)?;
        out.push(p);
    }
    Ok(out)
}

// ---------- Ticks ----------

/// Get ticks for [start, end) using DuckDB to scan the *overlapping* parquet files only.
/// Dedupe rule is conservative: first row per (time_ns, exec_id, maker_order_id, taker_order_id).
pub fn get_ticks_in_range(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Vec<Tick>> {
    if start >= end {
        return Ok(Vec::new());
    }

    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, DataKind::Tick, None)? else {
        return Ok(Vec::new());
    };

    let paths = partition_paths_for_range(conn, dataset_id, start, end)?;
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let src = build_read_parquet_list(&paths)?;

    // Push down time filter and project only needed columns.
    // Use epoch_ns() to avoid FromSql timestamp issues, then convert in Rust.
    // QUALIFY requires DuckDB ≥0.8.0 (covered by crate ≥1.0). If needed, emulate using subquery + ROW_NUMBER().
    let sql = format!(r#"
        with src as (
          select
              symbol,
              exchange,
              price,
              size,
              side,
              exec_id,
              maker_order_id,
              taker_order_id,
              venue_seq,
              epoch_ns(time)      as time_ns,
              epoch_ns(ts_event)  as ts_event_ns,
              epoch_ns(ts_recv)   as ts_recv_ns
          from {src}
          where symbol = ?
            and time >= to_timestamp(?)
            and time <  to_timestamp(?)
        ),
        ranked as (
          select *,
                 row_number() over (
                   partition by time_ns,
                                coalesce(exec_id,''),
                                coalesce(maker_order_id,''),
                                coalesce(taker_order_id,'')
                   order by coalesce(venue_seq, 2147483647) asc
                 ) as rn
          from src
        )
        select
            symbol,
            exchange,
            CAST(price AS VARCHAR) as price_s,
            CAST(size  AS VARCHAR) as size_s,
            side, exec_id, maker_order_id, taker_order_id, venue_seq,
            time_ns, ts_event_ns, ts_recv_ns
        from ranked
        where rn = 1
        order by time_ns asc, coalesce(venue_seq, 2147483647) asc
        "#, src=src);

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![symbol, start.to_rfc3339(), end.to_rfc3339()])?;
    let mut out = Vec::new();

    while let Some(r) = rows.next()? {
        let sym: String = r.get(0)?;
        let exch_str: String = r.get(1)?;
        let price_s: String = r.get(2)?;
        let size_s:  String = r.get(3)?;
        let side_str: Option<String> = r.get(4)?;
        let exec_id: Option<String> = r.get(5)?;
        let maker_order_id: Option<String> = r.get(6)?;
        let taker_order_id: Option<String> = r.get(7)?;
        let venue_seq: Option<i32> = r.get(8)?;
        let t_ns: i64 = r.get(9)?;
        let t_event_ns: Option<i64> = r.get(10)?;
        let t_recv_ns:  Option<i64> = r.get(11)?;

        let price = rust_decimal::Decimal::from_str(&price_s)?;
        let size  = rust_decimal::Decimal::from_str(&size_s)?;

        let exchange = Exchange::from_str(&exch_str)
            .ok_or_else(|| anyhow!("unknown exchange '{}' in parquet", exch_str))?;

        let side = match side_str.as_deref() {
            Some("Buy")  | Some("B") => Side::Buy,
            Some("Sell") | Some("S") => Side::Sell,
            _ => Side::None,
        };

        out.push(Tick {
            symbol: sym,
            exchange,
            price,
            size,
            side,
            time: epoch_ns_to_dt(t_ns),
            exec_id,
            maker_order_id,
            taker_order_id,
            venue_seq: venue_seq.map(|v| v as u32),
            ts_event: t_event_ns.map(epoch_ns_to_dt),
            ts_recv:  t_recv_ns.map(epoch_ns_to_dt),
        });
    }

    Ok(out)
}

/// Convenience: from `start` to latest available.
pub fn get_ticks_from_date_to_latest(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    start: DateTime<Utc>,
) -> anyhow::Result<Vec<Tick>> {
    let Some(SeqBound { ts: latest_ts, .. }) =
        latest_available(conn, provider, symbol, DataKind::Tick, None)?
    else {
        return Ok(Vec::new());
    };
    get_ticks_in_range(conn, provider, symbol, start, latest_ts)
}

// ---------- Candles ----------

/// Get candles for [start, end) for a specific resolution.
/// Assumes parquet column names matching your Candle schema.
/// If your parquet has `resolution` as text, we can filter it here too.
pub fn get_candles_in_range(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    resolution: Resolution,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    if start >= end {
        return Ok(Vec::new());
    }

    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, DataKind::Candle, Some(resolution))? else {
        return Ok(Vec::new());
    };

    let paths = partition_paths_for_range(conn, dataset_id, start, end)?;
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let src = build_read_parquet_list(&paths)?;

    // Cast numeric columns to VARCHAR so we can parse into rust_decimal::Decimal losslessly.
    // Use epoch_ns() to avoid FromSql timestamp issues and preserve nanoseconds.
    // If your parquet stores a `resolution` column, add: `and resolution = ?` and pass it as a param.
    let sql = format!(
        r#"
        select
            symbol,
            exchange,
            CAST(open  AS VARCHAR) as open_s,
            CAST(high  AS VARCHAR) as high_s,
            CAST(low   AS VARCHAR) as low_s,
            CAST(close AS VARCHAR) as close_s,
            CAST(volume        AS VARCHAR) as volume_s,
            CAST(ask_volume    AS VARCHAR) as ask_volume_s,
            CAST(bid_volume    AS VARCHAR) as bid_volume_s,
            CAST(num_of_trades AS VARCHAR) as num_trades_s,
            epoch_ns(time_start) as ts_start_ns,
            epoch_ns(time_end)   as ts_end_ns
        from {src}
        where symbol = ?
          and time_start >= to_timestamp(?)
          and time_end   <= to_timestamp(?)
        order by ts_start_ns asc
        "#,
        src = src
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![symbol, start.to_rfc3339(), end.to_rfc3339()])?;
    let mut out = Vec::new();

    while let Some(r) = rows.next()? {
        let sym: String = r.get(0)?;
        let exch_str: String = r.get(1)?;

        let open_s:  String = r.get(2)?;
        let high_s:  String = r.get(3)?;
        let low_s:   String = r.get(4)?;
        let close_s: String = r.get(5)?;
        let volume_s:     String = r.get(6)?;
        let ask_volume_s: String = r.get(7)?;
        let bid_volume_s: String = r.get(8)?;
        let num_trades_s: String = r.get(9)?;

        let ts_start_ns: i64 = r.get(10)?;
        let ts_end_ns:   i64 = r.get(11)?;

        let open  = Decimal::from_str(&open_s)?;
        let high  = Decimal::from_str(&high_s)?;
        let low   = Decimal::from_str(&low_s)?;
        let close = Decimal::from_str(&close_s)?;
        let volume      = Decimal::from_str(&volume_s)?;
        let ask_volume  = Decimal::from_str(&ask_volume_s)?;
        let bid_volume  = Decimal::from_str(&bid_volume_s)?;
        let num_trades  = Decimal::from_str(&num_trades_s)?;

        let exchange = Exchange::from_str(&exch_str)
            .ok_or_else(|| anyhow!("unknown exchange '{}' in parquet", exch_str))?;

        out.push(Candle {
            symbol: sym,
            exchange,
            time_start: epoch_ns_to_dt(ts_start_ns),
            time_end:   epoch_ns_to_dt(ts_end_ns),
            open, high, low, close,
            volume, ask_volume, bid_volume,
            num_of_trades: num_trades,
            resolution,
        });
    }

    Ok(out)
}

/// Convenience: candles from `start_date` to latest available.
pub fn get_candles_from_date_to_latest(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    resolution: Resolution,
    start: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    let Some(SeqBound { ts: latest_ts, .. }) =
        latest_available(conn, provider, symbol, DataKind::Candle, Some(resolution))?
    else {
        return Ok(Vec::new());
    };
    get_candles_in_range(conn, provider, symbol, resolution, start, latest_ts)
}

