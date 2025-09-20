use duckdb::{Connection, params, OptionalExt, Error};
use std::path::Path;
use thiserror::Error;
use standard_lib::market_data::base_data::Resolution;
use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use crate::models::{DataKind, SeqBound};

/// Create or reuse a DuckDB connection (file-backed or in-memory).
pub fn connect(db_file: Option<&Path>) -> anyhow::Result<Connection> {
    let conn = match db_file {
        Some(p) => Connection::open(p)?,
        None => Connection::open_in_memory()?,
    };
    // Sensible defaults for Parquet discovery & pushdown:
    // (DuckDB enables most pushdowns by default; hive partitioning is per-scan arg below.)
    Ok(conn)
}

#[derive(Debug, Error)]
pub enum DuckError {
    #[error("duckdb: {0}")] Duck(#[from] duckdb::Error),
}

pub struct Duck {
    pub(crate) conn: Connection,
}
impl Duck {
    /// `db_path` is a small persistent catalog DB (tables: providers, symbols, universes).
    /// Parquet lives under your FS lake; DuckDB queries it in-place with pushdown.
    pub fn open(db_path: &Path) -> Result<Self, DuckError> {
        let conn = Connection::open(db_path)?;
        let this = Self { conn };
        this.init()?;
        Ok(this)
    }

    fn init(&self) -> Result<(), DuckError> {
        self.conn.execute_batch(r#"
            PRAGMA threads=MAX;
            PRAGMA enable_progress_bar=false;

            CREATE TABLE IF NOT EXISTS providers(
              provider TEXT PRIMARY KEY,
              version  TEXT
            );

            CREATE TABLE IF NOT EXISTS symbols(
              symbol_id TEXT PRIMARY KEY,
              security  TEXT NOT NULL,
              exchange  TEXT NOT NULL,
              currency  TEXT NOT NULL,
              root      TEXT,
              continuous_of TEXT
            );

            CREATE TABLE IF NOT EXISTS universes(
              universe TEXT NOT NULL,
              symbol_id TEXT NOT NULL,
              provider TEXT NOT NULL,
              PRIMARY KEY (universe, symbol_id, provider)
            );
        "#)?;
        Ok(())
    }

    pub fn upsert_provider(&self, provider: &str, version: Option<&str>) -> Result<(), DuckError> {
        self.conn.execute(
            "INSERT INTO providers(provider, version) VALUES(?, ?)
             ON CONFLICT(provider) DO UPDATE SET version=excluded.version",
            params![provider, version],
        )?;
        Ok(())
    }

    pub fn upsert_symbol(&self,
                         symbol_id: &str, security: &str, exchange: &str, currency: &str, root: Option<&str>, continuous_of: Option<&str>
    ) -> Result<(), DuckError> {
        self.conn.execute(
            "INSERT INTO symbols(symbol_id, security, exchange, currency, root, continuous_of)
             VALUES(?,?,?,?,?,?)
             ON CONFLICT(symbol_id) DO UPDATE SET security=excluded.security, exchange=excluded.exchange,
                  currency=excluded.currency, root=excluded.root, continuous_of=excluded.continuous_of",
            params![symbol_id, security, exchange, currency, root, continuous_of],
        )?;
        Ok(())
    }

    pub fn add_universe_member(&self, universe: &str, symbol_id: &str, provider: &str) -> Result<(), DuckError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO universes(universe, symbol_id, provider) VALUES(?,?,?)",
            params![universe, symbol_id, provider],
        )?;
        Ok(())
    }

    /// Register a view that scans parquet files with pushdown filters.
    /// Example: create_ticks_view("ticks_all", "/data/parquet/ticks")
    pub fn create_ticks_view(&self, view_name: &str, ticks_root: &Path) -> Result<(), DuckError> {
        let root = ticks_root.to_string_lossy();
        // Partition-aware wildcard; columns must match parquet schema
        self.conn.execute_batch(&format!(r#"
            CREATE OR REPLACE VIEW {view} AS
            SELECT * FROM parquet_scan('{root}/**/*.parquet',
              hive_partitioning = 0
            );
            -- For common filters ensure sorted by key_ts_utc_us,key_tie if needed in result
        "#, view=view_name, root=root))?;
        Ok(())
    }

    pub fn create_candles_view(&self, view_name: &str, candles_root: &Path, res: &Resolution) -> Result<(), DuckError> {
        let root = candles_root.to_string_lossy();
        let rdir = crate::layout::LakeLayout::res_dir(res);
        self.conn.execute_batch(&format!(r#"
            CREATE OR REPLACE VIEW {view} AS
            SELECT * FROM parquet_scan('{root}/**/{rdir}/**/*.parquet', hive_partitioning = 0);
        "#, view=view_name, root=root, rdir=rdir))?;
        Ok(())
    }
}

/// Resolve or create provider_id
pub fn upsert_provider(conn: &Connection, provider_code: &str) -> Result<i64> {
    conn.execute(
        "insert or ignore into providers(provider_code) values (?)", params![provider_code]
    )?;
    let mut stmt = conn.prepare("select provider_id from providers where provider_code = ?")?;
    let id: i64 = stmt.query_row(params![provider_code], |r| r.get(0))?;
    Ok(id)
}

/// Resolve or create symbol_id for a provider
pub fn upsert_symbol(conn: &Connection, provider_id: i64, symbol: &str) -> Result<i64> {
    conn.execute(
        "insert or ignore into symbols(provider_id, symbol_text) values (?, ?)",
        params![provider_id, symbol]
    )?;
    let mut stmt = conn.prepare(
        "select symbol_id from symbols where provider_id = ? and symbol_text = ?"
    )?;
    let id: i64 = stmt.query_row(params![provider_id, symbol], |r| r.get(0))?;
    Ok(id)
}

/// Resolve or create dataset_id for (provider, symbol, kind, resolution_key)
pub fn upsert_dataset(
    conn: &Connection,
    provider_id: i64,
    symbol_id: i64,
    kind: DataKind,
    resolution: Option<Resolution>,
) -> Result<i64> {
    let kind_key = match kind {
        DataKind::Tick => "tick",
        DataKind::Bbo => "quote",
        DataKind::Candle => "candle",
        DataKind::BookL2 => "orderbook",
    };
    let res_key = resolution.and_then(|r| r.as_key());

    conn.execute(
        "insert or ignore into datasets(provider_id, symbol_id, kind, resolution)
         values (?, ?, ?, ?)",
        params![provider_id, symbol_id, kind_key, res_key]
    )?;

    let mut q = conn.prepare(
        "select dataset_id from datasets
         where provider_id = ? and symbol_id = ? and kind = ? and coalesce(resolution,'') = coalesce(?, '')"
    )?;
    let id: i64 = q.query_row(params![provider_id, symbol_id, kind_key, res_key], |r| r.get(0))?;
    Ok(id)
}

#[inline]
fn dt_to_millis(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

pub fn create_partitions_schema(conn: &Connection) -> duckdb::Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS partitions (
            -- what dataset this partition belongs to (per provider/symbol/resolution)
            dataset_id   INTEGER NOT NULL,
            -- path to the physical object (parquet file, duckdb table chunk, etc.)
            path         TEXT    NOT NULL,
            -- 'parquet' | 'duckdb'
            storage      TEXT    NOT NULL,

            -- basic stats
            rows         INTEGER NOT NULL,
            bytes        INTEGER NOT NULL,

            -- time range covered by this partition (milliseconds since unix epoch, UTC)
            min_ts_ms    INTEGER NOT NULL,
            max_ts_ms    INTEGER NOT NULL,

            -- optional monotonic sequence range, when feeds provide it
            min_seq      INTEGER,
            max_seq      INTEGER,

            -- optional day partition key; store as YYYY-MM-DD for readability
            day_key      TEXT,

            PRIMARY KEY (dataset_id, path)
        );

        -- helpful indexes for pruning
        CREATE INDEX IF NOT EXISTS idx_partitions_dataset_time
            ON partitions(dataset_id, min_ts_ms, max_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_partitions_day
            ON partitions(dataset_id, day_key);
        "#
    )
}

pub fn upsert_partition(
    conn: &Connection,
    dataset_id: i64,
    path: &str,
    storage: &str,      // 'parquet' | 'duckdb'
    rows: i64,
    bytes: i64,
    min_ts: DateTime<Utc>,
    max_ts: DateTime<Utc>,
    min_seq: Option<i64>,
    max_seq: Option<i64>,
    day_key: Option<NaiveDate>,
) -> anyhow::Result<()> {
    // Convert timestamps to compact INTEGER (ms) for SQLite.
    let min_ts_ms = dt_to_millis(min_ts);
    let max_ts_ms = dt_to_millis(max_ts);
    let day_key_str = day_key.map(|d| d.to_string()); // "YYYY-MM-DD"

    // Important: on conflict, we only *widen* ranges and update stats;
    // we never lose existing coverage.
    conn.execute(
        r#"
        INSERT INTO partitions
            (dataset_id, path, storage, rows, bytes, min_ts_ms, max_ts_ms, min_seq, max_seq, day_key)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dataset_id, path) DO UPDATE SET
            rows      = excluded.rows,
            bytes     = excluded.bytes,
            min_ts_ms = MIN(partitions.min_ts_ms, excluded.min_ts_ms),
            max_ts_ms = MAX(partitions.max_ts_ms, excluded.max_ts_ms),
            -- if either side is NULL, coalesce to the non-NULL; otherwise keep narrower/larger as appropriate
            min_seq   = CASE
                            WHEN partitions.min_seq IS NULL THEN excluded.min_seq
                            WHEN excluded.min_seq IS NULL   THEN partitions.min_seq
                            ELSE MIN(partitions.min_seq, excluded.min_seq)
                        END,
            max_seq   = CASE
                            WHEN partitions.max_seq IS NULL THEN excluded.max_seq
                            WHEN excluded.max_seq IS NULL   THEN partitions.max_seq
                            ELSE MAX(partitions.max_seq, excluded.max_seq)
                        END,
            day_key   = COALESCE(partitions.day_key, excluded.day_key)
        "#,
        params![
            dataset_id,
            path,
            storage,
            rows,
            bytes,
            min_ts_ms,
            max_ts_ms,
            min_seq,
            max_seq,
            day_key_str
        ],
    )?;
    Ok(())
}

/// Internal helper to resolve dataset_id from string keys.
fn resolve_dataset_id(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    kind: DataKind,
    resolution: Option<Resolution>,
) -> Result<Option<i64>> {
    let kind_key = match kind {
        DataKind::Tick => "tick",
        DataKind::Bbo => "quote",
        DataKind::Candle => "candle",
        DataKind::BookL2 => "orderbook",
    };
    let res_key = resolution.and_then(|r| r.as_key());

    let mut stmt = conn.prepare(
        "select d.dataset_id
           from datasets d
           join providers p on p.provider_id = d.provider_id
           join symbols   s on s.symbol_id   = d.symbol_id
          where p.provider_code = ?
            and s.symbol_text   = ?
            and d.kind          = ?
            and coalesce(d.resolution,'') = coalesce(?, '')"
    )?;
    let mut rows = stmt.query(params![provider, symbol, kind_key, res_key])?;
    if let Some(row) = rows.next()? {
        Ok(Some(row.get::<_, i64>(0)?))
    } else {
        Ok(None)
    }
}

#[inline]
fn ms_to_dt(ms: i64) -> std::result::Result<DateTime<Utc>, Error> {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| duckdb::Error::FromSqlConversionFailure(8, duckdb::types::Type::BigInt, Box::new(std::io::Error::new(std::io::ErrorKind::Other, "invalid millis"))))
}

pub fn earliest_available(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    kind: DataKind,
    resolution: Option<Resolution>,
) -> Result<Option<SeqBound>> {
    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, kind, resolution)? else {
        return Ok(None);
    };

    // earliest = smallest (min_ts_ms, min_seq)
    let mut stmt = conn.prepare(
        r#"
        SELECT min_ts_ms, min_seq
          FROM partitions
         WHERE dataset_id = ?
         ORDER BY min_ts_ms ASC, COALESCE(min_seq, 0) ASC
         LIMIT 1
        "#
    )?;

    let row: Option<(i64, Option<i64>)> =
        stmt.query_row(params![dataset_id], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;

    Ok(row.map(|(ts_ms, seq)| SeqBound { ts: ms_to_dt(ts_ms).unwrap(), seq }))
}

pub fn latest_available(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    kind: DataKind,
    resolution: Option<Resolution>,
) -> Result<Option<SeqBound>> {
    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, kind, resolution)? else {
        return Ok(None);
    };

    // latest = largest (max_ts_ms, max_seq)
    let mut stmt = conn.prepare(
        r#"
        SELECT max_ts_ms, max_seq
          FROM partitions
         WHERE dataset_id = ?
         ORDER BY max_ts_ms DESC, COALESCE(max_seq, 0) DESC
         LIMIT 1
        "#
    )?;

    let row: Option<(i64, Option<i64>)> =
        stmt.query_row(params![dataset_id], |r| Ok((r.get(0)?, r.get(1)?))).optional()?;

    Ok(row.map(|(ts_ms, seq)| SeqBound { ts: ms_to_dt(ts_ms).unwrap(), seq }))
}

/// Bulk variant: fetch earliest for many symbols at once.
pub fn earliest_for_many(
    conn: &Connection,
    provider: &str,
    symbols: &[&str],
    kind: DataKind,
    resolution: Option<Resolution>,
) -> Result<Vec<(String, Option<SeqBound>)>> {
    let mut out = Vec::with_capacity(symbols.len());
    for sym in symbols {
        let v = earliest_available(conn, provider, sym, kind, resolution)?;
        out.push(((*sym).to_string(), v));
    }
    Ok(out)
}

/// Bulk variant: latest for many symbols.
pub fn latest_for_many(
    conn: &Connection,
    provider: &str,
    symbols: &[&str],
    kind: DataKind,
    resolution: Option<Resolution>,
) -> Result<Vec<(String, Option<SeqBound>)>> {
    let mut out = Vec::with_capacity(symbols.len());
    for sym in symbols {
        let v = latest_available(conn, provider, sym, kind, resolution)?;
        out.push(((*sym).to_string(), v));
    }
    Ok(out)
}