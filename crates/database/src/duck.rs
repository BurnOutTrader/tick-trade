use duckdb::{Connection, params};
use std::path::Path;
use thiserror::Error;
use standard_lib::market_data::base_data::Resolution;

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