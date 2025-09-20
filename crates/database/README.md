# Tick-Trade Data Engine

A hybrid storage engine for market data built in Rust.  
It supports **DuckDB + Parquet** for efficient historical storage, compression, and cross-language interoperability.

---

## 1) Storage Model

We use a hybrid model combining **Parquet** and **DuckDB**:

- **Parquet** provides compact, columnar storage with excellent compression and interoperability (Spark, Polars, Pandas, Arrow).
- **DuckDB** acts as a query engine and metadata catalog, enabling efficient filtering, aggregations, and universe selection.

### Partitioning Strategy
- **Tick data**: one Parquet file per **day**.
- **Daily bars**: one Parquet file per **year**.
- **Weekly bars**: one Parquet file per **market/universe**.
- Files are grouped by:
    - Provider
    - Symbol (or continuous vs contract code)
    - Resolution

This ensures both **fast queries** and **minimal duplication**.

---

## 2) Catalog and Metadata

DuckDB maintains partition metadata:

- `datasets` — one row per provider/symbol/resolution.
- `partitions` — one row per file (path, size, row count, min/max timestamps, etc.).

This allows fast queries like:

```sql
-- Earliest available data for symbol
SELECT MIN(min_ts) FROM partitions WHERE dataset_id = ?;

-- Latest available data for symbol
SELECT MAX(max_ts) FROM partitions WHERE dataset_id = ?;
```

---

## 3) Using the API

Other parts of the application can serialize or access data using the provided Rust functions. These functions leverage DuckDB SQL queries over Parquet files and return strongly-typed domain models such as `Tick` and `Candle`.

Examples:

```rust
// Retrieve ticks in a given time range
let ticks: Vec<Tick> = get_ticks_in_range("AAPL", start_timestamp, end_timestamp)?;
```

```rust
// Retrieve candles (bars) in a given time range
let candles: Vec<Candle> = get_candles_in_range("AAPL", start_timestamp, end_timestamp)?;
```

```rust
// Query earliest and latest available data timestamps for a symbol
let earliest: Option<DateTime<Utc>> = earliest_available("AAPL")?;
let latest: Option<DateTime<Utc>> = latest_available("AAPL")?;
```

### Order Book (L2) storage & retrieval

We persist order-book snapshots in daily parquet partitions, one **new** part-file per ingest (never overwrite). Columns:

- `symbol TEXT`
- `exchange TEXT`
- `time TIMESTAMP` (UTC; written as RFC3339 → DuckDB stores ns precision)
- `bids_json TEXT` – JSON `[[price, size], ...]` with `Decimal` preserved as strings
- `asks_json TEXT` – same

> We purposely keep ladders as JSON in parquet for robustness and max compression via ZSTD. DuckDB still pushes down `WHERE symbol AND time` on scan; we parse JSON at the edges.

**Write:**
```rust
let day = Utc::now().date_naive(); // partition key
let out_file = write_orderbooks_partition(
    &conn,
    "databento",
    "MNQ",
    Exchange::CME,
    day,
    &snapshots,          // Vec<OrderBook>
    std::path::Path::new("./data"),
)?;
println!("book parquet written: {}", out_file.display());

let start = "2025-01-24T14:30:00Z".parse()?;
let end   = "2025-01-24T14:31:00Z".parse()?;
let books = get_books_in_range(&conn, "databento", "MNQ", start, end, Some(10))?;
for ob in books {
println!("{} {} {} bids:{} asks:{}",
         ob.time, ob.exchange, ob.symbol, ob.bids.len(), ob.asks.len());
}
```

## Notes / rationale

- **No overwrite on partial-day updates:** each ingest writes a **new** parquet “part” and `upsert_partition` registers it. Readers use `read_parquet([list_of_parts])`, so you never lose the first 50% of a day.
- **Max compression:** ZSTD + JSON ladders compress extremely well (repeated structure, small strings). If you later want fully typed Arrow `LIST<STRUCT<price,size>>`, we can add a parallel writer; the read APIs won’t change.
- **Cross-tooling:** Because it’s plain Parquet + TEXT columns, Polars/Pandas/Spark can also scan & filter by `symbol`/`time`. They can parse `bids_json/asks_json` if they need ladders.
- **Precision:** We keep `time` as ns internally (DuckDB stores TIMESTAMP_NS), and we serialize decimals as strings inside JSON (no float round-off).


