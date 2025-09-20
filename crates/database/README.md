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



## Replay
```rust
use chrono::{Utc, TimeZone};
use database::queries::{
    get_ticks_in_range, get_candles_in_range, get_books_in_range, /* etc */
};
use replay::historical_feed::{HistoricalFeed, HistoricalReceivers};

struct MySinks;
impl HistoricalReceivers for MySinks {
    fn on_ticks(&mut self, ts: chrono::DateTime<Utc>, batch: &[Tick]) {
        // fan-out to your tick receivers
        // e.g., event_hub.publish_ticks(ts, batch)
    }
    fn on_candles(&mut self, ts: chrono::DateTime<Utc>, batch: &[Candle]) {
        // fan-out to bar receivers
    }
    fn on_bbo(&mut self, ts: chrono::DateTime<Utc>, batch: &[Bbo]) {
        // ...
    }
    fn on_books(&mut self, ts: chrono::DateTime<Utc>, batch: &[OrderBook]) {
        // ...
    }
}

fn run_replay(conn: &duckdb::Connection) -> anyhow::Result<()> {
    let provider = "databento";
    let symbol   = "MNQ";
    let res      = standard_lib::market_data::base_data::Resolution::Seconds(1);

    let start = Utc.with_ymd_and_hms(2024, 9, 12, 13, 30, 0).unwrap();
    let end   = Utc.with_ymd_and_hms(2024, 9, 12, 20, 0, 0).unwrap();

    // Pull historical data (already time-sorted by our queries).
    let ticks   = get_ticks_in_range(conn, provider, symbol, start, end)?;
    let candles = get_candles_in_range(conn, provider, symbol, res, start, end)?;
    let books   = get_books_in_range(conn, provider, symbol, start, end, Some(10))?;
    let bbo     = Vec::new(); // if you fetch BBO, plug it in

    // Build the stepper (skip sorting because queries already ORDER BY time).
    let mut feed = HistoricalFeed::new(ticks, candles, bbo, books, /*ensure_sort=*/ false);

    let mut sinks = MySinks;
    while feed.step(&mut sinks) {
        // do pacing here if you want (e.g., sleep for realtime replay speed)
    }
    Ok(())
}
```



## Replay (Backtest)
How you use it
- You do not “install a DB server”. DuckDB/Parquet are file-backed; your read fns already operate with a duckdb::Connection.
- For each (provider, symbol, kind) you want to replay, prefetch with your query helpers, build a SymbolKindReplayer, register with the ReplayCoordinator, and run. The coordinator guarantees global time order and same-timestamp coalescing across all sources.

Example wiring (in your historical engine):
```rust
use chrono::{Utc, TimeZone, DateTime};
use std::time::Duration;
use duckdb::Connection;

pub async fn run_historical_example(conn: &Connection, hub: EventHub) -> anyhow::Result<()> {
    let provider = "Rithmic";
    let symbol   = "MNQ";
    let exch     = Exchange::CME;

    let start: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 12, 2, 0, 0, 0).unwrap();
    let end:   DateTime<Utc> = Utc.with_ymd_and_hms(2024, 12, 2, 23, 59, 59).unwrap();

    // Prefetch your datasets (these preserve ns and are time-sorted in the replayer)
    let ticks   = crate::queries::get_ticks_in_range(conn, provider, symbol, start, end)?;
    let candles = crate::queries::get_candles_in_range(conn, provider, symbol, Resolution::Seconds(1), start, end)?;
    let books   = crate::queries::get_books_in_range(conn, provider, symbol, start, end, Some(10))?;

    // Build sources
    let tick_src   = SymbolKindReplayer::new_ticks(format!("{provider} {symbol} Ticks"), ticks);
    let cndl_src   = SymbolKindReplayer::new_candles(format!("{provider} {symbol} Candles(1s)"), candles);
    let book_src   = SymbolKindReplayer::new_books(format!("{provider} {symbol} Books"), books);

    // Coordinate and run (optional pacing to avoid firehose; omit for max speed)
    let mut coord = ReplayCoordinator::new();
    coord.add_source(tick_src);
    coord.add_source(cndl_src);
    coord.add_source(book_src);

    coord.run(hub.clone(), Some(Duration::from_millis(0))).await?;
    Ok(())
}
```

Why this solves your concerns
- Different channels per provider/symbol/type: each SymbolKindReplayer is exactly one such channel. You can add as many as you want.
- Synchronize historical: the ReplayCoordinator uses a global min-heap on peek_ts() to emit all sources that have the next timestamp, together, every step.
- Same-timestamp fan-out: inside each replayer we drain “all rows with the same ts” into a batch and publish once, so consolidators/broadcasters see a coherent snapshot for that instant.
- Candles keyed on time_end: implemented in peek_ts() and the drain for candles.
- No clones in hot path: we operate on VecDeque and move items out.
- Works for live mode too: the same pattern applies; swap “prefetch” with streaming inputs that append to the deques.


