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

```rust
// Candles (e.g., 1s or 5m)
let written = ingest_candles(&conn, "Rithmic", "MNQ", "CME", Resolution::Seconds(1), &candle_rows, data_root, 9)?;

// BBO
let written = ingest_bbo(&conn, "Rithmic", "MNQ", "CME", &bbo_rows, data_root, 9)?;

// Order books
let written = ingest_books(&conn, "Rithmic", "MNQ", &book_snaps, data_root, 9)?;
```

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


## Replay (Backtest)

## 1. Loading data for the sim
```rust
use chrono::{Utc, TimeZone};
use duckdb::Connection;
use database::queries::{
    get_ticks_in_range, get_candles_in_range, get_books_in_range,
};
use standard_lib::market_data::base_data::{Resolution};

let conn = Connection::open("catalog.duckdb")?;
let provider = "Rithmic";
let symbol   = "MNQ";
let start    = Utc.with_ymd_and_hms(2024, 10, 23, 13, 30, 0).unwrap();
let end      = Utc.with_ymd_and_hms(2024, 10, 23, 20, 0, 0).unwrap();

// load only what you need
let ticks   = get_ticks_in_range(&conn, provider, symbol, start, end)?;
let candles = get_candles_in_range(&conn, provider, symbol, Resolution::Seconds(1), start, end)?;
let books   = get_books_in_range(&conn, provider, symbol, start, end, Some(10))?; // depth=10
```

## 2. Build replayers and the coordinator
```rust
use database::replay::{ReplayCoordinator, SymbolKindReplayer};
use std::time::Duration;

// Create one replayer per (symbol, kind)
let r_ticks   = SymbolKindReplayer::new_ticks(format!("{provider} {symbol} Ticks"),   ticks);
let r_candles = SymbolKindReplayer::new_candles(format!("{provider} {symbol} 1s"),   candles);
let r_books   = SymbolKindReplayer::new_books(format!("{provider} {symbol} Books"),  books);

// Build the coordinator
let mut coord = ReplayCoordinator::new();
coord.add_source(r_ticks);
coord.add_source(r_candles);
coord.add_source(r_books);

// You’ll also need your EventHub:
let hub = EventHub::new();
```

## 3. Pacing modes

### Fixed Rate Pacing 
```rust
// 10ms per replay step (all items at the same timestamp are batched per step)
let pace = Some(Duration::from_millis(10));
tokio::spawn(async move {
    let _ = coord.run(hub.clone(), pace).await;
});
```

### Real time pacing
```rust
use chrono::{DateTime, Utc};
use std::time::Duration;
use tokio::time::sleep;
use database::replay::{ReplayCoordinator, ReplayControl};

let hub = hub.clone();
let (mut runner, handle) = ReplayCoordinator::with_control(coord); // gives you a control handle

let speed = 4.0; // 4x realtime
tokio::spawn(async move {
    let mut last_ts: Option<DateTime<Utc>> = None;
    loop {
        // Ask the runner to step once (returns the timestamp it just emitted, if any)
        match runner.step_once(&hub).await {
            Some(ts) => {
                if let Some(prev) = last_ts {
                    let nanos = (ts - prev).num_nanoseconds().unwrap_or(0);
                    if nanos > 0 {
                        // Scale the gap by 1/speed (min clamp avoids zero-sleep busy loops)
                        let scaled = (nanos as f64 / speed).max(0.0);
                        let dur = Duration::from_nanos(scaled as u64);
                        sleep(dur).await;
                    }
                }
                last_ts = Some(ts);
            }
            None => break, // done
        }

        // Optional: poll control channel to pause/resume/change speed (see §4)
        if handle.is_paused() { handle.wait_until_resumed().await; }
    }
});
```

### Manual Steping
```rust
let (mut runner, handle) = ReplayCoordinator::with_control(coord);

// step on demand
while runner.step_once(&hub).await.is_some() {
    // …do other work, inspect hub snapshots, etc…
    // call again when you’re ready
}
```

Tips & gotchas
- Candles: keyed by time_end. If you previously treated them by time_start, fix usages (we already did in queries + replayer).
- Backpressure: broadcast channels are lossy under lag. For historical sims at high speed, bump topic buffer sizes (Topic::new(cap)), or slow the pace.
- Memory: If rows are huge, consider streaming in file chunks (e.g., per day) and rotate replayers as you progress. The coordinator design stays the same; sources can be short-lived.
- Determinism: Within a single timestamp, order is “source order”. If you need a strict rule (e.g., book before bbo, bbo before tick), do it in the receivers or split steps per kind.

#### Minimal End to End Example
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1) Hub
    let hub = EventHub::new();

    // 2) Load data & build coordinator (see §1–2)
    let conn = duckdb::Connection::open("catalog.duckdb")?;
    let (start, end) = ( /* … */ );
    let ticks   = get_ticks_in_range(&conn, "Rithmic", "MNQ", start, end)?;
    let candles = get_candles_in_range(&conn, "Rithmic", "MNQ", Resolution::Seconds(1), start, end)?;
    let books   = get_books_in_range(&conn, "Rithmic", "MNQ", start, end, Some(10))?;

    let mut coord = ReplayCoordinator::new();
    coord.add_source(SymbolKindReplayer::new_ticks("MNQ ticks".into(), ticks));
    coord.add_source(SymbolKindReplayer::new_candles("MNQ 1s".into(), candles));
    coord.add_source(SymbolKindReplayer::new_books("MNQ books".into(), books));

    // 3) Control
    let (mut runner, ctl) = ReplayCoordinator::with_control(coord);
    ctl.set_speed(8.0);

    // 4) Consumers
    let mut rx = hub.subscribe_candle_symbol("MNQ");
    tokio::spawn(async move {
        while let Ok(c) = rx.recv().await {
            println!("{} {}-{} C={}", c.symbol, c.time_start, c.time_end, c.close);
        }
    });

    // 5) Run (timestamp-paced)
    tokio::spawn({
        let hub = hub.clone();
        async move {
            let mut last = None;
            while let Some(ts) = runner.step_once(&hub).await {
                if ctl.is_paused() { ctl.wait_until_resumed().await; }
                if let Some(prev) = last {
                    let nanos = (ts - prev).num_nanoseconds().unwrap_or(0);
                    let scaled = (nanos as f64 / ctl.speed()).max(0.0);
                    tokio::time::sleep(std::time::Duration::from_nanos(scaled as u64)).await;
                }
                last = Some(ts);
            }
        }
    });

    // (Optional) UI: pause after 3 seconds, resume after 2, change speed
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    ctl.pause();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    ctl.set_speed(20.0);
    ctl.resume();

    // keep alive for demo
    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
    Ok(())
}
```