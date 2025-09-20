# Tick-Trade Data Engine

A hybrid storage engine for market data built in Rust.  
It supports **DuckDB + Parquet** for efficient historical storage, compression, and cross-language interoperability.

---

## 3) Storage Model

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

## 4) Catalog and Metadata

DuckDB maintains partition metadata:

- `datasets` — one row per provider/symbol/resolution.
- `partitions` — one row per file (path, size, row count, min/max timestamps, etc.).

This allows fast queries like:

```sql
-- Earliest available data for symbol
SELECT MIN(min_ts) FROM partitions WHERE dataset_id = ?;

-- Latest available data for symbol
SELECT MAX(max_ts) FROM partitions WHERE dataset_id = ?;