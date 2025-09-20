-- Providers and symbols are de-duplicated across the catalog.
create table if not exists providers (
                                         provider_id   integer primary key autoincrement,
                                         provider_code text unique not null
);

create table if not exists symbols (
                                       symbol_id   integer primary key autoincrement,
                                       provider_id integer not null references providers(provider_id) on delete cascade,
    -- canonical symbol within the provider (e.g., "MNQ", "MNQZ25", etc.)
    symbol_text text not null,
    unique(provider_id, symbol_text)
    );

-- Dataset kind + resolution per (provider, symbol)
-- kind: 'tick' | 'quote' | 'candle' | 'orderbook'
-- resolution: null for tick/quote/orderbook; e.g. 'S:1','M:5','H:1','TICKBARS:500','D','W' for candles
create table if not exists datasets (
                                        dataset_id  integer primary key autoincrement,
                                        provider_id integer not null references providers(provider_id) on delete cascade,
    symbol_id   integer not null references symbols(symbol_id)   on delete cascade,
    kind        text not null,
    resolution  text,
    unique(provider_id, symbol_id, kind, coalesce(resolution,''))
    );

-- One row per on-disk file/partition (Parquet or DuckDB table fragment).
-- We track min/max timestamps and optional sequence bounds for stable ordering.
create table if not exists partitions (
                                          partition_id   integer primary key autoincrement,
                                          dataset_id     integer not null references datasets(dataset_id) on delete cascade,
    path           text not null,               -- relative/absolute file path or duckdb table name
    storage        text not null,               -- 'parquet' | 'duckdb'
    rows           bigint not null,
    bytes          bigint not null,
    min_ts         timestamp not null,
    max_ts         timestamp not null,
    min_seq        bigint,                      -- optional (ticks/quotes/orderbook)
    max_seq        bigint,                      -- optional
    day_key        date,                        -- optional convenience for daily grouping
    created_at     timestamp default current_timestamp,
    unique(dataset_id, path)
    );

-- Helpful covering indexes for time-bounded lookups
create index if not exists idx_partitions_dataset_time on partitions(dataset_id, min_ts, max_ts);
create index if not exists idx_symbols_provider_symbol on symbols(provider_id, symbol_text);
create index if not exists idx_datasets_key on datasets(provider_id, symbol_id, kind, coalesce(resolution,''));
create index if not exists idx_partitions_dataset_day on partitions(dataset_id, day_key);