use std::fs;
use std::sync::Arc;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::arrow::{ArrowWriter};
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use thiserror::Error;
use crate::models::{TickRow, CandleRow, BboRow};

#[derive(Debug, Error)]
pub enum ParquetError {
    #[error("io: {0}")] Io(#[from] std::io::Error),
    #[error("parquet: {0}")] Pq(#[from] parquet::errors::ParquetError),
    #[error("arrow: {0}")] Arrow(#[from] arrow::error::ArrowError),
}

fn zstd_props(level: i32) -> WriterProperties {
    // Aggressive compression, better density. Smaller data pages help RLE/dict.
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(level).unwrap_or(ZstdLevel::default())))
        .set_dictionary_enabled(true)
        .set_data_page_size_limit(128 * 1024) // 128KB pages
        .set_write_batch_size(32 * 1024)     // batches -> better page utilization
        .build()
}

// ---------- Schemas ----------
fn tick_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("symbol_id", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Float64, false),
        Field::new("side", DataType::UInt8, false),
        Field::new("key_ts_utc_us", DataType::Int64, false),
        Field::new("key_tie", DataType::UInt32, false),
        Field::new("venue_seq", DataType::UInt32, true),
        Field::new("exec_id", DataType::Utf8, true),
    ]))
}
fn candle_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("symbol_id", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("res", DataType::Utf8, false),
        Field::new("time_start_us", DataType::Int64, false),
        Field::new("time_end_us", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("ask_volume", DataType::Float64, false),
        Field::new("bid_volume", DataType::Float64, false),
        Field::new("num_trades", DataType::UInt64, false),
    ]))
}
fn bbo_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("provider", DataType::Utf8, false),
        Field::new("symbol_id", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("key_ts_utc_us", DataType::Int64, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("bid_size", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("ask_size", DataType::Float64, false),
        Field::new("bid_orders", DataType::UInt32, true),
        Field::new("ask_orders", DataType::UInt32, true),
        Field::new("venue_seq", DataType::UInt32, true),
        Field::new("is_snapshot", DataType::Boolean, true),
    ]))
}

// ---------- Builders ----------
fn to_batch_ticks(rows: &[TickRow]) -> Result<RecordBatch, ParquetError> {
    let schema = tick_schema(); // make sure schema has an Int64 "key_ts_utc_ns"
    let mut provider = StringBuilder::new();
    let mut symbol_id = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut price = Float64Builder::new();
    let mut size = Float64Builder::new();
    let mut side = UInt8Builder::new();
    let mut ts_ns = Int64Builder::new();        // <-- ns
    let mut tie = UInt32Builder::new();
    let mut venue_seq = UInt32Builder::new();
    let mut exec_id = StringBuilder::new();

    for r in rows {
        provider.append_value(&r.provider);
        symbol_id.append_value(&r.symbol_id);
        exchange.append_value(&r.exchange);
        price.append_value(r.price);
        size.append_value(r.size);
        side.append_value(r.side);
        ts_ns.append_value(r.key_ts_utc_ns);    // <-- ns
        tie.append_value(r.key_tie);
        match r.venue_seq { Some(v)=>venue_seq.append_value(v), None=>venue_seq.append_null() }
        match &r.exec_id { Some(v)=>exec_id.append_value(v), None=>exec_id.append_null() }
    }

    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(provider.finish()), Arc::new(symbol_id.finish()), Arc::new(exchange.finish()),
        Arc::new(price.finish()), Arc::new(size.finish()), Arc::new(side.finish()),
        Arc::new(ts_ns.finish()), Arc::new(tie.finish()),
        Arc::new(venue_seq.finish()), Arc::new(exec_id.finish()),
    ])?)
}

fn to_batch_candles(rows: &[CandleRow]) -> Result<RecordBatch, ParquetError> {
    let schema = candle_schema(); // include Int64 "time_start_ns","time_end_ns"
    let mut provider = StringBuilder::new();
    let mut symbol_id = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut res = StringBuilder::new();
    let mut ts0_ns = Int64Builder::new();
    let mut ts1_ns = Int64Builder::new();
    let (mut o, mut h, mut l, mut c) = (Float64Builder::new(), Float64Builder::new(), Float64Builder::new(), Float64Builder::new());
    let (mut v, mut av, mut bv) = (Float64Builder::new(), Float64Builder::new(), Float64Builder::new());
    let mut n = UInt64Builder::new();

    for r in rows {
        provider.append_value(&r.provider); symbol_id.append_value(&r.symbol_id);
        exchange.append_value(&r.exchange); res.append_value(&r.res);
        ts0_ns.append_value(r.time_start_ns);   // <-- ns
        ts1_ns.append_value(r.time_end_ns);     // <-- ns
        o.append_value(r.open); h.append_value(r.high); l.append_value(r.low); c.append_value(r.close);
        v.append_value(r.volume); av.append_value(r.ask_volume); bv.append_value(r.bid_volume);
        n.append_value(r.num_trades);
    }

    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(provider.finish()), Arc::new(symbol_id.finish()), Arc::new(exchange.finish()),
        Arc::new(res.finish()), Arc::new(ts0_ns.finish()), Arc::new(ts1_ns.finish()),
        Arc::new(o.finish()), Arc::new(h.finish()), Arc::new(l.finish()), Arc::new(c.finish()),
        Arc::new(v.finish()), Arc::new(av.finish()), Arc::new(bv.finish()),
        Arc::new(n.finish()),
    ])?)
}

fn to_batch_bbo(rows: &[BboRow]) -> Result<RecordBatch, ParquetError> {
    let schema = bbo_schema(); // include Int64 "key_ts_utc_ns"
    let mut provider = StringBuilder::new();
    let mut symbol_id = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut ts_ns = Int64Builder::new();
    let (mut bid, mut bid_sz, mut ask, mut ask_sz) =
        (Float64Builder::new(), Float64Builder::new(), Float64Builder::new(), Float64Builder::new());
    let mut bid_orders = UInt32Builder::new();
    let mut ask_orders = UInt32Builder::new();
    let mut is_snapshot = BooleanBuilder::new();
    let mut venue_seq = UInt32Builder::new();

    for r in rows {
        provider.append_value(&r.provider);
        symbol_id.append_value(&r.symbol_id);
        exchange.append_value(&r.exchange);
        ts_ns.append_value(r.key_ts_utc_ns);     // <-- ns
        bid.append_value(r.bid); bid_sz.append_value(r.bid_size);
        ask.append_value(r.ask); ask_sz.append_value(r.ask_size);

        match r.bid_orders { Some(v)=>bid_orders.append_value(v), None=>bid_orders.append_null() }
        match r.ask_orders { Some(v)=>ask_orders.append_value(v), None=>ask_orders.append_null() }
        match r.is_snapshot{ Some(v)=>is_snapshot.append_value(v), None=>is_snapshot.append_null() }
    }

    Ok(RecordBatch::try_new(schema, vec![
        Arc::new(provider.finish()), Arc::new(symbol_id.finish()), Arc::new(exchange.finish()),
        Arc::new(ts_ns.finish()),
        Arc::new(bid.finish()), Arc::new(bid_sz.finish()),
        Arc::new(ask.finish()), Arc::new(ask_sz.finish()),
        Arc::new(bid_orders.finish()), Arc::new(ask_orders.finish()),
        Arc::new(venue_seq.finish()), Arc::new(is_snapshot.finish()),
    ])?)
}

// ---------- Public write helpers ----------
pub fn write_ticks_zstd(path: &std::path::Path, rows: &[TickRow], zstd_level: i32) -> Result<(), ParquetError> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, tick_schema(), Some(zstd_props(zstd_level)))?;
    let batch = to_batch_ticks(rows)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn write_candles_zstd(path: &std::path::Path, rows: &[CandleRow], zstd_level: i32) -> Result<(), ParquetError> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, candle_schema(), Some(zstd_props(zstd_level)))?;
    let batch = to_batch_candles(rows)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

pub fn write_bbo_zstd(path: &std::path::Path, rows: &[BboRow], zstd_level: i32) -> Result<(), ParquetError> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, bbo_schema(), Some(zstd_props(zstd_level)))?;
    let batch = to_batch_bbo(rows)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}