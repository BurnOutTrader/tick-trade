use chrono::{DateTime, Datelike, Utc};
use standard_lib::market_data::base_data::Resolution;

/// Filesystem layout with resolution-aware partitioning.
/// Examples:
///   ticks (daily files):    {root}/parquet/ticks/{provider}/{symbol}/yyyy/mm/dd.snappy.parquet
///   candles daily (yearly): {root}/parquet/candles/{provider}/{symbol}/D/{yyyy}.zstd.parquet
///   candles weekly (one):   {root}/parquet/candles/{provider}/{symbol}/W/all.zstd.parquet
pub struct LakeLayout {
    pub root: std::path::PathBuf,
}
impl LakeLayout {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self { Self { root: root.into() } }

    pub fn tick_path(&self, provider: &str, symbol_id: &str, ts: DateTime<Utc>) -> std::path::PathBuf {
        self.root
            .join("parquet").join("ticks").join(provider).join(symbol_id)
            .join(format!("{:04}", ts.year()))
            .join(format!("{:02}", ts.month()))
            .join(format!("{:02}.zstd.parquet", ts.day()))
    }

    pub fn candle_path(&self, provider: &str, symbol_id: &str, res: &Resolution, ts: DateTime<Utc>) -> std::path::PathBuf {
        match res {
            Resolution::Daily => self.root
                .join("parquet").join("candles").join(provider).join(symbol_id)
                .join("D").join(format!("{:04}.zstd.parquet", ts.year())),
            Resolution::Weekly => self.root
                .join("parquet").join("candles").join(provider).join(symbol_id)
                .join("W").join("all.zstd.parquet"),
            Resolution::Seconds(_)|Resolution::Minutes(_)|Resolution::Hours(_)|Resolution::TickBars(_) => {
                // Keep them “daily” like ticks for write locality and compression
                self.root
                    .join("parquet").join("candles").join(provider).join(symbol_id)
                    .join(Self::res_dir(res))
                    .join(format!("{:04}", ts.year()))
                    .join(format!("{:02}", ts.month()))
                    .join(format!("{:02}.zstd.parquet", ts.day()))
            }
            Resolution::Ticks | Resolution::Quote => unreachable!("use tick_path or bbo_path"),
        }
    }

    pub fn bbo_path(&self, provider: &str, symbol_id: &str, ts: DateTime<Utc>) -> std::path::PathBuf {
        self.root
            .join("parquet").join("bbo").join(provider).join(symbol_id)
            .join(format!("{:04}", ts.year()))
            .join(format!("{:02}", ts.month()))
            .join(format!("{:02}.zstd.parquet", ts.day()))
    }

    pub fn res_dir(res: &Resolution) -> String {
        match res {
            Resolution::Seconds(n) => format!("S{}", n),
            Resolution::Minutes(n) => format!("M{}", n),
            Resolution::Hours(n)   => format!("H{}", n),
            Resolution::TickBars(n)=> format!("TBAR_{}", n),
            Resolution::Daily      => "D".into(),
            Resolution::Weekly     => "W".into(),
            Resolution::Ticks      => "TICK".into(),
            Resolution::Quote      => "QUOTE".into(),
        }
    }
}