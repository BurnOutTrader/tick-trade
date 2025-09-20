use chrono::{Datelike, NaiveDate};
use std::path::{Path, PathBuf};

/// Logical dataset split knobs
#[derive(Debug, Clone, Copy)]
pub enum Partitioning {
    /// One file per day (ticks, quotes, 1s bars, etc.)
    Daily,
    /// One file per year (e.g., daily bars)
    Yearly,
    /// One file total (e.g., weekly bars)
    Single,
}

#[derive(Debug, Clone)]
pub struct Layout {
    pub root: PathBuf, // e.g. /data
}

impl Layout {
    pub fn new<P: Into<PathBuf>>(root: P) -> Self { Self { root: root.into() } }

    /// File path: {root}/{provider}/{symbol}/{data_kind}/{resolution}/YYYY/…/file.parquet
    /// We separate by provider (not market type) to keep datasets consistent.
    pub fn file_for(
        &self,
        provider: &str,
        symbol: &str,         // “MNQ”, “MNQZ25”, etc. (continuous vs contract up to you)
        data_kind: &str,      // "ticks" | "bbo" | "candles" | "orders" ...
        resolution: &str,     // "ticks" | "1s" | "1m" | "daily" | "weekly" ...
        when: NaiveDate,
        part: Partitioning,
    ) -> PathBuf {
        match part {
            Partitioning::Daily => {
                self.root
                    .join(provider)
                    .join(symbol)
                    .join(data_kind)
                    .join(resolution)
                    .join(format!("{:04}", when.year()))
                    .join(format!("{:02}", when.month()))
                    .join(format!("{:02}", when.day()))
                    .with_extension("parquet")
            }
            Partitioning::Yearly => {
                self.root
                    .join(provider)
                    .join(symbol)
                    .join(data_kind)
                    .join(resolution)
                    .join(format!("{:04}", when.year()))
                    .with_extension("parquet")
            }
            Partitioning::Single => {
                self.root
                    .join(provider)
                    .join(symbol)
                    .join(data_kind)
                    .join(resolution)
                    .with_extension("parquet")
            }
        }
    }

    /// Convenience to ensure parent dirs exist.
    pub fn ensure_parent_dirs(path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}