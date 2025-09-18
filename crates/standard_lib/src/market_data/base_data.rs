use rust_decimal::Decimal;
use strum_macros::Display;
use crate::securities::symbols::Exchange;

/// Resolution for time- or tick-based aggregation.
///
/// Used mainly for candles and bar consolidators.
///
/// - [`Tick`] – number of ticks per bar or 1 tick.
/// - [`Quote`] – Quote updates (BBO snapshots).
/// - [`Seconds(u8)`] – N-second bars (e.g. 1-second, 5-second).
/// - [`Minutes(u8)`] – N-minute bars.
/// - [`Hours(u8)`] – N-hour bars.
/// - [`TickBars(u64)`] – Bars built from a fixed number of ticks.
/// - [`Daily`] – One bar per trading day.
/// - [`Weekly`] – One bar per trading week.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Display, Copy)]
pub enum Resolution {
    Ticks,
    Quote,
    Seconds(u8),
    Minutes(u8),
    Hours(u8),
    TickBars(u32),
    Daily,
    Weekly,
}

/// Trade direction.
///
/// Indicates whether a trade or order was executed on the buy or sell side.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Display, Copy)]
pub enum Side {
    /// Buyer-initiated trade.
    Buy,
    /// Seller-initiated trade.
    Sell,
    /// Unknown direction.
    None
}

/// How the bar closed
#[derive(Clone, Debug, PartialEq, Eq, Hash, Display)]
pub enum BarClose {
    /// Close > Open
    Bullish,
    /// Close < Open
    Bearish,
    /// Close == Open
    Flat
}

/// A single executed trade (tick).
///
/// Represents the smallest atomic piece of trade data.
/// Often used as input for tick charts or indicators.
#[derive(Clone, Debug)]
pub struct Tick {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Exchange code (e.g. `Exchange::CME`, `Exchange::NASDAQ`).
    pub exchange: Exchange,
    /// Trade price.
    pub price: rust_decimal::Decimal,
    /// Trade size (quantity).
    pub size:  rust_decimal::Decimal,
    /// UTC timestamp of the trade.
    pub time:  chrono::DateTime<chrono::Utc>,
    /// Whether the trade was buyer- or seller-initiated.
    pub side: Side,

    // ---- Optional execution metadata (populated on MBO or capable feeds) ----
    /// Venue-provided execution/match identifier for this print.
    pub exec_id: Option<String>,
    /// Resting (maker) order ID involved in the execution, if known.
    pub maker_order_id: Option<String>,
    /// Aggressing (taker) order ID involved in the execution, if known.
    pub taker_order_id: Option<String>,
    /// Venue/publisher sequence number for ordering+gap checks.
    pub venue_seq: Option<u32>,

    /// Raw venue timing (optional; useful for latency & causality analysis).
    pub ts_event: Option<chrono::DateTime<chrono::Utc>>, // matching-engine time
    pub ts_recv:  Option<chrono::DateTime<chrono::Utc>>, // capture/server time
}

/// A candlestick / bar of aggregated trades.
///
/// Produced by consolidating ticks or vendor-provided candles.
#[derive(Clone, Debug)]
pub struct Candle {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Exchange code (e.g. `Exchange::CME`, `Exchange::NASDAQ`).
    pub exchange: Exchange,
    /// Start time of the candle (inclusive).
    pub time_start: chrono::DateTime<chrono::Utc>,
    /// End time of the candle (exclusive).
    pub time_end:   chrono::DateTime<chrono::Utc>,
    /// Open price.
    pub open: rust_decimal::Decimal,
    /// High price.
    pub high: rust_decimal::Decimal,
    /// Low price.
    pub low: rust_decimal::Decimal,
    /// Close price.
    pub close: rust_decimal::Decimal,
    /// Total traded volume.
    pub volume: rust_decimal::Decimal,
    /// Volume executed at the ask.
    pub ask_volume: rust_decimal::Decimal,
    /// Volume executed at the bid.
    pub bid_volume: rust_decimal::Decimal,
    /// Number of trades contributing to this bar.
    pub num_of_trades: rust_decimal::Decimal,
    /// Resolution used to build this candle.
    pub resolution: Resolution,
}

impl Candle {

    /// How the bar closed, bullish, bearish or flat.
    pub fn bar_close(&self) -> BarClose {
        if self.close > self.open {
            BarClose::Bullish
        } else if self.close < self.open {
            BarClose::Bearish
        } else {
            BarClose::Flat
        }
    }

    /// The range of the bar
    pub fn range(&self) -> Decimal {
        self.high - self.low
    }
}


#[derive(Clone, Debug)]
/// Best bid and offer (BBO).
///
/// A lightweight snapshot of the top of the order book.
pub struct Bbo {
    // ---- Core (vendor-agnostic) ----
    pub symbol: String,
    pub exchange: Exchange,
    pub bid: rust_decimal::Decimal,
    pub bid_size: rust_decimal::Decimal,
    pub ask: rust_decimal::Decimal,
    pub ask_size: rust_decimal::Decimal,
    pub time: chrono::DateTime<chrono::Utc>, // normalized event time

    // ---- Optional cross-vendor metadata ----
    /// Order counts at top level (Rithmic: *_orders, Databento: bid_ct_00/ask_ct_00).
    pub bid_orders: Option<u32>,
    pub ask_orders: Option<u32>,

    /// Venue/publisher sequence number (Databento: sequence, other feeds often have one).
    pub venue_seq: Option<u32>,

    /// Matching-engine and capture timestamps when available.
    pub ts_event: Option<chrono::DateTime<chrono::Utc>>,
    pub ts_recv:  Option<chrono::DateTime<chrono::Utc>>,

    /// Whether this record is a snapshot/seed vs. incremental (many feeds set this).
    pub is_snapshot: Option<bool>,

    /// Feed-specific flags if provided (e.g., Databento flags byte).
    pub flags: Option<u8>,
}
/// Full order book snapshot.
///
/// Contains bid and ask ladders up to the requested depth.
/// Depth levels are sorted: index 0 = best price level.
#[derive(Clone, Debug)]
pub struct OrderBook {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Exchange code (e.g. `Exchange::CME`, `Exchange::NASDAQ`).   
    pub exchange: Exchange,
    /// Bid levels: `(price, size)`, best first.
    pub bids: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
    /// Ask levels: `(price, size)`, best first.
    pub asks: Vec<(rust_decimal::Decimal, rust_decimal::Decimal)>,
    /// UTC timestamp of the snapshot.
    pub time: chrono::DateTime<chrono::Utc>,
}


