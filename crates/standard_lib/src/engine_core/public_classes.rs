use std::sync::Arc;
use tokio::sync::broadcast;
use crate::market_data::base_data::{Bbo, Candle, OrderBook, Resolution, Tick};
use crate::securities::security::Security;
use crate::securities::symbols::Exchange;

/// Request for market data.
///
/// Used when calling [`acquire`](super::Bus::acquire) to subscribe to live or
/// historical feeds. A request uniquely identifies a subscription by
/// `(vendor, symbol, exchange, kinds, resolution, depth)`.
///
/// Fields:
/// - `symbol` – Market symbol (e.g. `"MNQ"`, `"AAPL"`, `"BTC-USDT"`).
/// - `exchange` – Exchange code (e.g. `"CME"`, `"NASDAQ"`, `"BINANCE"`).
/// - `kinds` – Which feeds to request (ticks, candles, book, etc.).
/// - `book_depth` – Optional max book depth (e.g. `Some(10)` for top 10 levels).
/// - `resolution` – Optional bar size for candle feeds (e.g. `1s`, `1m`, `1h`).
/// - `vendor` – Vendor selection policy.
#[derive(Clone, Debug)]
pub struct MarketDataRequest {
    pub(crate) vendor: String,
    pub symbol: String,
    pub exchange: Exchange,
    pub kinds: FeedKind,
    pub book_depth: Option<u8>,
    pub resolution: Option<Resolution>,
}

/// Strategy execution mode.
///
/// Determines whether the engine should operate against a live vendor or
/// replay historical data into the event hub.
///
/// - [`Live`] – Connect to real-time vendor feeds and trade live.
/// - [`Historical`] – Replay stored data for backtesting / warmup.
#[derive(Clone, PartialEq, Eq, Debug, Hash, Copy)]
pub enum StrategyMode {
    /// Live trading or paper trading against a connected vendor.
    Live,

    /// Historical mode for backtesting or simulation.
    Backtest,
}


bitflags::bitflags! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
    pub struct FeedKind: u32 {
    const MBO     = 1<<0;
    const BOOK    = 1<<1;
    const BBO     = 1<<2;
    const TICKS   = 1<<3;
    const CANDLES = 1<<4;
    }
}


pub enum StrategyEvent {
    SymbolAdded {
        id: String,
        exchange: Exchange,
        sec: Arc<Security>,
        feeds: Feeds
    },
    SymbolRemoved {
        id: String,
        exchange: Exchange,
    },
    // (optional) other control signals, e.g., heartbeat/time, config updates, etc.
}

pub struct Feeds {
    pub vcode: String,
    pub tick_rx:   Option<broadcast::Receiver<Tick>>,
    pub bbo_rx:    Option<broadcast::Receiver<Bbo>>,
    pub book_rx:   Option<broadcast::Receiver<OrderBook>>,
    pub candle_rx: Option<broadcast::Receiver<Candle>>,
}