use std::collections::btree_set::Range;
use standard_lib::market_data::base_data::{Bbo, Tick};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use anyhow::anyhow;
use chrono::{DateTime, Duration, TimeZone, Utc};
use prost::Message;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use uuid::Uuid;
use standard_lib::engine_core::api_traits::{HistoricalDataProvider, HistoricalRequest, HistoryEvent, HistoryHandle, RequestType};
use standard_lib::engine_core::data_events::FeedKind;
use standard_lib::engine_core::provider_resolver::{ProviderResolver, SecurityResolver};
use standard_lib::market_data::base_data::{Candle, Resolution};
use standard_lib::securities::market_hours::{candle_end, hours_for_exchange, next_session_open_after};
use standard_lib::securities::symbols::Exchange;
use crate::client::api_base::RithmicApiClient;
use crate::client::rithmic_proto_objects::rti::{OrderBook, ResponseTimeBarReplay};
use crate::client::rithmic_proto_objects::rti::request_login::SysInfraType;
#[derive(Clone, Debug)]
pub enum HistFeed {
    Ticks,
    Bbo,
    Book(Resolution),                   // future
    Candles(Resolution),    // Seconds/Minutes/Hours/Daily/Weekly
}

#[derive(Clone, Debug)]
pub struct HistKey {
    pub vendor:   String,     // "RITHMIC"
    pub exchange: Exchange,
    pub symbol:   String,     // vendor code, e.g. "MNQZ25"
    pub feed:     HistFeed,
}

#[derive(Debug)]
pub enum HistRecord {
    Tick(Tick),
    Bbo(Bbo),            // your type
    Candle(Candle),
    OrderBook(OrderBook)
}

#[async_trait::async_trait]
pub trait HistoricalStore: Send + Sync {
    /// Append a batch (already time-sorted & stable seq) to the correct partition.
    async fn append(&self, key: &HistKey, batch: Vec<HistRecord>) -> anyhow::Result<()>;

    /// Stream a time range as decoded engine types. Implementations should:
    /// - use .idx to seek frames
    /// - decode only frames overlapping [range.start, range.end)
    /// - return strictly time-ordered, then seq-ordered events
    async fn stream(
        &self,
        key: &HistKey,
        range: Range<DateTime<Utc>>,
    ) -> anyhow::Result<mpsc::Receiver<HistRecord>>;
}

// ---- cancellable handle ----
struct RithHistHandle {
    id: Uuid,
    cancelled: AtomicBool,
}
impl RithHistHandle {
    fn new(id: Uuid) -> Self {
        Self {
            id,
            cancelled: AtomicBool::new(false)
        }
    }
    #[inline] fn is_cancelled(&self) -> bool { self.cancelled.load(Ordering::Relaxed) }
}
impl HistoryHandle for RithHistHandle {
    fn cancel(&self) { self.cancelled.store(true, Ordering::Relaxed); }
    fn id(&self) -> Uuid { self.id }
}

#[async_trait::async_trait]
impl HistoricalDataProvider for RithmicApiClient {
    fn name(&self) -> &str { &self.name }

    async fn ensure_connected(self: Arc<Self>) -> anyhow::Result<()> {
        {
            let ready = self.live_ready.read().await;
            if *ready { return Ok(()); }
        }
        // History plant is where bulk downloads come from. Ticker plant is
        // kept up so the same client can also do live if needed.
        self.connect_and_login(SysInfraType::HistoryPlant).await?;
        self.connect_and_login(SysInfraType::TickerPlant).await?;
        Ok(())
    }

    async fn fetch(
        self: Arc<Self>,
        req: HistoricalRequest,
    ) -> anyhow::Result<(Arc<dyn HistoryHandle>, Receiver<HistoryEvent>)> {
        // ----- validate -----
        let earliest = DateTime::parse_from_rfc3339("2019-06-03T00:00:00Z")
            .unwrap().with_timezone(&Utc);

        if req.end <= req.start {
            return Err(anyhow!("invalid time range: end <= start"));
        }
        if req.start < earliest {
            return Err(anyhow!("start {} precedes earliest {}", req.start, earliest));
        }
        match req.r_type {
            RequestType::Tick => {
                return Err(anyhow!("Rithmic history: Tick replay not supported with rithmic"));
            }
            RequestType::Candle => {
                if req.resolution.is_none() {
                    return Err(anyhow!("candle request requires `resolution`"));
                }
            }
            _ => return Err(anyhow!("Rithmic history: only Tick/Candle supported")),
        }

        // ----- user stream & handle -----
        let (tx_user, rx_user) = mpsc::channel::<HistoryEvent>(2048);
        let handle = Arc::new(RithHistHandle::new(Uuid::new_v4()));

        // ----- paging task -----
        let me   = self.clone();
        let h    = handle.clone();
        let req0 = req.clone();
        let mh   = hours_for_exchange(req.exchange);

        tokio::spawn(async move {
            const EPS_NS: i64 = 1; // avoid duplicates at page boundary
            let res = req0.resolution.clone().unwrap();
            let mut cursor = req0.start;

            'outer: while cursor < req0.end {
                if h.is_cancelled() {
                    let _ = tx_user.send(HistoryEvent::EndOfStream).await;
                    break 'outer;
                }

                // Request one page (returns an in-memory ChunkVec — no recv loop)
                let chunks = match me
                    .replay_bars(
                        req0.symbol.clone(),
                        req0.exchange,
                        res.clone(),
                        cursor,
                        req0.end,
                    )
                    .await
                {
                    Ok(c)  => c,
                    Err(e) => {
                        let _ = tx_user.send(HistoryEvent::Error(anyhow!(e))).await;
                        break 'outer;
                    }
                };

                let mut last_end: Option<DateTime<Utc>> = None;
                let mut got_any = false;

                // Drain all chunks synchronously
                for chunk in chunks.into_iter() {
                    if h.is_cancelled() { break 'outer; }
                    if let Ok(msg) = ResponseTimeBarReplay::decode(&chunk[..]) {
                        if let Some(x) = map_time_bar_replay(&msg, req0.symbol.clone(), req0.exchange, res) {
                            got_any = true;
                            last_end = Some(x.time_end);
                            if tx_user.send(HistoryEvent::Candle(x)).await.is_err() {
                                break 'outer; // downstream closed
                            }
                        }
                    }
                }

                // Advance cursor
                if let Some(e) = last_end {
                    cursor = (e + Duration::nanoseconds(EPS_NS)).min(req0.end);
                } else {
                    // No bars in this window: jump to next session open (regular+extended)
                    let probe = cursor + Duration::seconds(1);
                    let next_open = next_session_open_after(&mh, probe);
                    cursor = next_open.min(req0.end);
                }

                // Safety: if we still cannot move forward, stop to avoid a tight loop
                if !got_any && cursor >= req0.end {
                    break 'outer;
                }
            }

            let _ = tx_user.send(HistoryEvent::EndOfStream).await;
        });

        Ok((handle as Arc<dyn HistoryHandle>, rx_user))
    }

    // Optional hints; keep returning `true` unless you want routing logic,
    // e.g., deny Tick when provider only supports bars.
    fn supports(&self, kinds: FeedKind) -> bool {
        if kinds == FeedKind::CANDLES | FeedKind::TICKS {
            return true
        }
        false
    }
    fn supports_resolution(&self, res: &Option<Resolution>) -> bool {
        match res {
            None => false,
            Some(r) => {
                match r {
                    Resolution::Ticks => true,
                    Resolution::Quote => false,
                    Resolution::Seconds(_) => true,
                    Resolution::Minutes(_) => true,
                    Resolution::Hours(_) => true,
                    Resolution::TickBars(_) => true,
                    Resolution::Daily => true,
                    Resolution::Weekly => true,
                }
            }
        }
    }
    
    fn resolver(self: Arc<Self>) -> Arc<dyn SecurityResolver> {
        Arc::new(ProviderResolver::new(self.name().to_string(), self.clone()))
    }
}

/// Map a single ResponseTimeBarReplay (Rithmic time-bar row) into a normalized Candle.
/// Returns None if any required field is missing or malformed.
#[inline]
pub fn map_time_bar_replay(msg: &ResponseTimeBarReplay, symbol: String, exchange: Exchange, resolution: Resolution) -> Option<Candle> {
    // ----- required: marker -> time_start -----
    let time_start: DateTime<Utc> = {
        let sec = msg.marker?;
        Utc.timestamp_opt(sec as i64, 0).single()?
    };

    // ----- required: OHLC -----
    let open  = Decimal::from_f64(msg.open_price?)?;
    let high  = Decimal::from_f64(msg.high_price?)?;
    let low   = Decimal::from_f64(msg.low_price?)?;
    let close = Decimal::from_f64(msg.close_price?)?;

    // ----- optional: volumes/trades -----
    let volume        = msg.volume.and_then(Decimal::from_u64).unwrap_or_default();
    let bid_volume    = msg.bid_volume.and_then(Decimal::from_u64).unwrap_or_default();
    let ask_volume    = msg.ask_volume.and_then(Decimal::from_u64).unwrap_or_default();
    let num_of_trades = msg.num_trades.and_then(Decimal::from_u64).unwrap_or_default();

    // ----- compute inclusive bar end using your exchange-aware helper -----
    let time_end = candle_end(time_start, resolution, exchange)?;

    Some(Candle {
        symbol,
        exchange,
        time_start,
        time_end,
        open,
        high,
        low,
        close,
        volume,
        ask_volume,
        bid_volume,
        num_of_trades,
        resolution,
    })
}
