use std::sync::Arc;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
#[allow(unused_imports)]
use crate::websocket::rithmic_proto_objects::rti::{AccountListUpdates, AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates, DepthByOrder, DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode, OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, RequestAccountList, RequestAccountRmsInfo, RequestHeartbeat, RequestLoginInfo, RequestMarketDataUpdate, RequestPnLPositionSnapshot, RequestPnLPositionUpdates, RequestProductCodes, RequestProductRmsInfo, RequestReferenceData, RequestTickBarUpdate, RequestTimeBarUpdate, RequestVolumeProfileMinuteBars, ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo, ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData, ResponseBracketOrder, ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot, ResponseDepthByOrderUpdates, ResponseEasyToBorrowList, ResponseExitPosition, ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying, ResponseGetInstrumentByUnderlyingKeys, ResponseGetVolumeAtPrice, ResponseGiveTickSizeTypeTable, ResponseHeartbeat, ResponseLinkOrders, ResponseListAcceptedAgreements, ResponseListExchangePermissions, ResponseListUnacceptedAgreements, ResponseLogin, ResponseLoginInfo, ResponseLogout, ResponseMarketDataUpdate, ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder, ResponseModifyOrderReferenceData, ResponseNewOrder, ResponseOcoOrder, ResponseOrderSessionConfig, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates, ResponseProductCodes, ResponseProductRmsInfo, ResponseReferenceData, ResponseReplayExecutions, ResponseResumeBars, ResponseRithmicSystemInfo, ResponseSearchSymbols, ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement, ResponseShowBracketStops, ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates, ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders, ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay, ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes, ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel, ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar, TradeRoute, TradeStatistics, UpdateEasyToBorrowList};
use prost::Message as ProstMessage;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;
use tracing::{error, info, warn};
use standard_lib::market_data::base_data::{Side, Bbo, Tick};
use standard_lib::engine_core::event_hub::EventHub;
use standard_lib::securities::symbols::Exchange;

#[allow(unused, dead_code)]
#[inline]
pub async fn match_ticker_plant_id(
    template_id: i32,
    message_buf: Bytes,
    hub: &Arc<EventHub>,
) {
    const PLANT: SysInfraType = SysInfraType::TickerPlant;
    match template_id {
        19 => {}
        101 => {
            if let Ok(msg) = ResponseMarketDataUpdate::decode(&message_buf[..]) {
                // Depth By Order Updates Response
                // From Server
                info!("Market Data Update Response (Template ID: 101) from Server: {:?}", msg);
            }
        }
        118 => {
            if let Ok(msg) = ResponseDepthByOrderUpdates::decode(&message_buf[..]) {
                // Depth By Order Updates Response
                // From Server
                info!("Depth By Order Updates Response (Template ID: 118) from Server: {:?}", msg);
            }
        },
        116 => {
            if let Ok(msg) = ResponseDepthByOrderSnapshot::decode(&message_buf[..]) {
                // Depth By Order Updates Response
                // From Server
                info!("Depth By Order Updates Response (Template ID: 118) from Server: {:?}", msg);
            }
        },
        150 => {
            if let Ok(msg) = LastTrade::decode(&message_buf[..]) {
                //info!("Last Trade (Template ID: 150) from Server: {:?}", msg);
                if let Some(x) = map_last_trade(msg) {
                    hub.publish_tick(x); }
            }
        },
        151 => {
            if let Ok(msg) = BestBidOffer::decode(&message_buf[..]) {
                // Best Bid Offer
                // From Server
                //info!("Best Bid Offer (Template ID: 151) from Server: {:?}", msg);
                if let Some(x) = map_rithmic_bbo(msg) {
                    hub.publish_bbo(x); }
            }
        },
        152 => {
            if let Ok(msg) = TradeStatistics::decode(&message_buf[..]) {
                // Trade Statistics
                // From Server
                info!("Trade Statistics (Template ID: 152) from Server: {:?}", msg);
            }
        },
        153 => {
            if let Ok(msg) = QuoteStatistics::decode(&message_buf[..]) {
                // Quote Statistics
                // From Server
                info!("Quote Statistics (Template ID: 153) from Server: {:?}", msg);
            }
        },
        154 => {
            if let Ok(msg) = IndicatorPrices::decode(&message_buf[..]) {
                // Indicator Prices
                // From Server
                info!("Indicator Prices (Template ID: 154) from Server: {:?}", msg);
            }
        },
        155 => {
            if let Ok(msg) = EndOfDayPrices::decode(&message_buf[..]) {
                // End Of Day Prices
                // From Server
                info!("End Of Day Prices (Template ID: 155) from Server: {:?}", msg);
            }
        },
        156 => {
            if let Ok(msg) = OrderBook::decode(&message_buf[..]) {
                // Order Book
                // From Server
                //info!("Order Book (Template ID: 156) from Server: {:?}", msg);
                if let Some(x) = map_order_book(&msg) {
                    hub.publish_book(x); }
            }
        },
        157 => {
            if let Ok(msg) = MarketMode::decode(&message_buf[..]) {
                // Market Mode
                // From Server
                info!("Market Mode (Template ID: 157) from Server: {:?}", msg);
            }
        },
        158 => {
            if let Ok(msg) = OpenInterest::decode(&message_buf[..]) {
                // Open Interest
                // From Server
                info!("Open Interest (Template ID: 158) from Server: {:?}", msg);
            }
        },
        159 => {
            if let Ok(msg) = FrontMonthContractUpdate::decode(&message_buf[..]) {
                // Front Month Contract Update
                // From Server
                info!("Front Month Contract Update (Template ID: 159) from Server: {:?}", msg);
            }
        },
        160 => {
            if let Ok(msg) = DepthByOrder::decode(&message_buf[..]) {
                // Depth By Order
                // From Server
                info!("Depth By Order (Template ID: 160) from Server: {:?}", msg);
            }
        },
        161 => {
            if let Ok(msg) = DepthByOrderEndEvent::decode(&message_buf[..]) {
                // Depth By Order End Event
                // From Server
                info!("DepthByOrderEndEvent (Template ID: 161) from Server: {:?}", msg);
            }
        },
        162 => {
            if let Ok(msg) = SymbolMarginRate::decode(&message_buf[..]) {
                // Symbol Margin Rate
                // From Server
                info!("Symbol Margin Rate (Template ID: 162) from Server: {:?}", msg);
            }
        },
        163 => {
            if let Ok(msg) = OrderPriceLimits::decode(&message_buf[..]) {
                // Order Price Limits
                // From Server
                info!("Order Price Limits (Template ID: 163) from Server: {:?}", msg);
            }
        },
        _ => warn!("Rithmic Tick Plant: No match for template_id: {}", template_id)
    }
}

#[inline]
pub(crate) fn map_last_trade(m: LastTrade) -> Option<Tick> {
    // skip snapshot messages
    if matches!(m.is_snapshot, Some(true)) {
        return None;
    }

    // required core fields
    let symbol   = m.symbol.clone()?;
    let exchange = Exchange::from_str(m.exchange.as_deref()?)?;
    let price    = d_from_f64(m.trade_price)?;
    let size     = d_from_i32(m.trade_size)?;
    let time     = ts_from(m.ssboe, m.usecs);

    let side = match m.aggressor {
        Some(1) => Side::Buy,
        Some(2) => Side::Sell,
        _       => Side::None,
    };

    // optional metadata
    let exec_id: Option<String> = None;
    let maker_order_id = m.exchange_order_id.clone();
    let taker_order_id = m.aggressor_exchange_order_id.clone();
    let venue_seq: Option<u32> = None;

    // timestamps
    let ts_event = match (m.source_ssboe, m.source_usecs, m.source_nsecs) {
        (Some(s), Some(u), _) => Some(ts_from(Some(s), Some(u))),
        (Some(s), None, Some(n)) => Some(ts_from(Some(s), Some(n / 1_000))), // nsecs → usecs
        _ => None,
    };

    let ts_recv = match (m.jop_ssboe, m.jop_nsecs) {
        (Some(s), Some(n)) => Some(ts_from(Some(s), Some(n / 1_000))), // nsecs → usecs
        _ => None,
    };

    Some(Tick {
        symbol,
        exchange,
        price,
        size,
        time,
        side,
        exec_id,
        maker_order_id,
        taker_order_id,
        venue_seq,
        ts_event,
        ts_recv,
    })
}

#[inline]
pub(crate) fn map_rithmic_bbo(m: BestBidOffer) -> Option<Bbo> {
    if matches!(m.is_snapshot, Some(true)) {
        // keep or drop snapshots based on your policy
        // return None;
    }

    let symbol   = m.symbol.clone()?;
    let exchange = Exchange::from_str(m.exchange.as_deref()?)?;

    let bid      = d_from_f64(m.bid_price)?;
    let ask      = d_from_f64(m.ask_price)?;
    let bid_size = d_from_i32(m.bid_size)?;
    let ask_size = d_from_i32(m.ask_size)?;
    let time     = ts_from(m.ssboe, m.usecs); // your helper: (ssboe,usecs)->Utc

    Some(Bbo {
        symbol,
        exchange,
        bid,
        bid_size,
        ask,
        ask_size,
        time,
        bid_orders: m.bid_orders.map(|v| v as u32),
        ask_orders: m.ask_orders.map(|v| v as u32),
        venue_seq: None,
        is_snapshot: m.is_snapshot,
    })
}

/// For production, you’ll maintain a ladder and publish a normalized book.
/// Here we just expose changed levels in this frame.
#[inline]
fn map_order_book(m: &crate::websocket::rithmic_proto_objects::rti::OrderBook) -> Option<standard_lib::market_data::base_data::OrderBook> {
    let bids = m.bid_price.iter().zip(m.bid_size.iter())
        .filter_map(|(p,s)| Some((Decimal::from_f64(*p)?, Decimal::from_i32(*s)?)))
        .collect::<Vec<_>>();
    let asks = m.ask_price.iter().zip(m.ask_size.iter())
        .filter_map(|(p,s)| Some((Decimal::from_f64(*p)?, Decimal::from_i32(*s)?)))
        .collect::<Vec<_>>();
    let exchange = match &m.exchange {
        None => return None,
        Some(ex) => match Exchange::from_str(&ex) {
            None => {
                error!("Invalid exchange: {}", ex);
                return None;
            },
            Some(ex) => ex
        }
    };
    Some(standard_lib::market_data::base_data::OrderBook {
        symbol:   m.symbol.clone()?,
        exchange,
        bids,
        asks,
        time: ts_from(m.ssboe, m.usecs),
    })
}

#[inline]
fn d_from_f64(x: Option<f64>) -> Option<Decimal> { x.and_then(Decimal::from_f64) }
#[inline]
fn d_from_i32(x: Option<i32>) -> Option<Decimal> { x.and_then(Decimal::from_i32) }
#[inline]
fn ts_from(ssboe: Option<i32>, usecs: Option<i32>) -> DateTime<Utc> {
    match (ssboe, usecs) {
        (Some(s), Some(u)) if u < 1_000_000 => Utc.timestamp_opt(s as i64, u as u32 * 1_000).single().unwrap_or_else(Utc::now),
        _ => Utc::now(),
    }
}
