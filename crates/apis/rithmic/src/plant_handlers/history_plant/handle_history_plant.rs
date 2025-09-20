use std::str::FromStr;
use std::sync::Arc;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
#[allow(unused_imports)]
use crate::websocket::rithmic_proto_objects::rti::{AccountListUpdates, AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates, DepthByOrder, DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode, OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, RequestAccountList, RequestAccountRmsInfo, RequestHeartbeat, RequestLoginInfo, RequestMarketDataUpdate, RequestPnLPositionSnapshot, RequestPnLPositionUpdates, RequestProductCodes, RequestProductRmsInfo, RequestReferenceData, RequestTickBarUpdate, RequestTimeBarUpdate, RequestVolumeProfileMinuteBars, ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo, ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData, ResponseBracketOrder, ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot, ResponseDepthByOrderUpdates, ResponseEasyToBorrowList, ResponseExitPosition, ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying, ResponseGetInstrumentByUnderlyingKeys, ResponseGetVolumeAtPrice, ResponseGiveTickSizeTypeTable, ResponseHeartbeat, ResponseLinkOrders, ResponseListAcceptedAgreements, ResponseListExchangePermissions, ResponseListUnacceptedAgreements, ResponseLogin, ResponseLoginInfo, ResponseLogout, ResponseMarketDataUpdate, ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder, ResponseModifyOrderReferenceData, ResponseNewOrder, ResponseOcoOrder, ResponseOrderSessionConfig, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates, ResponseProductCodes, ResponseProductRmsInfo, ResponseReferenceData, ResponseReplayExecutions, ResponseResumeBars, ResponseRithmicSystemInfo, ResponseSearchSymbols, ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement, ResponseShowBracketStops, ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates, ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders, ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay, ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes, ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel, ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar, TradeRoute, TradeStatistics, UpdateEasyToBorrowList};
use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;
use prost::Message as ProstMessage;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;
use tracing::error;
use standard_lib::market_data::base_data::{Candle, Resolution};
use standard_lib::securities::market_hours::candle_end;
use standard_lib::securities::symbols::Exchange;
use crate::websocket::api_base::RithmicApiClient;
use crate::websocket::rithmic_proto_objects::rti::time_bar::BarType;

#[allow(dead_code, unused)]
#[inline]
pub async fn match_history_plant_id(
    template_id: i32, message_buf: Bytes,
    client: &Arc<RithmicApiClient>,
) {
    const PLANT: SysInfraType = SysInfraType::HistoryPlant;
    match template_id {
        19 => {}
        201 => {
            if let Ok(msg) = ResponseTimeBarUpdate::decode(&message_buf[..]) {
                // Time Bar Update Response
                // From Server
                //println!("Time Bar Update Response (Template ID: 201) from Server: {:?}", msg);
                if msg.rp_code[0] != "0" {
                    if msg.rp_code.len() > 1 {
                        for code in msg.rp_code {
                            error!("Rithmic subscribe fail: {}", code);
                        }
                    }
                }
            }
        },
        203 => {
            if let Ok(msg) = ResponseTimeBarReplay::decode(&message_buf[..]) {}
        },
        205 => {
            if let Ok(msg) = ResponseTickBarUpdate::decode(&message_buf[..]) {
                // Tick Bar Update Response
                // From Server
                //println!("Tick Bar Update Response (Template ID: 205) from Server: {:?}", msg);
                if msg.rp_code[0] != "0" {
                    if msg.rp_code.len() > 1 {
                        for code in msg.rp_code {
                            error!("Rithmic subscribe fail: {}", code);
                        }
                    }
                }
            }
        },
        207 => {

        },
        209 => {
            if let Ok(msg) = ResponseVolumeProfileMinuteBars::decode(&message_buf[..]) {
                // Volume Profile Minute Bars Response
                // From Server
                //println!("Volume Profile Minute Bars Response (Template ID: 209) from Server: {:?}", msg);
                if msg.rp_code[0] != "0" {
                    if msg.rp_code.len() > 1 {
                        for code in msg.rp_code {
                            error!("Rithmic subscribe fail: {}", code);
                        }
                    }
                }
            }
        },
        211 => {
            if let Ok(msg) = ResponseResumeBars::decode(&message_buf[..]) {
                // Resume Bars Response
                // From Server
                println!("Resume Bars Response (Template ID: 211) from Server: {:?}", msg);
            }
        },
        250 => {
            if let Ok(msg) = TimeBar::decode(&message_buf[..]) {
                // Time Bar
                // From Server
                //println!("Time Bar (Template ID: 250) from Server: {:?}", msg);
                if let Some(x) = map_time_bar(msg) {
                    client.hub.publish_candle(x);
                }
            }
        },
        251 => {
            if let Ok(msg) = TickBar::decode(&message_buf[..]) {
                // Tick Bar
                // From Server
                println!("Tick Bar (Template ID: 251) from Server: {:?}", msg);
            }
        },
        _ => println!("No match for template_id: {}", template_id)
    }
}


pub(crate) fn map_time_bar(msg: TimeBar) -> Option<Candle> {
    //println!("Time Bar (Template ID: 250) from Server: {:?}", msg);
    let time_start = msg.marker
        .and_then(|marker| Utc.timestamp_opt(marker as i64, 0).single()).unwrap();

    let symbol = match msg.symbol.clone() {
        None => return None,
        Some(symbol) => symbol,
    };

    // Deserialize the exchange field
    let exchange = match msg.exchange {
        None => return None,
        Some(ex) => match Exchange::from_str(&ex) {
            None => {
                error!("Invalid exchange: {}", ex);
                return None;
            },
            Some(ex) => ex
        }
    };

    let period = match msg.period.clone() {
        Some(p) => match p.parse::<u64>().ok() {
            None => return None,
            Some(period) => period
        },
        None => return None,
    };
    // Retrieve broadcaster for the symbol
    // Construct the symbol object

    let high = match msg.high_price.and_then(Decimal::from_f64) {
        Some(price) => price,
        None => return None,  // Exit if high price is invalid
    };

    let low = match msg.low_price.and_then(Decimal::from_f64) {
        Some(price) => price,
        None => return None,  // Exit if low price is invalid
    };

    let open = match msg.open_price.and_then(Decimal::from_f64) {
        Some(price) => price,
        None => return None,  // Exit if open price is invalid
    };

    let close = match msg.close_price.and_then(Decimal::from_f64) {
        Some(price) => price,
        None => return None,  // Exit if close price is invalid
    };


    let mut resolution = match msg.r#type.clone() {
        Some(val) => {
            match val {
                1 => Resolution::Seconds(period as u8),
                2 => Resolution::Minutes(period as u8),
                _ => // Try parsing as i32 first
                    match i32::from_str(val.to_string().as_str()) {
                        Ok(1) => Resolution::Seconds(period as u8),
                        Ok(2) => Resolution::Minutes(period as u8),
                        // If parsing as number fails, try as bar type string
                        _ => match BarType::from_str_name(&val.to_string().as_str()) {
                            Some(BarType::SecondBar) => Resolution::Seconds(period as u8),
                            Some(BarType::MinuteBar) => Resolution::Minutes(period as u8),
                            Some(BarType::DailyBar) => Resolution::Daily,
                            Some(BarType::WeeklyBar) => Resolution::Weekly, // Unsupported bar types
                            None => return None, // Unknown bar type
                        }
                    }
            }
        },
        None => {
            error!("Invalid resolution: {:?}", msg.r#type);
            return None
        }, // Exit if msg.r#type is None
    };

    // Convert resolution if needed
    if let Resolution::Minutes(mins) = resolution {
        if mins >= 60 && mins % 60 == 0 {
            resolution = Resolution::Hours(mins / 60);
        }
    } else if resolution == Resolution::Seconds(60) {
        resolution = Resolution::Minutes(1);
    }

    let volume = msg.volume.and_then(Decimal::from_u64).unwrap_or_else(|| dec!(0.0));
    let ask_volume = msg.ask_volume.and_then(Decimal::from_u64).unwrap_or_else(|| dec!(0.0));
    let bid_volume = msg.bid_volume.and_then(Decimal::from_u64).unwrap_or_else(|| dec!(0.0));
    let num_of_trades = msg.num_trades.and_then(Decimal::from_u64).unwrap_or_else(|| dec!(0.0));

    let time_end = match candle_end(time_start, resolution, exchange) {
        None => {
            error!("Invalid time_end: {:?}", time_start);
            return None;
        },
        Some(t) => t
    };

    // Construct the candle
    Some(Candle {
        symbol,
        exchange,
        time_start,
        high,
        low,
        open,
        close,
        volume,
        resolution,
        time_end,
        ask_volume,
        num_of_trades,
        bid_volume,
    })
}
