/*use std::str::FromStr;
use std::sync::Arc;
use ahash::AHashMap;
use chrono::{DateTime, TimeZone, Utc};
use dashmap::DashMap;
#[allow(unused_imports)]
use crate::apis::rithmic::rithmic_proto_objects::rti::{AccountListUpdates, AccountPnLPositionUpdate, AccountRmsUpdates, BestBidOffer, BracketUpdates, DepthByOrder, DepthByOrderEndEvent, EndOfDayPrices, ExchangeOrderNotification, FrontMonthContractUpdate, IndicatorPrices, InstrumentPnLPositionUpdate, LastTrade, MarketMode, OpenInterest, OrderBook, OrderPriceLimits, QuoteStatistics, RequestAccountList, RequestAccountRmsInfo, RequestHeartbeat, RequestLoginInfo, RequestMarketDataUpdate, RequestPnLPositionSnapshot, RequestPnLPositionUpdates, RequestProductCodes, RequestProductRmsInfo, RequestReferenceData, RequestTickBarUpdate, RequestTimeBarUpdate, RequestVolumeProfileMinuteBars, ResponseAcceptAgreement, ResponseAccountList, ResponseAccountRmsInfo, ResponseAccountRmsUpdates, ResponseAuxilliaryReferenceData, ResponseBracketOrder, ResponseCancelAllOrders, ResponseCancelOrder, ResponseDepthByOrderSnapshot, ResponseDepthByOrderUpdates, ResponseEasyToBorrowList, ResponseExitPosition, ResponseFrontMonthContract, ResponseGetInstrumentByUnderlying, ResponseGetInstrumentByUnderlyingKeys, ResponseGetVolumeAtPrice, ResponseGiveTickSizeTypeTable, ResponseHeartbeat, ResponseLinkOrders, ResponseListAcceptedAgreements, ResponseListExchangePermissions, ResponseListUnacceptedAgreements, ResponseLogin, ResponseLoginInfo, ResponseLogout, ResponseMarketDataUpdate, ResponseMarketDataUpdateByUnderlying, ResponseModifyOrder, ResponseModifyOrderReferenceData, ResponseNewOrder, ResponseOcoOrder, ResponseOrderSessionConfig, ResponsePnLPositionSnapshot, ResponsePnLPositionUpdates, ResponseProductCodes, ResponseProductRmsInfo, ResponseReferenceData, ResponseReplayExecutions, ResponseResumeBars, ResponseRithmicSystemInfo, ResponseSearchSymbols, ResponseSetRithmicMrktDataSelfCertStatus, ResponseShowAgreement, ResponseShowBracketStops, ResponseShowBrackets, ResponseShowOrderHistory, ResponseShowOrderHistoryDates, ResponseShowOrderHistoryDetail, ResponseShowOrderHistorySummary, ResponseShowOrders, ResponseSubscribeForOrderUpdates, ResponseSubscribeToBracketUpdates, ResponseTickBarReplay, ResponseTickBarUpdate, ResponseTimeBarReplay, ResponseTimeBarUpdate, ResponseTradeRoutes, ResponseUpdateStopBracketLevel, ResponseUpdateTargetBracketLevel, ResponseVolumeProfileMinuteBars, RithmicOrderNotification, SymbolMarginRate, TickBar, TimeBar, TradeRoute, TradeStatistics, UpdateEasyToBorrowList};
use crate::apis::rithmic::rithmic_proto_objects::rti::{Reject, RequestNewOrder};
use lazy_static::lazy_static;
use prost::Message as ProstMessage;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal_macros::dec;
use tokio::sync::mpsc::Sender;
use crate::apis::rithmic::rithmic_systems::RithmicSystem;
use crate::product_maps::rithmic::maps::{find_base_symbol, get_exchange_by_symbol_name};
use crate::standardized_types::accounts::{Account, AccountInfo};
use crate::standardized_types::enums::{FuturesExchange, OrderSide, StrategyMode};
use crate::standardized_types::new_types::{Price, Volume};
use crate::standardized_types::orders::{Order, OrderId, OrderState, OrderType, OrderUpdateEvent, OrderUpdateType, TimeInForce};
use crate::rithmic_api::api_client::{get_front_month, request_updates, send_message, RithmicCallbackResponse, DEFAULT_TRADE_ROUTES, FCM_ID, IB_ID, LAST_HEARTBEAT, SYSTEM};
use crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_login::SysInfraType;
use crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_new_order::{OrderPlacement, PriceType, TransactionType};
use crate::rithmic_api::plant_handlers::create_datetime;
use crate::standardized_types::subscriptions::Symbol;
use crate::strategies::fund_forge_strategy::Engine;
use crate::strategies::strategy_events::StrategyEvent;

type BasketId = String;
lazy_static! {
    pub static ref BASKET_ID_TO_ID_MAP: DashMap<BasketId , OrderId> = DashMap::new();
    pub static ref ID_TO_BASKET_ID_MAP: DashMap<OrderId, BasketId> = DashMap::new();
    pub static ref ID_TO_TAG: DashMap<OrderId , String> = DashMap::new();
    pub static ref ACCOUNT_INFO: DashMap<Account, AccountInfo> = DashMap::new();
    pub static ref PENDING_ORDERS: DashMap<OrderId, Order> = DashMap::new();
    pub static ref COMPLETED_ORDERS: DashMap<OrderId, Order> = DashMap::new();
    pub static ref PENDING_UPDATES: DashMap<OrderId, OrderUpdateType> = DashMap::new();
}

pub async fn rithmic_order_details(order: &Order) -> Result<CommonRithmicOrderDetails, OrderUpdateEvent> {
    let quantity = match order.quantity_open.to_i32() {
        None => {
            return Err(reject_order(&order, "Invalid quantity".to_string()))
        }
        Some(q) => q
    };

    let (symbol_code, exchange): (Symbol, FuturesExchange) = {
        match get_exchange_by_symbol_name(&order.symbol) {
            None => return Err(reject_order(&order, format!("Exchange Not found with {} for {}", order.account, order.symbol))),
            Some(mut exchange) => {
                let front_month = match get_front_month(order.symbol.clone(), exchange.clone()).await {
                    Ok(info) => info,
                    Err(e) => return Err(reject_order(&order, format!("{}", e)))
                };
                (front_month.symbol, exchange)
            }
        }
    };

    let fcm_id = crate::rithmic_api::api_client::FCM_ID.get().expect("FCM_ID not initialized");
    let system = crate::rithmic_api::api_client::SYSTEM.get().expect("SYSTEM not initialized");

    let route = match DEFAULT_TRADE_ROUTES.get(&(fcm_id.clone(), exchange.clone())) {
        None => {
            match system {
                RithmicSystem::RithmicPaperTrading | RithmicSystem::TopstepTrader | RithmicSystem::SpeedUp | RithmicSystem::TradeFundrr | RithmicSystem::UProfitTrader | RithmicSystem::Apex | RithmicSystem::MESCapital |
                RithmicSystem::TheTradingPit | RithmicSystem::FundedFuturesNetwork | RithmicSystem::Bulenox | RithmicSystem::PropShopTrader | RithmicSystem::FourPropTrader | RithmicSystem::FastTrackTrading
                => "simulator".to_string(),
                //RithmicSystem::Rithmic01 => "globex".to_string(),
                _ => return Err(reject_order(&order, format!("Order Route Not found with {} for {}", order.account, order.symbol)))
            }
        }
        Some(route) => route.value().clone()
    };
    //println!("Route: {}", route);

    let transaction_type = match order.side {
        OrderSide::Buy => TransactionType::Buy,
        OrderSide::Sell => TransactionType::Sell,
    };

    PENDING_ORDERS.insert(order.id.clone(), order.clone());

    Ok(CommonRithmicOrderDetails {
        symbol_code,
        exchange,
        transaction_type,
        route,
        quantity,
    })
}

fn reject_order(order: &Order, reason: String) -> OrderUpdateEvent {
    OrderUpdateEvent::OrderRejected {
        account: order.account.clone(),
        symbol: order.symbol.clone(),
        order_id: order.id.clone(),
        reason,
        tag: order.tag.clone(),
        time: Utc::now().to_string()
    }
}

pub async fn submit_order(mut order: Order, details: CommonRithmicOrderDetails) -> Result<(), OrderUpdateEvent> {
    let (duration, cancel_at_ssboe, cancel_at_usecs) = match order.time_in_force {
        TimeInForce::IOC => (crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Ioc.into(), None, None),
        TimeInForce::FOK => (crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Fok.into(), None, None),
        TimeInForce::GTC => (crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Gtc.into(), None, None),
        TimeInForce::Day => (crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Day.into(), None, None),
        TimeInForce::Time(ref time_stamp) => {
            let cancel_time = match DateTime::<Utc>::from_timestamp(*time_stamp, 0) {
                Some(dt) => dt,
                None => return Err(reject_order(&order, format!("Failed to parse time stamp: {}", time_stamp)))
            };

            (crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Gtc.into(),
             Some(cancel_time.timestamp() as i32),
             Some(cancel_time.timestamp_subsec_micros() as i32))
        }
    };

    // Rest of the function remains the same...
    /*match order.side {
        OrderSide::Buy => eprintln!("Buying {}" , order.quantity_open),
        OrderSide::Sell => eprintln!("Selling {}" , order.quantity_open),
    }*/

    let trigger_price = match order.trigger_price {
        None => None,
        Some(price) => {
            match price.to_f64() {
                None => return Err(reject_order(&order, format!("Failed to parse trigger price: {}", price))),
                Some(price) => Some(price)
            }
        }
    };

    let limit_price = match order.limit_price {
        None => None,
        Some(price) => {
            match price.to_f64() {
                None => return Err(reject_order(&order, format!("Failed to parse limit price: {}", price))),
                Some(price) => Some(price)
            }
        }
    };

    let order_type = match order.order_type {
        OrderType::Limit => 1,
        OrderType::Market => 2,
        OrderType::MarketIfTouched => 5,
        OrderType::StopMarket => 4,
        OrderType::StopLimit => 3,
        OrderType::EnterLong => 2,
        OrderType::EnterShort => 2,
        OrderType::ExitLong => 2,
        OrderType::ExitShort => 2,
    };

    if order.exchange.is_none() {
        order.exchange = Some(details.exchange);
    }

    let req = RequestNewOrder {
        template_id: 312,
        user_msg: vec![order.account.clone(), order.tag.clone(), order.symbol.clone(), details.symbol_code.clone()],
        user_tag: Some(order.id.clone()),
        window_name: Some("fund forge submit_order()".to_string()),
        fcm_id: Some(crate::rithmic_api::api_client::FCM_ID.get().expect("FCM_ID not initialized").clone()),
        ib_id: Some(crate::rithmic_api::api_client::IB_ID.get().expect("IB_ID not initialized").clone()),
        account_id: Some(order.account.clone()),
        symbol: Some(details.symbol_code.clone()),
        exchange: Some(details.exchange.to_string()),
        quantity: Some(details.quantity),
        price: limit_price,
        trigger_price,
        transaction_type: Some(details.transaction_type.into()),
        duration: Some(duration),
        price_type: Some(order_type),
        trade_route: Some(details.route),
        manual_or_auto: Some(OrderPlacement::Auto.into()),
        trailing_stop: None,
        trail_by_ticks: None,
        trail_by_price_id: None,
        release_at_ssboe: None,
        release_at_usecs: None,
        cancel_at_ssboe,
        cancel_at_usecs,
        cancel_after_secs: None,
        if_touched_symbol: None,
        if_touched_exchange: None,
        if_touched_condition: None,
        if_touched_price_field: None,
        if_touched_price: None,
    };

    send_message(&SysInfraType::OrderPlant, req).await;
    Ok(())
}

pub async fn submit_market_order(mut order: Order, details: CommonRithmicOrderDetails) {
    let duration = match order.time_in_force {
        TimeInForce::IOC => crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Ioc.into(),
        TimeInForce::FOK => crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Fok.into(),
        _ => crate::rithmic_api::client_base::rithmic_proto_objects::rti::request_bracket_order::Duration::Fok.into()
    };

    if order.exchange.is_none() {
        order.exchange = Some(details.exchange);
    }

    let req = RequestNewOrder {
        template_id: 312,
        user_msg: vec![order.account.clone(), order.tag.clone(), order.symbol.clone()],
        user_tag: Some(order.id.clone()),
        window_name: None, //removed this
        fcm_id: Some(FCM_ID.get().expect("FCM_ID not initialized").clone()),
        ib_id: Some(crate::rithmic_api::api_client::IB_ID.get().expect("IB_ID not initialized").clone()),
        account_id: Some(order.account.clone()),
        symbol: Some(details.symbol_code.clone()),
        exchange: Some(details.exchange.to_string()),
        quantity: Some(details.quantity),
        price: None,
        trigger_price: None,
        transaction_type: Some(details.transaction_type.into()),
        duration: Some(duration),
        price_type: Some(PriceType::Market.into()),
        trade_route: Some(details.route),
        manual_or_auto: Some(OrderPlacement::Auto.into()),
        trailing_stop: None,
        trail_by_ticks: None,
        trail_by_price_id: None,
        release_at_ssboe: None,
        release_at_usecs: None,
        cancel_at_ssboe: None,
        cancel_at_usecs: None,
        cancel_after_secs: None,
        if_touched_symbol: None,
        if_touched_exchange: None,
        if_touched_condition: None,
        if_touched_price_field: None,
        if_touched_price: None,
    };

    send_message(&SysInfraType::OrderPlant, req).await;
}


pub async fn match_order_plant_id(
    template_id: i32, message_buf: Vec<u8>,
    engine: Arc<Engine>,
) {
    let fcm_id = FCM_ID.get().expect("FCM_ID not initialized").clone();
    let ib_id = IB_ID.get().expect("IB_ID not initialized").clone();
    let system = SYSTEM.get().expect("SYSTEM not initialized").clone();

    const PLANT: SysInfraType = SysInfraType::OrderPlant;
    match template_id {
        75 => {
            if let Ok(msg) = Reject::decode(&message_buf[..]) {
                // Login Response
                // From Server
                println!("Reject Response (Template ID: 11) from Server: {:?}", msg);
            }
        }
        15 => {
            if let Ok(msg) = ResponseReferenceData::decode(&message_buf[..]) {
                // Reference Data Response
                // From Server
                println!("Reference Data Response (Template ID: 15) from Server: {:?}", msg);
            }
        },
        17 => {
            if let Ok(msg) = ResponseRithmicSystemInfo::decode(&message_buf[..]) {
                // Rithmic System Info Response
                // From Server
                println!("Rithmic System Info Response (Template ID: 17) from Server: {:?}", msg);
            }
        },
        19 => {
            if let Ok(msg) = ResponseHeartbeat::decode(&message_buf[..]) {
                // Response Heartbeat
                // From Server
                //println!("Response Heartbeat (Template ID: 19) from Server: {:?}", msg);
                if let (Some(ssboe), Some(usecs)) = (msg.ssboe, msg.usecs) {
                    // Convert heartbeat timestamp to DateTime<Utc>
                    let heartbeat_time = match Utc.timestamp_opt(ssboe as i64, (usecs * 1000) as u32) {
                        chrono::LocalResult::Single(dt) => dt,
                        _ => return, // Skip if timestamp is invalid
                    };

                    // Calculate latency
                    let now = Utc::now();
                    // Store both the send time and latency
                    LAST_HEARTBEAT.insert(PLANT, now);
                }
            }
        },
        301 => {
            if let Ok(msg) = ResponseLoginInfo::decode(&message_buf[..]) {
                // Account List Response
                // From Server
                println!("Response Login Info (Template ID: 303) from Server: {:?}", msg);
            }
        }
        303 => {
            if let Ok(msg) = ResponseAccountList::decode(&message_buf[..]) {
                // Account List Response
                // From Server
                println!("Account List Response (Template ID: 303) from Server: {:?}", msg);
            }
        },
        305 => {
            if let Ok(msg) = ResponseAccountRmsInfo::decode(&message_buf[..]) {
                println!("Response Account Rms Info (Template ID: 305) from Server: {:?}", msg);
                if let Some(id) = msg.account_id {
                    let mut account_info = AccountInfo {
                        account_id: id.to_string(),
                        system: system.clone(),
                        cash_value: Default::default(),
                        cash_available: Default::default(),
                        open_pnl: Default::default(),
                        booked_pnl: Default::default(),
                        day_open_pnl: Default::default(),
                        day_booked_pnl: Default::default(),
                        cash_used: Default::default(),
                        positions: vec![],
                        is_hedging: false,
                        buy_limit: None,
                        sell_limit: None,
                        max_orders: None,
                        daily_max_loss: None,
                        daily_max_loss_reset_time: None,
                        leverage: 0
                    };
                    if let Some(ref max_size) = msg.buy_limit {
                        account_info.buy_limit = Some(Decimal::from_u32(max_size.clone() as u32).unwrap());
                    }
                    if let Some(ref max_size) = msg.sell_limit {
                        account_info.sell_limit = Some(Decimal::from_u32(max_size.clone() as u32).unwrap());
                    }
                    if let Some(ref max_orders) = msg.max_order_quantity {
                        account_info.max_orders = Some(Decimal::from_u32(max_orders.clone() as u32).unwrap());
                    }
                    if let Some(ref max_loss) = msg.loss_limit {
                        account_info.daily_max_loss = Some(Decimal::from_u32(max_loss.clone() as u32).unwrap());
                    }
                    ACCOUNT_INFO.insert(id.clone(), account_info);
                    request_updates(id.to_string()).await;
                    println!("Account Info Added: {}", id);
                }
            }
        },
        307 => {
            if let Ok(msg) = ResponseProductRmsInfo::decode(&message_buf[..]) {
                // Product RMS Info Response
                // From Server
                println!("Product RMS Info Response (Template ID: 307) from Server: {:?}", msg);
            }
        },
        309 => {
            if let Ok(msg) = ResponseSubscribeForOrderUpdates::decode(&message_buf[..]) {
                // Subscribe For Order Updates Response
                // From Server
                println!("Subscribe For Order Updates Response (Template ID: 309) from Server: {:?}", msg);
            }
        },
        311 => {
            if let Ok(msg) = ResponseTradeRoutes::decode(&message_buf[..]) {
                // Trade Routes Response
                // From Server
                if let Some(ref id) = msg.fcm_id {
                    if *id != fcm_id {
                        return;
                    }
                }
                if let Some(route) = &msg.trade_route {
                    if let Some(exchange) = &msg.exchange {
                        let exchange = match FuturesExchange::from_string(&exchange) {
                            Ok(exchange) => exchange,
                            Err(_) => return,
                        };
                        if let Some(fcm_id) = msg.fcm_id {
                            DEFAULT_TRADE_ROUTES.insert((fcm_id, exchange.clone()), route.clone());
                        }
                    }
                }
            }
        },
        313 => {
            if let Ok(msg) = ResponseNewOrder::decode(&message_buf[..]) {
                // New Order Response
                // From Server
               //println!("New Order Response (Template ID: 313) from Server: {:?}", msg);
                if let (Some(basket_id), Some(ssboe), Some(usecs)) = (msg.basket_id, msg.ssboe, msg.usecs) {
                    let time = create_datetime(ssboe as i64, usecs as i64).to_string();

                    let order_id = match msg.user_tag {
                        None => return,
                        Some(order_id) => order_id
                    };

                    BASKET_ID_TO_ID_MAP.insert(basket_id.clone(), order_id.clone());
                    ID_TO_BASKET_ID_MAP.insert(order_id.clone(), basket_id.clone());

                    let stream_name = match msg.user_msg.get(0) {
                        None => return,
                        Some(stream_name) => stream_name
                    };

                    let stream_name = u16::from_str(&stream_name).unwrap_or_default();

                    let tag = match msg.user_msg.get(2) {
                        None => return,
                        Some(tag) => tag
                    };
                    ID_TO_TAG.insert(order_id.clone(), tag.clone());

                    let account_id = match msg.user_msg.get(1) {
                        None => return,
                        Some(id) => id
                    };

                    let symbol_name = match msg.user_msg.get(3) {
                        None => return,
                        Some(name) => name.clone()
                    };

                    if let Some(mut kvp) = PENDING_ORDERS.get_mut(&order_id) {
                        if kvp.state == OrderState::Accepted {
                            return;
                        }
                        kvp.value_mut().state = OrderState::Accepted;
                    }

                    let event = OrderUpdateEvent::OrderAccepted {
                        account: account_id.clone(),
                        symbol: symbol_name.clone(),
                        order_id: order_id.clone(),
                        tag: tag.to_owned(),
                        time,
                    };
                    let strategy_event = StrategyEvent::OrderEvents(event);
                    match engine.strategy_event_sender.send(strategy_event).await {
                        Ok(_) => {},
                        Err(e) => {
                            println!("Error sending Order Accepted: {}, {}", order_id, e);
                        }
                    }
                }
            }
        },
        315 => {
            if let Ok(msg) = ResponseModifyOrder::decode(&message_buf[..]) {
                // Modify Order Response
                // From Server
                println!("Modify Order Response (Template ID: 315) from Server: {:?}", msg);
            }
        },
        317 => {
            if let Ok(msg) = ResponseCancelOrder::decode(&message_buf[..]) {
                // Cancel Order Response
                // From Server
                println!("Cancel Order Response (Template ID: 317) from Server: {:?}", msg);
            }
        },
        319 => {
            if let Ok(msg) = ResponseShowOrderHistoryDates::decode(&message_buf[..]) {
                // Show Order History Dates Response
                // From Server
                println!("Show Order History Dates Response (Template ID: 319) from Server: {:?}", msg);
            }
        },
        321 => {
            if let Ok(msg) = ResponseShowOrders::decode(&message_buf[..]) {
                // Show Orders Response
                // From Server
                println!("Show Orders Response (Template ID: 321) from Server: {:?}", msg);
            }
        },
        323 => {
            if let Ok(msg) = ResponseShowOrderHistory::decode(&message_buf[..]) {
                // Show Order History Response
                // From Server
                println!("Show Order History Response (Template ID: 323) from Server: {:?}", msg);
            }
        },
        325 => {
            if let Ok(msg) = ResponseShowOrderHistorySummary::decode(&message_buf[..]) {
                // Show Order History Summary Response
                // From Server
                println!("Show Order History Summary Response (Template ID: 325) from Server: {:?}", msg);
            }
        },
        327 => {
            if let Ok(msg) = ResponseShowOrderHistoryDetail::decode(&message_buf[..]) {
                // Show Order History Detail Response
                // From Server
                println!("Show Order History Detail Response (Template ID: 327) from Server: {:?}", msg);
            }
        },
        329 => {
            if let Ok(msg) = ResponseOcoOrder::decode(&message_buf[..]) {
                // OCO Order Response
                // From Server
                println!("OCO Order Response (Template ID: 329) from Server: {:?}", msg);
            }
        },
        331 => {
            if let Ok(msg) = ResponseBracketOrder::decode(&message_buf[..]) {
                // Bracket Order Response
                // From Server
                println!("Bracket Order Response (Template ID: 331) from Server: {:?}", msg);
            }
        },
        333 => {
            if let Ok(msg) = ResponseUpdateTargetBracketLevel::decode(&message_buf[..]) {
                // Update Target Bracket Level Response
                // From Server
                println!("Update Target Bracket Level Response (Template ID: 333) from Server: {:?}", msg);
            }
        },
        335 => {
            if let Ok(msg) = ResponseUpdateStopBracketLevel::decode(&message_buf[..]) {
                // Update Stop Bracket Level Response
                // From Server
                println!("Update Stop Bracket Level Response (Template ID: 335) from Server: {:?}", msg);
            }
        },
        337 => {
            if let Ok(msg) = ResponseSubscribeToBracketUpdates::decode(&message_buf[..]) {
                // Subscribe To Bracket Updates Response
                // From Server
                println!("Subscribe To Bracket Updates Response (Template ID: 337) from Server: {:?}", msg);
            }
        },
        339 => {
            if let Ok(msg) = ResponseShowBrackets::decode(&message_buf[..]) {
                // Show Brackets Response
                // From Server
                println!("Show Brackets Response (Template ID: 339) from Server: {:?}", msg);
            }
        },
        341 => {
            if let Ok(msg) = ResponseShowBracketStops::decode(&message_buf[..]) {
                // Show Bracket Stops Response
                // From Server
                println!("Show Bracket Stops Response (Template ID: 341) from Server: {:?}", msg);
            }
        },
        343 => {
            if let Ok(msg) = ResponseListExchangePermissions::decode(&message_buf[..]) {
                // List Exchange Permissions Response
                // From Server
                println!("List Exchange Permissions Response (Template ID: 343) from Server: {:?}", msg);
            }
        },
        345 => {
            if let Ok(msg) = ResponseLinkOrders::decode(&message_buf[..]) {
                // Link Orders Response
                // From Server
                println!("Link Orders Response (Template ID: 345) from Server: {:?}", msg);
            }
        },
        347 => {
            if let Ok(msg) = ResponseCancelAllOrders::decode(&message_buf[..]) {
                // Cancel All Orders Response
                // From Server
                println!("Cancel All Orders Response (Template ID: 347) from Server: {:?}", msg);
            }
        },
        349 => {
            if let Ok(msg) = ResponseEasyToBorrowList::decode(&message_buf[..]) {
                // Easy To Borrow List Response
                // From Server
                println!("Easy To Borrow List Response (Template ID: 349) from Server: {:?}", msg);
            }
        },
        350 => {
            if let Ok(msg) = TradeRoute::decode(&message_buf[..]) {
                if let Some(ref id) = msg.fcm_id {
                    if *id != fcm_id {
                        return;
                    }
                }
                // Trade Route
                // From Server
                //println!("Trade Route (Template ID: 350) from Server: {:?}", msg);
                if let Some(route) = &msg.trade_route {
                    if let Some(exchange) = &msg.exchange {
                        //println!("Trade Routes Response (Template ID: 311) from Server: {:?}", msg);
                        let exchange = match FuturesExchange::from_string(&exchange) {
                            Ok(exchange) => exchange,
                            Err(_) => return,
                        };
                        if let Some(fcm_id) = msg.fcm_id {
                            DEFAULT_TRADE_ROUTES.insert((fcm_id, exchange.clone()), route.clone());
                        }
                    }
                }
            }
        },
        351 => {
            // Rithmic Order Notification
            // From Server
            /*
            Rithmic Order Notification (Template ID: 351) from Server: RithmicOrderNotification { template_id: 351, user_tag: None, notify_type: Some(OrderRcvdFromClnt),
            is_snapshot: None, status: Some("Order received from websocket"), basket_id: Some("233651480"), original_basket_id: None, linked_basket_ids: None,
            fcm_id: Some("TopstepTrader"), ib_id: Some("TopstepTrader"), user_id: Some("kevtaz"), account_id: Some("S1Sep246906077"), symbol: Some("M6AZ4"),
            exchange: Some("CME"), trade_exchange: Some("CME"), trade_route: Some("simulator"), exchange_order_id: None, instrument_type: None,
            completion_reason: None, quantity: Some(2), quan_release_pending: None, price: None, trigger_price: None, transaction_type: Some(Sell),
            duration: Some(Day), price_type: Some(Market), orig_price_type: Some(Market), manual_or_auto: Some(Manual), bracket_type: None,
            avg_fill_price: None, total_fill_size: None, total_unfilled_size: None, trail_by_ticks: None, trail_by_price_id: None, sequence_number: None,
            orig_sequence_number: None, cor_sequence_number: None, currency: None, country_code: None, text: None, report_text: None, remarks: None,
            window_name: Some("Quote Board, Sell Button, Confirm "), originator_window_name: None, cancel_at_ssboe: None, cancel_at_usecs: None, cancel_after_secs: None,
            ssboe: Some(1729085413), usecs: Some(477767) }
            */
            if let Ok(msg) = RithmicOrderNotification::decode(&message_buf[..]) {
                //todo I think these are only for rithmic web or r trader orders
                //println!("Rithmic Order Notification (Template ID: 351) from Server: {:?}", msg);
               /* if let (Some(basket_id), Some(ssboe), Some(usecs), Some(account_id), Some(notify_type), Some(order_id)) =
                    (msg.basket_id, msg.ssboe, msg.usecs, msg.account_id, msg.notify_type, msg.user_tag) {
                    let time = create_datetime(ssboe as i64, usecs as i64).to_string();
                    let notify_type = match NotifyType::try_from(notify_type) {
                        Err(e) => return,
                        Ok(notify) => notify
                    };

                    let tag = match ID_TO_TAG.get_requests(&order_id) {
                        None => {
                            eprintln!("Tag not found for order: {}", order_id);
                            return;
                        },
                        Some(tag) => tag.value().clone()
                    };
                    match notify_type {
                        NotifyType::Open => {
                            //todo, we dont need to do this here
                        /*    let event = OrderUpdateEvent::OrderAccepted {
                                brokerage: websocket.brokerage.clone(),
                                account_id: AccountId::from(account_id),
                                order_id: order_id.clone(),
                                tag,
                                time,
                            };
                            send_order_update(&order_id, event).await;*/
                        },
                        NotifyType::Complete => {
                            if msg.completion_reason == Some("Fill".to_string()) {
                                if let (Some(price), Some(quantity)) = (msg.price, msg.quantity) {
                                    let price = match Decimal::from_f64_retain(price) {
                                        None => return,
                                        Some(p) => p
                                    };
                                    let quantity = match Decimal::from_i32(quantity) {
                                        None => return,
                                        Some(q) => q
                                    };
                                    let event = OrderUpdateEvent::OrderFilled {
                                        brokerage: websocket.brokerage.clone(),
                                        account_id: AccountId::from(account_id),
                                        order_id: order_id.clone(),
                                        price,
                                        quantity,
                                        tag,
                                        time,
                                    };
                                    send_order_update(&order_id, event).await;
                                }
                            }
                        },
                        NotifyType::Modified => {
                            // Assuming you have an OrderUpdated event
                            if let Some((order_id, update_type)) = ID_UPDATE_TYPE.remove(&order_id) {
                                let event = OrderUpdateEvent::OrderUpdated {
                                    brokerage: websocket.brokerage.clone(),
                                    account_id: AccountId::from(account_id),
                                    update_type,
                                    order_id: order_id.clone(),
                                    tag,
                                    time,
                                };
                                send_order_update(&order_id, event).await;
                            }
                        },
                        NotifyType::ModificationFailed | NotifyType::CancellationFailed => {
                            let event = OrderUpdateEvent::OrderUpdateRejected {
                                brokerage: websocket.brokerage.clone(),
                                account_id: AccountId::from(account_id),
                                order_id: order_id.clone(),
                                reason: msg.status.unwrap_or_default(),
                                time,
                            };
                            send_order_update(&order_id, event).await;
                        },
                        _ => return,  // Ignore other notification types
                    };
                    // You can handle the order_event here
                }*/
            }
        },

        352 => {
            // Exchange Order Notification
            // From Server
            /*
            Exchange Order Notification (Template ID: 352) from Server: ExchangeOrderNotification {
            template_id: 352, user_tag: Some("Sell Market: Rithmic:S1Sep246906077, MNQ, 4"), notify_type: Some(Fill),
            is_snapshot: Some(true), is_rithmic_internal_msg: None, report_type: Some("fill"), status: Some("complete"),
            basket_id: Some("234556404"), original_basket_id: None, linked_basket_ids: None, fcm_id: Some("TopstepTrader"),
            ib_id: Some("TopstepTrader"), user_id: Some("kevtaz"), account_id: Some("S1Sep246906077"), symbol: Some("MNQZ4"),
            exchange: Some("CME"), trade_exchange: Some("CME"), trade_route: Some("simulator"), exchange_order_id: Some("234556404"),
            tp_exchange_order_id: None, instrument_type: Some("Future"), quantity: Some(1), price: None, trigger_price: None,
            transaction_type: Some(Sell), duration: Some(Fok), price_type: Some(Market), orig_price_type: Some(Market), manual_or_auto: Some(Auto),
            bracket_type: None, confirmed_size: Some(1), confirmed_time: Some("02:34:50"), confirmed_date: Some("20241018"), confirmed_id: Some("1606359"),
            modified_size: None, modified_time: None, modified_date: None, modify_id: None, cancelled_size: Some(0), cancelled_time: None, cancelled_date: None,
            cancelled_id: None, fill_price: Some(20386.25), fill_size: Some(1), fill_time: Some("02:34:50"), fill_date: Some("20241018"), fill_id: Some("1606360"),
            avg_fill_price: Some(20386.25), total_fill_size: Some(1), total_unfilled_size: Some(0), trigger_id: None, trail_by_ticks: None, trail_by_price_id: None,
            sequence_number: Some("Z1XH2"), orig_sequence_number: Some("Z1XH2"), cor_sequence_number: Some("Z1XH2"), currency: Some("USD"), country_code: None, text: None,
            report_text: None, remarks: None, window_name: Some(""), originator_window_name: None, cancel_at_ssboe: None, cancel_at_usecs: None, cancel_after_secs: None,
            ssboe: Some(1729218890), usecs: Some(913270), exch_receipt_ssboe: None, exch_receipt_nsecs: None }
            */
            if let Ok(msg) = ExchangeOrderNotification::decode(&message_buf[..]) {
                //println!("Exchange Order Notification (Template ID: 352) from Server: {:?}", msg);
                if let (Some(basket_id), Some(ssboe), Some(usecs), Some(account_id), Some(notify_type), Some(user_tag)) =
                    (msg.basket_id, msg.ssboe, msg.usecs, msg.account_id, msg.notify_type, msg.user_tag) {
                    let time = create_datetime(ssboe as i64, usecs as i64).to_string();

                    //todo[Rithmic Api] Not sure if i should do this or not
                    match msg.is_snapshot {
                        None => {}
                        Some(some) => {
                            match some {
                                true => return,
                                false => {}
                            }
                        }
                    }
                    let order_id = match BASKET_ID_TO_ID_MAP.get(&basket_id) {
                        Some(id) => id.value().clone(),
                        None => {
                            //eprintln!("Order ID not found for basket: {}", basket_id);
                            return;
                        }
                    };

                    let (symbol_name, symbol_code) = match msg.symbol {
                        None => return,
                        Some(code) => {
                            let symbol_name = match find_base_symbol(&code) {
                                None => return,
                                Some(symbol_name) => symbol_name
                            };
                            (symbol_name, code)
                        }
                    };

                    let reason = msg.text
                        .or(msg.remarks)
                        .unwrap_or_else(|| "Cancelled".to_string());

                    let tag = if let Some(kvp) = PENDING_ORDERS.get(&order_id) {
                        kvp.value().tag.clone()
                    } else {
                        "External Order".to_string()
                    };

                    match notify_type {
                        1 => {

                            if let Some(mut open_order) = PENDING_ORDERS.get_mut(&order_id) {
                                if open_order.state == OrderState::Accepted {
                                    return;
                                }
                                open_order.state = OrderState::Accepted;
                                open_order.symbol = symbol_name.clone();
                            } else {
                                return;
                            }

                            BASKET_ID_TO_ID_MAP.insert(basket_id.clone(), order_id.clone());
                            ID_TO_BASKET_ID_MAP.insert(order_id.clone(), basket_id.clone());

                            let event = OrderUpdateEvent::OrderAccepted {
                                account: account_id.clone(),
                                symbol: symbol_name,
                                order_id: order_id.clone(),
                                tag,
                                time: time.clone(),
                            };
                            let strategy_event = StrategyEvent::OrderEvents(event);
                            match engine.strategy_event_sender.send(strategy_event).await {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("Error sending OrderUpdateEvent::OrderAccepted: {}", e);
                                }
                            }
                        },
                        5 => {
                            if let (Some(fill_price), Some(fill_size), Some(total_unfilled_size)) =
                                (msg.fill_price, msg.fill_size, msg.total_unfilled_size) {
                                let price = match Price::from_f64(fill_price) {
                                    Some(p) => p,
                                    None => return,
                                };
                                let fill_quantity = match Volume::from_f64(fill_size as f64) {
                                    Some(q) => q,
                                    None => return,
                                };

                                let side = match msg.transaction_type {
                                    None => {
                                        eprintln!("NO SIDE ON TRANSACTION");
                                        return;
                                    },
                                    Some(tt) => {
                                        match tt {
                                            1 => OrderSide::Buy,
                                            2 | 3 => OrderSide::Sell,
                                            _ => return
                                        }
                                    }
                                };
                                if total_unfilled_size == 0 {

                                    if let Some((order_id, order)) = PENDING_ORDERS.remove(&order_id) {
                                        COMPLETED_ORDERS.insert(order_id, order);
                                    } else {
                                        return;
                                    }

                                    BASKET_ID_TO_ID_MAP.remove(&basket_id);
                                    ID_TO_BASKET_ID_MAP.remove(&order_id);

                                    if let Some((order_id, mut open_order)) = PENDING_ORDERS.remove(&order_id) {
                                        let quantity = open_order.quantity_open;
                                        open_order.quantity_filled += open_order.quantity_open;
                                        open_order.quantity_open = dec!(0.0);
                                        open_order.time_filled_utc = Some(time.clone());
                                        open_order.state = OrderState::Filled;
                                        COMPLETED_ORDERS.insert(order_id.clone(), open_order);
                                        //println!("{}", order_update_event);
                                        engine.ledger_service.update_or_create_position(&account_id, symbol_name.clone(), quantity, side.clone(), Utc::now(), Decimal::from_f64(fill_price).unwrap(), tag.to_string(), None, order_id.clone()).await;

                                        let event = OrderUpdateEvent::OrderFilled {
                                            side,
                                            account: account_id.clone(),
                                            symbol: symbol_name,
                                            order_id,
                                            price,
                                            quantity: fill_quantity,
                                            tag,
                                            time: time.clone(),
                                        };
                                        let strategy_event = StrategyEvent::OrderEvents(event);
                                        match engine.strategy_event_sender.send(strategy_event).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                eprintln!("Error sending OrderUpdateEvent::OrderFilled: {}", e);
                                            }
                                        }
                                    }

                                } else if total_unfilled_size > 0 {
                                    if let Some((order_id, mut open_order)) = PENDING_ORDERS.remove(&order_id) {
                                        let quantity = open_order.quantity_open;
                                        //println!("{}", order_update_event);
                                        open_order.state = OrderState::PartiallyFilled;
                                        open_order.quantity_filled += quantity;
                                        open_order.quantity_open -= quantity;
                                        open_order.time_filled_utc = Some(time.clone());
                                        COMPLETED_ORDERS.insert(order_id.clone(), open_order);
                                        engine.ledger_service.update_or_create_position(&account_id, symbol_name.clone(),  quantity.clone(), side.clone(), Utc::now(), Decimal::from_f64(fill_price).unwrap(), tag.to_string(), None, order_id.clone()).await;

                                        let event = OrderUpdateEvent::OrderPartiallyFilled {
                                            side,
                                            account: account_id.clone(),
                                            symbol: symbol_name,
                                            order_id: order_id.clone(),
                                            price,
                                            quantity: fill_quantity,
                                            tag,
                                            time: time.clone(),
                                        };
                                        let strategy_event = StrategyEvent::OrderEvents(event);
                                        match engine.strategy_event_sender.send(strategy_event).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                eprintln!("Error sending OrderUpdateEvent::OrderPartiallyFilled: {}", e);
                                            }
                                        }
                                    }
                                }
                            } else {
                                return;
                            }
                        },
                        3 => {
                            if let Some((order_id,mut order)) = PENDING_ORDERS.remove(&order_id) {
                                order.state = OrderState::Cancelled;
                                order.quantity_open = dec!(0);
                                COMPLETED_ORDERS.insert(order_id, order);
                            } else {
                                return;
                            }
                            BASKET_ID_TO_ID_MAP.remove(&basket_id);
                            ID_TO_BASKET_ID_MAP.remove(&order_id);
                            let event = OrderUpdateEvent::OrderCancelled {
                                account: account_id.clone(),
                                order_id: order_id.clone(),
                                symbol: symbol_name,
                                tag,
                                time: time.clone(),
                                reason,
                            };

                            let strategy_event = StrategyEvent::OrderEvents(event);
                            match engine.strategy_event_sender.send(strategy_event).await {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("Error sending OrderUpdateEvent::OrderCancelled: {}", e);
                                }
                            }
                        },
                        6 => {
                            if let Some((order_id,mut order)) = PENDING_ORDERS.remove(&order_id) {
                                order.state = OrderState::Rejected(reason.clone());
                                order.quantity_open = dec!(0);
                                COMPLETED_ORDERS.insert(order_id, order);
                            } else {
                                return;
                            }
                            BASKET_ID_TO_ID_MAP.remove(&basket_id);
                            ID_TO_BASKET_ID_MAP.remove(&order_id);
                            let event = OrderUpdateEvent::OrderRejected {
                                account: account_id.clone(),
                                order_id: order_id.clone(),
                                reason,
                                symbol: symbol_name,
                                tag,
                                time: time.clone(),
                            };
                            let strategy_event = StrategyEvent::OrderEvents(event);
                            match engine.strategy_event_sender.send(strategy_event).await {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("Error sending OrderUpdateEvent::OrderRejected: {}", e);
                                }
                            }
                        },
                       2 => {
                           let update_type = if let Some(mut kvp) = PENDING_ORDERS.get_mut(&order_id) {
                               if let Some((order_id, pending_update)) = PENDING_UPDATES.remove(&order_id) {
                                   match &pending_update {
                                       OrderUpdateType::LimitPrice(price) => kvp.value_mut().limit_price = Some(price.clone()),
                                       OrderUpdateType::TriggerPrice(price) => kvp.value_mut().trigger_price = Some(price.clone()),
                                       OrderUpdateType::Quantity(q) => kvp.value_mut().quantity_open = q.clone(),
                                   }
                                   pending_update
                               } else {
                                   return;
                               }
                           } else {
                               return;
                           };

                           let event = OrderUpdateEvent::OrderUpdated {
                               account: account_id.clone(),
                               order_id: order_id.clone(),
                               update_type,
                               symbol: symbol_name,
                               tag,
                               time: time.clone(),
                               text: "User Request".to_string(),
                           };
                           let strategy_event = StrategyEvent::OrderEvents(event);
                           match engine.strategy_event_sender.send(strategy_event).await {
                               Ok(_) => {}
                               Err(e) => {
                                   eprintln!("Error sending OrderUpdateEvent::OrderUpdated: {}", e);
                               }
                           }
                        },
                        7 | 8 => {
                            PENDING_UPDATES.remove(&order_id);
                            let event =OrderUpdateEvent::OrderUpdateRejected {
                                account: account_id.clone(),
                                order_id: order_id.clone(),
                                reason: msg.status.unwrap_or_default(),
                                time: time.clone(),
                            };

                            let strategy_event = StrategyEvent::OrderEvents(event);
                            match engine.strategy_event_sender.send(strategy_event).await {
                                Ok(_) => {}
                                Err(e) => {
                                    eprintln!("Error sending OrderUpdateEvent::OrderUpdateRejected: {}", e);
                                }
                            }
                        },
                        _ => return,  // Ignore other notification types
                    };
                }
            }
        },
        353 => {
            if let Ok(msg) = BracketUpdates::decode(&message_buf[..]) {
                // Bracket Updates
                // From Server
                println!("Bracket Updates (Template ID: 353) from Server: {:?}", msg);
            }
        },
        354 => {
            if let Ok(msg) = AccountListUpdates::decode(&message_buf[..]) {
                // Account List Updates
                // From Server
                println!("Account List Updates (Template ID: 354) from Server: {:?}", msg);
            }
        },
        355 => {
            if let Ok(msg) = UpdateEasyToBorrowList::decode(&message_buf[..]) {
                // Update Easy To Borrow List
                // From Server
                println!("Update Easy To Borrow List (Template ID: 355) from Server: {:?}", msg);
            }
        },
        3501 => {
            if let Ok(msg) = ResponseModifyOrderReferenceData::decode(&message_buf[..]) {
                // Modify Order Reference Data Response
                // From Server
                println!("Modify Order Reference Data Response (Template ID: 3501) from Server: {:?}", msg);
            }
        },
        3503 => {
            if let Ok(msg) = ResponseOrderSessionConfig::decode(&message_buf[..]) {
                // Order Session Config Response
                // From Server
                println!("Order Session Config Response (Template ID: 3503) from Server: {:?}", msg);
            }
        },
        3505 => {
            if let Ok(msg) = ResponseExitPosition::decode(&message_buf[..]) {
                // Exit Position Response
                // From Server
                println!("Exit Position Response (Template ID: 3505) from Server: {:?}", msg);
            }
        },
        3507 => {
            if let Ok(msg) = ResponseReplayExecutions::decode(&message_buf[..]) {
                // Replay Executions Response
                // From Server
                println!("Replay Executions Response (Template ID: 3507) from Server: {:?}", msg);
            }
        },
        3509 => {
            if let Ok(msg) = ResponseAccountRmsUpdates::decode(&message_buf[..]) {
                // Account RMS Updates Response
                // From Server
                println!("Account RMS Updates Response (Template ID: 3509) from Server: {:?}", msg);
            }
        },
        356 => {
            if let Ok(msg) = AccountRmsUpdates::decode(&message_buf[..]) {
                // Account RMS Updates
                // From Server
                println!("Account RMS Updates (Template ID: 356) from Server: {:?}", msg);
            }
        },

        _ => println!("No match for template_id: {}", template_id)
    }
}

pub struct CommonRithmicOrderDetails {
    pub symbol_code: String,
    pub exchange: FuturesExchange,
    pub transaction_type: TransactionType,
    pub route: String,
    pub quantity: i32
}

*/