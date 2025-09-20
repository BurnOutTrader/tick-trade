use crate::websocket::rithmic_proto_objects::rti::{request_market_data_update, RequestAuxilliaryReferenceData, RequestDepthByOrderSnapshot, RequestDepthByOrderUpdates, RequestFrontMonthContract, RequestGetVolumeAtPrice, RequestProductCodes};
use crate::websocket::client::RithmicApiClient;
use crate::websocket::errors::RithmicApiError;
use crate::websocket::rithmic_proto_objects::rti::{RequestGetInstrumentByUnderlying, RequestGiveTickSizeTypeTable, RequestMarketDataUpdate, RequestMarketDataUpdateByUnderlying, RequestSearchSymbols};
use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;
use crate::websocket::rithmic_proto_objects::rti::request_search_symbols::{InstrumentType, Pattern};
use crate::websocket::rithmic_proto_objects::rti;
use crate::websocket::rithmic_proto_objects::rti::request_market_data_update::UpdateBits;
use std::sync::Arc;
use bytes::Bytes;
use smallvec::SmallVec;
use crate::websocket::models::{ActiveSub, SubType};

const PLANT: SysInfraType = SysInfraType::TickerPlant;

#[allow(dead_code)]
impl RithmicApiClient {

    /// request: 1 subscribe, 2 unsubscribe.
    /// update_bits: subscribe to ticks, order books, quotes etc
    /// for order book need to use instrument, other requests will automatically use front month.
    pub async fn request_market_data_update(
        self: &Arc<Self>,
        symbol: String,
        exchange: String,
        request: i32,
        update_bits: request_market_data_update::UpdateBits
    ) -> Result<(), RithmicApiError> {
        const TID: i32 = 100;
        let req = RequestMarketDataUpdate {
            template_id: TID,
            user_msg: vec![],
            symbol: Some(symbol.clone()),
            exchange: Some(exchange.clone()),
            request: Some(request),
            update_bits: Some(update_bits as u32),
        };

        let t = match update_bits {
            UpdateBits::LastTrade => SubType::LastTrade,
            UpdateBits::Bbo => SubType::Bbo,
            UpdateBits::OrderBook => SubType::OrderBook,
            _ => return Err(RithmicApiError::MappingError("Unacceptable update bits for market data update".to_string()))
        };
        let sub = ActiveSub::new(symbol, exchange, None, t);
        if request == 1 {
            let mut lock = self.active_subscriptions.write().await;
            lock.entry(PLANT).or_default().push(sub);
        } else if request == 2 {
            let mut lock = self.active_subscriptions.write().await;
            if let Some(v) = lock.get_mut(&PLANT) {
                // remove all matching entries
                v.retain(|x| x != &sub);
            }
        }
        self.send_message(PLANT, req).await
    }

    pub async fn request_get_instrument_by_underlying(
        self: &Arc<Self>,
        underlying_symbol: String,
        exchange: String,
        expiration_date: Option<String>
    ) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const TID: i32 = 102;
        let req = RequestGetInstrumentByUnderlying {
            template_id: TID,
            user_msg: vec![],
            exchange: Some(exchange),
            underlying_symbol: Some(underlying_symbol),
            expiration_date,
        };
        self.send_with_reply(PLANT, req).await
    }

    pub async fn request_market_data_update_by_underlying(
        self: &Arc<Self>,
        underlying_symbol: String,
        exchange: String,
        expiration_date: Option<String>,
        request: rti::request_market_data_update_by_underlying::Request,
        update_bits: rti::request_market_data_update_by_underlying::UpdateBits
    ) -> Result<(), RithmicApiError> {
        const TID: i32 = 105;
        let req = RequestMarketDataUpdateByUnderlying {
            template_id: TID,
            user_msg: vec![],
            exchange: Some(exchange),
            expiration_date,
            request: Some(request as i32),
            underlying_symbol: Some(underlying_symbol),
            update_bits: Some(update_bits as u32),
        };
        self.send_message(PLANT, req).await
    }

    pub async fn request_give_tick_size_type_table(
        self: &Arc<Self>,
        tick_size_type: String,
    ) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const TID: i32 = 107;
        let req = RequestGiveTickSizeTypeTable {
            template_id: TID,
            user_msg: vec![],
            tick_size_type: Some(tick_size_type),
        };
        self.send_with_reply(PLANT, req).await
    }

    pub async fn request_search_symbols(
        self: &Arc<Self>,
        search_text: String,
        exchange: Option<String>,
        product_code: Option<String>,
        instrument_type: InstrumentType,
        pattern: Pattern,
    ) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const TID: i32 = 109;
        let req = RequestSearchSymbols {
            template_id: TID,
            user_msg: vec![],
            search_text: Some(search_text),
            exchange,
            product_code,
            instrument_type: Some(instrument_type.into()),
            pattern: Some(pattern as i32),
        };
        self.send_with_reply(PLANT, req).await
    }

    pub async fn request_product_codes (
        self: &Arc<Self>,
        exchange: Option<String>,
        give_toi_products_only: bool,
    ) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const TID: i32 = 111;
        let req = RequestProductCodes {
            template_id: TID,
            user_msg: vec![],
            exchange,
            give_toi_products_only: Some(give_toi_products_only),
        };
        self.send_with_reply(PLANT, req).await
    }

    pub async fn request_front_month_contract (
        self: &Arc<Self>,
        symbol: String,
        exchange: Option<String>,
        need_updates: bool
    ) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const TID: i32 = 113;
        let req = RequestFrontMonthContract {
            template_id: TID,
            user_msg: vec![],
            symbol: Some(symbol),
            exchange,
            need_updates: Some(need_updates),
        };
        self.send_with_reply(PLANT, req).await
    }

    // get a 1 time snapshot of depth
    pub async fn request_depth_by_order_snapshot (
        self: &Arc<Self>,
        symbol: String,
        exchange: String,
        depth_price: f64
    ) -> Result<(), RithmicApiError> {
        const TID: i32 = 115;
        let req = RequestDepthByOrderSnapshot {
            template_id: TID,
            user_msg: vec![],
            symbol: Some(symbol),
            exchange: Some(exchange),
            depth_price: Some(depth_price),
        };
        self.send_message(PLANT, req).await
    }

    // Stream regular depth updates
    pub async fn request_depth_by_order_updates (
        self: &Arc<Self>,
        symbol: String,
        exchange: String,
        depth_price: f64
    ) -> Result<(), RithmicApiError> {
        const TID: i32 = 117;
        let req = RequestDepthByOrderUpdates {
            template_id: TID,
            user_msg: vec![],
            request: Some(1),
            symbol: Some(symbol),
            exchange: Some(exchange),
            depth_price: Some(depth_price),
        };
        self.send_message(PLANT, req).await
    }


    /// Basically returns a volume profile of the day
    ///
    /// trade_price: [24048.75, 24048.5, 24048.25, 24048.0]
    ///
    ///  volume_at_price: [48, 16, 10, 40, 41]
    pub async fn request_volume_at_price (
        self: &Arc<Self>,
        symbol: String,
        exchange: Option<String>,
    ) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const TID: i32 = 119;
        let req = RequestGetVolumeAtPrice {
            template_id: TID,
            user_msg: vec![],
            symbol: Some(symbol),
            exchange,
        };
        self.send_with_reply(PLANT, req).await
    }

    pub async fn request_auxiliary_reference_data(
        self: &Arc<Self>,
        symbol: String,
        exchange: Option<String>,
    ) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const TID: i32 = 121;
        let req = RequestAuxilliaryReferenceData {
            template_id: TID,
            user_msg: vec![],
            symbol: Some(symbol),
            exchange,
        };
        self.send_with_reply(PLANT, req).await
    }
}