use std::sync::Arc;
use bytes::Bytes;
use smallvec::SmallVec;
use crate::client::api_base::RithmicApiClient;
use crate::client::errors::RithmicApiError;
use crate::client::rithmic_proto_objects::rti::request_login::SysInfraType;
use crate::client::rithmic_proto_objects::rti::{RequestReferenceData, RequestRithmicSystemGatewayInfo, RequestRithmicSystemInfo};

#[allow(dead_code)]
impl RithmicApiClient {
    ///  returns
    /// Reference Data Response (Template ID: 15) from Server: ResponseReferenceData { template_id: 15, user_msg: ["cid=1"], rp_code: ["0"], presence_bits: Some(425583),
    /// clear_bits: Some(16384), symbol: Some("MNQ"), exchange: Some("CME"), exchange_symbol: Some("MNQU5"), symbol_name: Some("Front Month for MNQ - MNQU5.CME"),
    /// trading_symbol: Some("MNQU5"), trading_exchange: Some("CME"), product_code: Some("MNQ"), instrument_type: Some("Future"), underlying_symbol: None,
    /// expiration_date: Some("20250919"), currency: Some("USD"), put_call_indicator: None, tick_size_type: None, price_display_format: None, is_tradable: None,
    /// is_underlying_for_binary_contrats: None, strike_price: None, ftoq_price: Some(0.01), qtof_price: Some(100.0), min_qprice_change: Some(0.25), min_fprice_change: Some(25.0), single_point_value: Some(2.0) }
    ///
    /// error: RithmicApiError
    pub async fn request_reference_data(self: &Arc<Self>, plant: SysInfraType, symbol: String, exchange: String) -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        let req = RequestReferenceData {
            template_id: 14,
            user_msg: vec![],
            symbol: Some(symbol.clone()),
            exchange: Some(exchange.clone()),
        };
        self.send_with_reply(plant, req).await
    }

    ///ResponseRithmicSystemInfo { template_id: 17, user_msg: ["cid=1"], rp_code: ["0"], system_name: ["Rithmic 01", "Rithmic 04 Colo",
    /// "Rithmic Paper Trading", "TopstepTrader", "Apex", "TradeFundrr", "MES Capital", "TheTradingPit", "FundedFuturesNetwork",
    /// "PropShopTrader", "4PropTrader", "DayTraders.com", "10XFutures", "LucidTrading", "ThriveTrading", "LegendsTrading", "Earn2Trade",
    /// "P.T-t", "YPF-t", "Profit.Trade"],
    /// has_aggregated_quotes: [false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false] }
    pub async fn request_system_info(self: &Arc<Self>, plant: SysInfraType)  -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        const REQ: RequestRithmicSystemInfo = RequestRithmicSystemInfo {
            template_id: 16,
            user_msg: vec![],
        };
        self.send_with_reply(plant, REQ).await
    }

    ///ResponseRithmicSystemGatewayInfo { template_id: 21, user_msg: ["cid=1"], rp_code: ["0"], system_name: Some("Apex"),
    /// gateway_name: ["Chicago Area", "Seoul", "Mumbai", "Hong Kong", "Sao Paolo", "Singapore", "Sydney", "Europe", "Tokyo", "Frankfurt", "Cape Town", "NYC Area"],
    /// gateway_uri: ["wss://rprotocol.rithmic.com:443", "wss://rprotocol-kr.rithmic.com:443", "wss://rprotocol-in.rithmic.com:443",
    /// "wss://rprotocol-hk.rithmic.com:443", "wss://rprotocol-br.rithmic.com:443", "wss://rprotocol-sg.rithmic.com:443",
    /// "wss://rprotocol-au.rithmic.com:443", "wss://rprotocol-ie.rithmic.com:443", "wss://rprotocol-jp.rithmic.com:443",
    /// "wss://rprotocol-de.rithmic.com:443", "wss://rprotocol-za.rithmic.com:443", "wss://rprotocol-nyc.rithmic.com:443"] }
    pub async fn request_system_gateway_info(self: &Arc<Self>, plant: SysInfraType, system_name: String)  -> Result<SmallVec<Bytes, 8>, RithmicApiError> {
        let req = RequestRithmicSystemGatewayInfo {
            template_id: 20,
            user_msg: vec![],
            system_name: Some(system_name),
        };
        self.send_with_reply(plant, req).await
    }
}
