#![allow(dead_code)]

use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use lazy_static::lazy_static;
use strum_macros::Display;


#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Hash, PartialOrd, Ord, Display)]
pub enum RithmicServer {
    Chicago,
    Sydney,
    SaoPaolo,
    Colo75,
    Frankfurt,
    HongKong,
    Ireland,
    Mumbai,
    Seoul,
    CapeTown,
    Tokyo,
    Singapore,
    Test
}

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Hash, PartialOrd, Ord, Display, Copy)]
pub enum RithmicSystem {
    #[strum(serialize = "Rithmic 04 Colo")]
    Rithmic04Colo,
    #[strum(serialize = "Rithmic 01")]
    Rithmic01,
    #[strum(serialize = "Rithmic Paper Trading")]
    RithmicPaperTrading,
    #[strum(serialize = "TopstepTrader")]
    TopstepTrader,
    #[strum(serialize = "SpeedUp")]
    SpeedUp,
    #[strum(serialize = "TradeFundrr")]
    TradeFundrr,
    #[strum(serialize = "UProfitTrader")]
    UProfitTrader,
    #[strum(serialize = "Apex")]
    Apex,
    #[strum(serialize = "MES Capital")]
    MESCapital,
    #[strum(serialize = "The Trading Pit")]
    TheTradingPit,
    #[strum(serialize = "Funded Futures Network")]
    FundedFuturesNetwork,
    #[strum(serialize = "Bulenox")]
    Bulenox,
    #[strum(serialize = "PropShopTrader")]
    PropShopTrader,
    #[strum(serialize = "4PropTrader")]
    FourPropTrader,
    #[strum(serialize = "FastTrackTrading")]
    FastTrackTrading,
    #[strum(serialize = "Test")]
    Test
}

lazy_static! {
    pub(crate) static ref RITHMIC_SERVERS: BTreeMap<RithmicServer, String> = {
        BTreeMap::from([
            (RithmicServer::Chicago, "wss://rprotocol.rithmic.com:443".to_string()),
            (RithmicServer::Sydney, "wss://rprotocol-au.rithmic.com:443".to_string()),
            (RithmicServer::SaoPaolo, "wss://rprotocol-br.rithmic.com:443".to_string()),
            (RithmicServer::Colo75, "wss://protocol-colo75.rithmic.com:443".to_string()),
            (RithmicServer::Frankfurt, "wss://rprotocol-de.rithmic.com:443".to_string()),
            (RithmicServer::HongKong, "wss://rprotocol-hk.rithmic.com:443".to_string()),
            (RithmicServer::Ireland, "wss://rprotocol-ie.rithmic.com:443".to_string()),
            (RithmicServer::Mumbai, "wss://rprotocol-in.rithmic.com:443".to_string()),
            (RithmicServer::Seoul, "wss://rprotocol-kr.rithmic.com:443".to_string()),
            (RithmicServer::CapeTown, "wss://rprotocol-za.rithmic.com:443".to_string()),
            (RithmicServer::Tokyo, "wss://rprotocol-jp.rithmic.com:443".to_string()),
            (RithmicServer::Singapore, "wss://rprotocol-sg.rithmic.com:443".to_string()),
            (RithmicServer::Test, "wss://rituz00100.rithmic.com:443".to_string()),
        ])
    };
}

pub struct RithmicCredentials {
    pub(crate) user: String,
    pub(crate) server_name: RithmicServer,
    pub(crate) system_name: RithmicSystem,
    pub(crate) app_name: String,
    pub(crate) app_version: String,
    pub(crate) password: String,
    pub(crate) fcm_id: Option<String>,
    pub(crate) ib_id: Option<String>,
    pub(crate) user_type: Option<i32>
}

impl RithmicCredentials {
    pub fn new(
        user: String,
        server_name: RithmicServer,
        system_name: RithmicSystem,
        app_name: String,
        app_version: String,
        password: String,
        fcm_id: Option<String>,
        ib_id: Option<String>,
        user_type: Option<i32>
    ) -> Self {
        Self {
            user,
            server_name,
            system_name,
            app_name,
            app_version,
            password,
            fcm_id,
            ib_id,
            user_type
        }
    }
}