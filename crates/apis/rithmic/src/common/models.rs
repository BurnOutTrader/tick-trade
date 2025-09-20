use serde::{Deserialize, Serialize};
use strum_macros::Display;
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Hash, PartialOrd, Ord, Display, Zeroize)]
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

#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug, Hash, PartialOrd, Ord, Display, Copy, Zeroize)]
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

#[derive(Deserialize, ZeroizeOnDrop, Zeroize)]
pub struct RithmicCredentials {
    #[zeroize(skip)]
    pub(crate) user: String,
    pub(crate) server_name: RithmicServer,
    pub(crate) system_name: RithmicSystem,
    #[zeroize(skip)]
    pub(crate) app_name: String,
    pub(crate) app_version: String,
    #[zeroize(skip)]
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