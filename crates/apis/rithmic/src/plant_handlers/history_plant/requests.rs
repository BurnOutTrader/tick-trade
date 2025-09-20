use std::sync::Arc;
use bytes::Bytes;
use standard_lib::market_data::base_data::Resolution;
use crate::websocket::api_base::{ActiveSub, RithmicApiClient, SubType};
use crate::websocket::errors::RithmicApiError;
use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;
use crate::websocket::rithmic_proto_objects::rti::RequestTimeBarUpdate;
const PLANT: SysInfraType = SysInfraType::HistoryPlant;
#[allow(dead_code)]
impl RithmicApiClient {
    pub(crate) async fn request_time_bar_update(self: &Arc<Self>, symbol: String, exchange: String, resolution: Resolution, request: i32) -> Result<(), RithmicApiError> {
        let (bar_type, num) = match resolution {
            Resolution::Seconds(q) => (crate::websocket::rithmic_proto_objects::rti::request_time_bar_update::BarType::SecondBar, Some(q as i32)),
            Resolution::Minutes(q) => (crate::websocket::rithmic_proto_objects::rti::request_time_bar_update::BarType::SecondBar, Some(q as i32)),
            Resolution::Hours(q) => (crate::websocket::rithmic_proto_objects::rti::request_time_bar_update::BarType::SecondBar, Some(q as i32)),
            Resolution::TickBars(_) => return Err(RithmicApiError::MappingError("TickBars not supported for TimeBarUpdate".to_string())),
            Resolution::Daily => (crate::websocket::rithmic_proto_objects::rti::request_time_bar_update::BarType::DailyBar, None),
            Resolution::Weekly => (crate::websocket::rithmic_proto_objects::rti::request_time_bar_update::BarType::WeeklyBar, None),
            _ => return Err(RithmicApiError::MappingError("Unacceptable resolution for time bar update".to_string())),
        };
        const TID: i32 = 200;
        let req = RequestTimeBarUpdate {
            template_id: TID,
            user_msg: vec![],
            symbol: Some(symbol.clone()),
            exchange: Some(exchange.clone()),
            request: Some(request),
            bar_type: Some(bar_type as i32),
            bar_type_period: num,
        };
        let sub = ActiveSub::new(symbol, exchange, Some(resolution), SubType::TimeBar);
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

    // these arent needed as its handled in websocket base
    /*pub(crate) fn request_time_bar_replay(self: Arc<Self>) -> Result<Bytes, RithmicApiError> {
        todo!()
    }

    pub(crate) fn request_tick_bar_replay(self: Arc<Self>) -> Result<Bytes, RithmicApiError> {
        todo!()
    }

     */

    pub(crate) fn request_tick_bar_update(self: Arc<Self>) -> Result<Bytes, RithmicApiError> {
        todo!()
    }

    pub(crate) fn request_volume_profile_minute_bars(self: Arc<Self>) -> Result<Bytes, RithmicApiError> {
        todo!()
    }

    pub(crate) fn request_resume_bars(self: Arc<Self>) -> Result<Bytes, RithmicApiError> {
        todo!()
    }
}