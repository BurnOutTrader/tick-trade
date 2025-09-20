use std::sync::Arc;
use bytes::Bytes;
use crate::websocket::client::RithmicApiClient;
use crate::websocket::errors::RithmicApiError;

#[allow(dead_code)]
impl RithmicApiClient {
    pub(crate) fn request_pnl_position_updates(self: Arc<Self>) -> Result<Bytes, RithmicApiError> {
        todo!()
    }

    pub(crate) fn request_pnl_position_snapshot(self: Arc<Self>) -> Result<Bytes, RithmicApiError> {
        todo!()
    }
}