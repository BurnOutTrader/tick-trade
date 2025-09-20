pub mod handle_repo_plant;
pub mod tick_plant;
pub mod history_plant;
pub mod order_plant;
pub mod pnl_plant;
pub mod common_requests;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use smallvec::SmallVec;
use tokio::sync::oneshot;
use crate::websocket::rithmic_proto_objects::rti::{RequestAuxilliaryReferenceData, RequestDepthByOrderSnapshot, RequestFrontMonthContract, RequestGetInstrumentByUnderlying, RequestGetVolumeAtPrice, RequestGiveTickSizeTypeTable, RequestProductCodes, RequestReferenceData, RequestRithmicSystemGatewayInfo, RequestRithmicSystemInfo, RequestSearchSymbols, RequestTickBarReplay, RequestTimeBarReplay};

#[allow(dead_code)]
pub(crate) fn create_datetime(ssboe: i64, usecs: i64) -> DateTime<Utc> {
    let nanosecs = usecs * 1000; // Convert microseconds to nanoseconds
    DateTime::<Utc>::from_timestamp(ssboe, nanosecs as u32).unwrap()
}

// Inline up to 8 chunks, then spill
pub type ChunkVec = SmallVec<Bytes, 8>;

pub struct PendingEntry {
    pub(crate) chunks: ChunkVec,
    pub(crate) tx: oneshot::Sender<ChunkVec>,
}


/// For protobuf requests that have `#[prost(string, repeated, tag="132760")] pub user_msg: Vec<String>`
pub trait HasUserMsg {
    fn user_msg_mut(&mut self) -> &mut Vec<String>;
}

macro_rules! impl_has_user_msg {
    ($($t:ty),* $(,)?) => {
        $(impl HasUserMsg for $t {
            #[inline]
            fn user_msg_mut(&mut self) -> &mut Vec<String> { &mut self.user_msg }
        })*
    };
}

impl_has_user_msg!(
    RequestReferenceData,
    RequestRithmicSystemInfo,
    RequestRithmicSystemGatewayInfo,
    RequestGetInstrumentByUnderlying,
    RequestGiveTickSizeTypeTable,
    RequestSearchSymbols,
    RequestProductCodes,
    RequestFrontMonthContract,
    RequestDepthByOrderSnapshot,
    RequestGetVolumeAtPrice,
    RequestAuxilliaryReferenceData,
    RequestTimeBarReplay,
    RequestTickBarReplay

    // ... any other request types that actually have user_msg
);



