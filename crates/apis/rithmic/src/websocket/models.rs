use bytes::Bytes;
use std::sync::{atomic::{AtomicBool, Ordering}, Arc};
use ahash::AHashMap;
use concurrent_queue::ConcurrentQueue;
use lazy_static::lazy_static;
use tokio::sync::Notify;
use strum_macros::Display;
use smallvec::SmallVec;
use anyhow::anyhow;
use standard_lib::engine_core::data_events::{MarketDataRequest, SubscriptionHandle};
use standard_lib::market_data::base_data::Resolution;
use crate::common::models::RithmicServer;
use crate::websocket::client;
use crate::websocket::client::RithmicApiClient;
use crate::websocket::rithmic_proto_objects::rti::request_market_data_update::UpdateBits;

pub(crate) struct Outgoing {
    pub q: Arc<ConcurrentQueue<Bytes>>, // lock-free MPMC
    pub bell: Arc<Notify>,              // doorbell
    pub has_items: AtomicBool,          // edge detector for bell
}

impl Outgoing {
    pub(crate)  fn new() -> Self {
        Self {
            q: Arc::new(ConcurrentQueue::unbounded()),
            bell: Arc::new(Notify::new()),
            has_items: AtomicBool::new(false),
        }
    }
    #[inline]
    pub(crate)  fn push(&self, b: Bytes) {
        let was_empty = self.q.is_empty();
        let _ = self.q.push(b);
        if was_empty && !self.has_items.swap(true, Ordering::AcqRel) {
            self.bell.notify_one();
        }
    }
}

lazy_static! {
    pub(crate) static ref RITHMIC_SERVERS: AHashMap<RithmicServer, String> = {
        [
            (RithmicServer::Chicago,    "wss://rprotocol.rithmic.com:443".to_string()),
            (RithmicServer::Sydney,     "wss://rprotocol-au.rithmic.com:443".to_string()),
            (RithmicServer::SaoPaolo,   "wss://rprotocol-br.rithmic.com:443".to_string()),
            (RithmicServer::Colo75,     "wss://protocol-colo75.rithmic.com:443".to_string()),
            (RithmicServer::Frankfurt,  "wss://rprotocol-de.rithmic.com:443".to_string()),
            (RithmicServer::HongKong,   "wss://rprotocol-hk.rithmic.com:443".to_string()),
            (RithmicServer::Ireland,    "wss://rprotocol-ie.rithmic.com:443".to_string()),
            (RithmicServer::Mumbai,     "wss://rprotocol-in.rithmic.com:443".to_string()),
            (RithmicServer::Seoul,      "wss://rprotocol-kr.rithmic.com:443".to_string()),
            (RithmicServer::CapeTown,   "wss://rprotocol-za.rithmic.com:443".to_string()),
            (RithmicServer::Tokyo,      "wss://rprotocol-jp.rithmic.com:443".to_string()),
            (RithmicServer::Singapore,  "wss://rprotocol-sg.rithmic.com:443".to_string()),
            (RithmicServer::Test,       "wss://rituz00100.rithmic.com:443".to_string()),
        ]
        .into()
    };
}

/// RAII subscription; on drop or explicit `unsubscribe()` sends Unsubscribe requests once.
pub(crate)  struct LiveSub {
    pub(crate) client: Arc<RithmicApiClient>,
    pub(crate) req: MarketDataRequest,
    pub(crate) id: u64,
    pub(crate) closed: AtomicBool,
}

impl SubscriptionHandle for LiveSub {
    fn unsubscribe(&self) -> anyhow::Result<(), anyhow::Error> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(anyhow!("Already unsubscribed"));
        }
        let client = self.client.clone();
        let req = self.req.clone();
        // Fire-and-forget; if you want backpressure, return a JoinHandle or make this async.
        tokio::spawn(async move {
            if let Err(e) = client::do_live_req(&client, req, 2).await {
                tracing::warn!("unsubscribe failed: {e}");
            }
        });
        Ok(())
    }
    fn id(&self) -> u64 { self.id }
}

impl Drop for LiveSub {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::SeqCst) { return; }
        let client = self.client.clone();
        let req = self.req.clone();
        tokio::spawn(async move {
            if let Err(e) = client::do_live_req(&client, req, 2).await {
                tracing::warn!("drop-unsubscribe failed: {e}");
            }
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Display, Eq)]
pub enum SubType {
    Bbo,
    OrderBook,
    LastTrade,
    TimeBar,
    #[allow(dead_code)]
    TickBar
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActiveSub {
    pub(crate) symbol: String,
    pub(crate) exchange: String,
    pub(crate) resolution: Option<Resolution>,
    pub(crate) kind: SubType,
}

impl ActiveSub {
    pub(crate) fn new(symbol: String, exchange: String, resolution: Option<Resolution>, kind: SubType) -> Self {
        Self {
            symbol,
            exchange,
            resolution,
            kind,
        }
    }
}

pub fn bits_for(kind: &SubType) -> Option<UpdateBits> {
    match kind {
        SubType::Bbo        => Some(UpdateBits::Bbo),
        SubType::OrderBook  => Some(UpdateBits::OrderBook),
        SubType::LastTrade  => Some(UpdateBits::LastTrade),
        _ => None,
    }
}

/// Buffer for early/partial responses keyed by (plant, cid).
/// Uses `SmallVec<Bytes, 8>` so the common 1–8 segment replies avoid heap allocs.
/// We store `Bytes` by value and never clone in the hot path; they are ref-counted slices.
#[derive(Default)]
pub(crate) struct Staged {
    pub(crate) chunks: SmallVec<Bytes, 8>,
    pub(crate) final_seen: bool,
}