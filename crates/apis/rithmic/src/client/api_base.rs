use std::collections::HashMap;
use crate::client::helpers::{is_weekend_or_off_hours, next_chicago_market_open};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, RwLock};
use dashmap::{DashMap, Entry};
use prost::Message as ProstMessage;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio::time::{sleep, Duration, MissedTickBehavior};
use futures_util::{stream, SinkExt, StreamExt};
use futures_util::stream::{SplitSink, SplitStream};
use tokio::time;
#[allow(unused_imports)]
use super::rithmic_proto_objects::rti::{RequestHeartbeat, RequestLogin, RequestLogout, RequestRithmicSystemInfo, ResponseLogin, ResponseRithmicSystemInfo};
use crate::client::errors::RithmicApiError;
use crate::client::rithmic_proto_objects::rti::request_login::SysInfraType;
use crate::client::server_models::{RithmicCredentials, RITHMIC_SERVERS};
use tracing::{error, info, warn};
use crate::client::outgoing::Outgoing;
use crate::plant_handlers::handle_repo_plant::match_repo_plant_id;
use crate::plant_handlers::tick_plant::handle_tick_plant::match_ticker_plant_id;
use bytes::Bytes;
use standard_lib::market_data::base_data::Resolution;
use standard_lib::engine_core::data_events::SubscriptionHandle;
use crate::client::rithmic_proto_objects::rti::request_market_data_update::UpdateBits;
use crate::plant_handlers::{ChunkVec, HasUserMsg, PendingEntry};
use crate::plant_handlers::history_plant::handle_history_plant::match_history_plant_id;
use std::sync::atomic::AtomicBool;
use anyhow::{anyhow, Result};
use chrono_tz::Tz;
use smallvec::SmallVec;
use strum_macros::Display;
use standard_lib::engine_core::api_traits::{HistoricalDataProvider, MarketDataProvider};
use standard_lib::engine_core::event_hub::EventHub;
use standard_lib::engine_core::provider_resolver::{ProviderResolver, SecurityResolver};
use standard_lib::engine_core::public_classes::{FeedKind, MarketDataRequest};
use standard_lib::securities::symbols::Exchange;
use crate::client::rithmic_proto_objects::rti::{RequestTickBarReplay, RequestTimeBarReplay};
use crate::client::rithmic_proto_objects::rti::request_time_bar_replay::{Direction, TimeOrder};



const TIME_OUT_MS: u64 = 30000;
pub const TEMPLATE_VERSION: &str = "5.29";
type RpcKey = (SysInfraType, u64);           // (plant, correlation id)

///Server uses Big Endian format for binary data
pub struct RithmicApiClient {
    pub(crate) name: String,
    credentials: RithmicCredentials,
    pub(crate) fcm_id: RwLock<Option<String>>,
    pub(crate) ib_id: RwLock<Option<String>>,
    pub(crate) out_queues: DashMap<SysInfraType, Arc<Outgoing>>,
    pub(crate) reconnection_locks: DashMap<SysInfraType, ()>,
    pub(crate) last_heartbeat_time: DashMap<SysInfraType, Instant>,

    // Call back management
    pub(crate) pending: DashMap<RpcKey, PendingEntry>,      // in-flight requests
    // Chunks that arrived before the request was registered (race-safe buffer)
    pub(crate) rendezvous: DashMap<RpcKey, Staged>,
    pub(crate) cid_seq: DashMap<SysInfraType, Arc<AtomicU64>>, // per-plant counter
    pub(crate) hub: Arc<EventHub>,

    pub(crate) live_ready: RwLock<bool>,

    pub(crate) active_subscriptions: RwLock<HashMap<SysInfraType, Vec<ActiveSub>>>,
}

impl RithmicApiClient {
    fn plant_path(plant: SysInfraType) -> &'static str {
        match plant {
            SysInfraType::TickerPlant => "/ticker",
            SysInfraType::OrderPlant => "/order",
            SysInfraType::HistoryPlant => "/history",
            SysInfraType::PnlPlant => "/pnl",
            SysInfraType::RepositoryPlant => "/repo",
        }
    }

    fn build_ws_url(domain: &str, plant: SysInfraType) -> String {
        let path = Self::plant_path(plant);
        if domain.ends_with('/') {
            format!("{}{}", domain.trim_end_matches('/'), path)
        } else {
            format!("{}{}", domain, path)
        }
    }

    pub fn new(
        name: Option<String>,
        credentials: RithmicCredentials,
        hub: Arc<EventHub>,
    ) -> Result<Arc<Self>, RithmicApiError> {
        let name = name.unwrap_or_else(|| "RITHMIC".to_string());
        Ok(Arc::new(Self {
            name,
            credentials,
            fcm_id: RwLock::new(None),
            ib_id: RwLock::new(None),
            reconnection_locks: DashMap::with_capacity(5),
            last_heartbeat_time: DashMap::with_capacity(5),
            pending: Default::default(),
            // Initialize rendezvous cache for out-of-order/pre-registered chunks
            rendezvous: Default::default(),
            out_queues: DashMap::with_capacity(5),
            cid_seq: Default::default(),
            hub,
            live_ready: RwLock::new(false),
            active_subscriptions: Default::default(),
        }))
    }

    /// only used to register and login before splitting the stream.
    async fn send_single_protobuf_message<T: ProstMessage>(
        stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, message: &T
    ) -> Result<(), RithmicApiError> {
        let mut buf = Vec::new();

        match message.encode(&mut buf) {
            Ok(_) => {}
            Err(e) => return Err(RithmicApiError::EngineErrorDebug(format!("Failed to encode RithmicMessage: {}", e)))
        }

        let length = buf.len() as u32;
        let mut prefixed_msg = length.to_be_bytes().to_vec();
        prefixed_msg.extend(buf);
        stream.send(Message::Binary(prefixed_msg.into())).await.map_err(|e| RithmicApiError::WebSocket(e))
    }

    /// Used to receive system and login response before splitting the stream.
    async fn read_single_protobuf_message<T: ProstMessage + Default>(
        stream: &mut WebSocketStream<MaybeTlsStream<TcpStream>>
    ) -> Result<T, RithmicApiError> {
        while let Some(msg) = stream.next().await { //todo change from while to if and test
            let msg = match msg {
                Ok(msg) => msg,
                Err(e) => return Err(RithmicApiError::EngineErrorDebug(format!("Failed to read RithmicMessage: {}", e)))
            };
            if let Message::Binary(data) = msg {
                //println!("Received binary data: {:?}", data);

                // Create a cursor for reading the data
                let mut cursor = Cursor::new(data);

                // Read the 4-byte length header
                let mut length_buf = [0u8; 4];
                let _ = tokio::io::AsyncReadExt::read_exact(&mut cursor, &mut length_buf).await.map_err(RithmicApiError::Io);
                let length = u32::from_be_bytes(length_buf) as usize;

                // Read the Protobuf message
                let mut message_buf = vec![0u8; length];
                tokio::io::AsyncReadExt::read_exact(&mut cursor, &mut message_buf).await.map_err(RithmicApiError::Io)?;

                // Decode the Protobuf message
                return match T::decode(&message_buf[..]) {
                    Ok(decoded_msg) => Ok(decoded_msg),
                    Err(e) => Err(RithmicApiError::ProtobufDecode(e)), // Use the ProtobufDecode variant
                }
            }
        }
        Err(RithmicApiError::EngineErrorDebug("No valid message received".to_string()))
    }

    /// Connect to the desired plant and sign in with your credentials.
    pub async fn connect_and_login (
        self: &Arc<Self>,
        plant: SysInfraType,
    ) -> Result<(), RithmicApiError> {
        self.cid_seq.entry(plant)
            .or_insert_with(|| Arc::new(AtomicU64::new(1)));

        let domain = RITHMIC_SERVERS.get(&self.credentials.server_name).unwrap();

        // Build plant-specific URL and attempt with simple retries
        let url = Self::build_ws_url(domain, plant);
        info!("Connecting to URL: {} for plant: {:?}", url, plant);
        let mut last_err: Option<String> = None;
        let mut attempt = 0;
        let mut stream_opt: Option<WebSocketStream<MaybeTlsStream<TcpStream>>> = None;
        loop {
            attempt += 1;
            match connect_async(&url).await {
                Ok((stream, _response)) => {
                    stream_opt = Some(stream);
                    break;
                }
                Err(e) => {
                    warn!("Failed to connect to rithmic, for login 1 (plant {:?}) attempt {}: {}", plant, attempt, e);
                    last_err = Some(e.to_string());
                    if attempt >= 3 { break; }
                    sleep(Duration::from_millis(250)).await;
                }
            }
        }
        let mut stream = match stream_opt {
            Some(s) => s,
            None => {
                let err = last_err.unwrap_or_else(|| "unknown error".to_string());
                return Err(RithmicApiError::EngineErrorDebug(format!(
                    "Failed to connect to rithmic, for login 2 (plant {:?}) after {} attempts to {}: {}",
                    plant, attempt, url, err
                )));
            }
        };

        // After handshake, we can send confidential data
        // Login Request 10 From Client
        const TID: i32 = 10;
        let login_request = RequestLogin {
            template_id: TID,
            template_version: Some(TEMPLATE_VERSION.to_string()),
            user_msg: vec![],
            user: Some(self.credentials.user.clone()),
            password: Some(self.credentials.password.clone()),
            app_name: Some(self.credentials.app_name.clone()),
            app_version: Some(self.credentials.app_version.clone()),
            system_name: Some(self.credentials.system_name.to_string()),
            infra_type: Some(plant as i32),
            mac_addr: vec![],
            os_version: None,
            os_platform: None,
            aggregated_quotes: Some(false),
        };

        RithmicApiClient::send_single_protobuf_message(&mut stream, &login_request).await?;

        // Login Response 11 From Server
        let response: ResponseLogin = RithmicApiClient::read_single_protobuf_message(&mut stream).await?;
        info!("{:?}:{:?}", response, plant);
        if response.rp_code.is_empty() {
            warn!("{:?}",response);
        }
        if response.rp_code[0] != "0".to_string() {
            info!("{:?}",response);
        }

        if let Some(fcm_id) = response.fcm_id { *self.fcm_id.write().await = Some(fcm_id) }

        if let Some(fcm_id) = response.ib_id { *self.ib_id.write().await = Some(fcm_id) }

        let (writer, receiver) = stream.split();
        // install outgoing queue (or get existing)
        let out = self.out_queues.entry(plant)
            .or_insert_with(|| Arc::new(Outgoing::new()))
            .clone();

        // spawn writer that owns the sink
        tokio::spawn(Self::writer_task(plant, writer, out.clone()));

        self.handle_rithmic_responses(plant, receiver);

        self.last_heartbeat_time.insert(plant, Instant::now());
        if let Some(heartbeat_interval_seconds) = response.heartbeat_interval {
            self.start_heart_beat(heartbeat_interval_seconds as u64, plant);
        }

        if plant == SysInfraType::HistoryPlant || plant == SysInfraType::TickerPlant {
            let mut lock = self.live_ready.write().await;
            *lock = true;
        }

        Ok(())
    }

    pub async fn send_message<T: ProstMessage>(
        self: &Arc<Self>,
        system: SysInfraType,
        msg: T,
    ) -> Result<(), RithmicApiError> {
        // Encode with precise capacity
        let mut body = BytesMut::with_capacity(msg.encoded_len());
        msg.encode(&mut body).map_err(|e| RithmicApiError::EngineErrorDebug(format!("encode: {e}")))?;

        // Prefix BE length (4 bytes) + body into one buffer
        let len = body.len() as u32;
        let mut framed = BytesMut::with_capacity(4 + body.len());
        framed.extend_from_slice(&len.to_be_bytes());
        framed.extend_from_slice(&body);

        if let Some(q) = self.out_queues.get(&system) {
            q.push(framed.freeze());  // lock-free, non-blocking
            self.last_heartbeat_time.insert(system, Instant::now());
            Ok(())
        } else {
            // no queue -> reconnect and try once
            self.attempt_reconnect(system).await;
            if let Some(q) = self.out_queues.get(&system) {
                q.push(framed.freeze());
                self.last_heartbeat_time.insert(system, Instant::now());
                Ok(())
            } else {
                Err(RithmicApiError::Disconnected("no writer after reconnect".into()))
            }
        }
    }

    fn next_cid(&self, plant: SysInfraType) -> u64 {
        let binding = self
            .cid_seq
            .entry(plant)
            .or_insert_with(|| Arc::new(AtomicU64::new(1)));
        let counter = binding
            .value();

        let mut old = counter.fetch_add(1, Ordering::Relaxed);
        if old == u64::MAX {
            // we just handed out MAX, wrap back to 1
            counter.store(1, Ordering::Relaxed);
            old = 1;
        }
        old
    }

    /// Issue a request and await a multi-part reply.
    ///
    /// Ordering guarantees:
    /// - We register `pending` **before** the network send so out-of-order early bytes (if any) have a place to land.
    /// - We **stitch rendezvous after the send** to satisfy the intuitive "we haven't sent yet" flow and to avoid any confusion.
    ///
    /// Performance:
    /// - `Bytes` are appended by value; no clones on the hot path.
    /// - Rendezvous chunks are moved into the pending buffer with `drain(..)`.
    pub async fn send_with_reply<Req>(
        self: &Arc<Self>,
        plant: SysInfraType,
        mut req: Req,
    ) -> Result<ChunkVec, RithmicApiError>
    where
        Req: ProstMessage + HasUserMsg,
    {
        let cid = self.next_cid(plant);

        let (tx, rx) = tokio::sync::oneshot::channel::<ChunkVec>();
        self.pending.insert((plant, cid), PendingEntry {
            chunks: SmallVec::new(),
            tx,
        });

        // Attach CID and **send first**
        req.user_msg_mut().insert(0, format!("cid={cid}"));
        self.send_message(plant, req).await?;

        // Then stitch any early-arrived chunks (if the server raced us)
        if let Some((_k, mut staged)) = self.rendezvous.remove(&(plant, cid)) {
            if let Some(mut entry_ref) = self.pending.get_mut(&(plant, cid)) {
                entry_ref.chunks.extend(staged.chunks.drain(..));
            }
            if staged.final_seen {
                if let Some((_, mut entry)) = self.pending.remove(&(plant, cid)) {
                    let chunks = std::mem::take(&mut entry.chunks);
                    return Ok(chunks);
                }
            }
        }

        match tokio::time::timeout(Duration::from_millis(TIME_OUT_MS), rx).await {
            Ok(Ok(chunks)) => Ok(chunks),
            Ok(Err(_)) => {
                self.pending.remove(&(plant, cid));
                Err(RithmicApiError::Disconnected("RPC waiter dropped".into()))
            }
            Err(_) => {
                self.pending.remove(&(plant, cid));
                Err(RithmicApiError::EngineErrorDebug("RPC timeout".into()))
            }
        }
    }

    /// Flushes outgoing frames with small batching to reduce syscalls.
    /// Uses a lock-free queue + a doorbell to avoid needless wake-ups under idle load.
    async fn writer_task(
        plant: SysInfraType,
        mut sink: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        out: Arc<Outgoing>,
    ) {
        const MAX_BATCH: usize = 256;           // frames per flush
        const IDLE_PARK_NS: u64 = 0;            // if you want a tiny busy-poll, keep 0 to block

        loop {
            // 1) drain what’s there (coalesce up to MAX_BATCH)
            let mut drained = 0usize;
            while drained < MAX_BATCH {
                match out.q.pop() {
                    Ok(bytes) => {
                        // send one frame (already length-prefixed)
                        if let Err(e) = sink.send(Message::Binary(bytes)).await {
                            eprintln!("writer({plant:?}) send error: {e}");
                            return; // let reconnect recreate writer
                        }
                        drained += 1;
                    }
                    Err(_) => break,
                }
            }

            // 2) arm doorbell (avoid lost wakeups)
            out.has_items.store(false, Ordering::Release);
            if out.q.is_empty() {
                if IDLE_PARK_NS == 0 {
                    out.bell.notified().await;
                } else {
                    // tiny busy-poll alternative:
                    tokio::time::sleep(std::time::Duration::from_nanos(IDLE_PARK_NS)).await;
                }
            } else {
                // there’s more; set flag so producer won’t “steal” our notify
                out.has_items.store(true, Ordering::Release);
                // loop to drain more
            }
        }
    }

    /// Signs out of rithmic with the specific plant safely shuts down the web socket. removing references from our api object.
    pub async fn shutdown_plant(
        self: &Arc<Self>,
        mut write_stream: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> Result<(), RithmicApiError> {
        //Logout Request 12
        const TID: i32 = 12;
        let logout_request = RequestLogout {
            template_id: TID,
            user_msg: vec![format!("{} Signing Out", self.credentials.app_name)],
        };

        let mut buf = Vec::new();
        match logout_request.encode(&mut buf) {
            Ok(_) => {}
            Err(e) => return Err(RithmicApiError::EngineErrorDebug(format!("Failed to encode RithmicMessage: {}", e)))
        }

        let length = buf.len() as u32;
        let mut prefixed_msg = length.to_be_bytes().to_vec();
        prefixed_msg.extend(buf);

        match write_stream.send(Message::Binary(prefixed_msg.into())).await {
            Ok(_) => Ok(()),
            Err(e) => Err(RithmicApiError::Disconnected(e.to_string()))
        }
    }

    pub(crate) async fn attempt_reconnect(self: &Arc<Self>, plant: SysInfraType) {
        // Atomically check-and-set
        match self.reconnection_locks.entry(plant) {
            Entry::Occupied(_) => return,           // someone is already reconnecting
            Entry::Vacant(v) => { v.insert(()); }   // we are the reconnection owner
        }

        // Ensure the marker is removed even on early returns
        struct Guard<'a> {
            map: &'a DashMap<SysInfraType, ()>,
            k:   SysInfraType,
        }
        impl<'a> Drop for Guard<'a> {
            fn drop(&mut self) { self.map.remove(&self.k); }
        }
        let _guard = Guard { map: &self.reconnection_locks, k: plant };

        warn!("Reconnecting to rithmic: {plant:?}");

        let mut delay = Duration::from_secs(2);
        let max_delay = Duration::from_secs(30);
        const TZ: Tz = chrono_tz::America::Chicago;
        loop {
            let now = chrono::Utc::now();
            let chicago_time = now.with_timezone(&TZ);

            if is_weekend_or_off_hours(chicago_time) {
                let next_market_open = next_chicago_market_open(chicago_time);
                let reconnect_time = next_market_open - chrono::Duration::minutes(5);
                let wait = (reconnect_time - now).num_seconds().max(0) as u64; // clamp
                info!("Outside market hours. Waiting {wait}s until ~open-5min");
                sleep(Duration::from_secs(wait)).await;
                continue;
            }

            match self.connect_and_login(plant).await {
                Ok(_) => {
                    info!("Reconnected successfully: {plant:?}");
                    if plant == SysInfraType::HistoryPlant || plant == SysInfraType::TickerPlant {
                        self.resubscribe_all_for(plant).await;
                    }
                    break;
                }
                Err(e) => {
                    error!("Reconnect failed {plant:?}: {e}. retry in {delay:?}");
                    sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                }
            }
        } // _guard drops here -> removes marker
    }

    pub async fn resubscribe_all_for(self: &Arc<Self>, plant: SysInfraType) {


        // 1) Snapshot and drop lock before awaiting
        let entries: Vec<ActiveSub> = {
            let guard = self.active_subscriptions.read().await;
            guard
                .get(&plant)
                .cloned()                // clone the Vec<ActiveSub>
                .unwrap_or_default()
        };

        // 2) Choose resubscribe fn based on plant
        match plant {
            SysInfraType::TickerPlant => {
                // Optional: run a few concurrently
                let concurrency = 8usize;
                stream::iter(entries)
                    .for_each_concurrent(concurrency, |s| async move {
                        let Some(bits) = bits_for(&s.kind) else {
                            error!("Invalid sub type in resubscribe: {:?}", s.kind);
                            return;
                        };
                        match self.request_market_data_update(s.symbol, s.exchange, 1, bits).await {
                            Ok(_)  => info!("Resubscribed TickerPlant OK"),
                            Err(e) => error!("Resubscribe TickerPlant failed: {e}"),
                        }
                    })
                    .await;
            }
            SysInfraType::HistoryPlant => {
                let concurrency = 4usize;
                stream::iter(entries)
                    .for_each_concurrent(concurrency, |s| async move {
                        let Some(res) = s.resolution else {
                            error!("HistoryPlant resubscribe missing resolution for {:?}", s);
                            return;
                        };
                        match self.request_time_bar_update(s.symbol, s.exchange, res, 1).await {
                            Ok(_)  => info!("Resubscribed HistoryPlant OK"),
                            Err(e) => error!("Resubscribe HistoryPlant failed: {e}"),
                        }
                    })
                    .await;
            }
            _ => {
                // if you have other plants, handle them here
                warn!("No resubscribe handler for plant: {:?}", plant);
            }
        }
    }

    pub fn start_heart_beat(
        self: &Arc<Self>,
        heartbeat_interval_seconds: u64,
        plant: SysInfraType,
    ) {
        // Clone Arc so the spawned future is 'static + Send
        let client = Arc::clone(self);

        tokio::spawn(async move {
            // Treat server interval as a *deadline* for any outbound traffic
            let hb_period = Duration::from_secs(heartbeat_interval_seconds.max(1));
            let safety    = Duration::from_millis(250);

            // Poll more frequently than the deadline
            let mut tick = time::interval(std::cmp::min(hb_period / 4, Duration::from_millis(500)));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            let tick_period = tick.period(); // tokio >= 1.37

            loop {
                tick.tick().await;

                // Read monotonic last outbound time (set by writer_task on each successful send)
                let last: Instant = client
                    .last_heartbeat_time
                    .get(&plant)
                    .map(|t| *t)                 // copy Instant out; guard drops immediately
                    .unwrap_or_else(Instant::now);

                let elapsed = last.elapsed();

                // If waiting until the next tick risks crossing the deadline, send now.
                if elapsed + tick_period >= hb_period - safety {
                    const TID: i32 = 18;
                    let now = Utc::now();
                    let hb = RequestHeartbeat {
                        template_id: TID,
                        user_msg: vec![],
                        ssboe: Some(now.timestamp() as i32),
                        usecs: Some(now.timestamp_subsec_micros() as i32),
                    };

                    if let Err(e) = client.send_message(plant, hb).await {
                        error!("Heartbeat send failed for {:?}: {}", plant, e);
                        // Let reconnect logic handle it; we just keep looping.
                    }
                    // No need to touch `last_outbound` here; writer_task updates it on send.
                }
            }
        });
    }

    /// Returns counts of pending and staged entries for a plant.
    pub fn rpc_buffers_snapshot(&self, plant: SysInfraType) -> (usize, usize) {
        let pending = self.pending.iter().filter(|kv| kv.key().0 == plant).count();
        let staged  = self.rendezvous.iter().filter(|kv| kv.key().0 == plant).count();
        (pending, staged)
    }

    /// Reads WS frames, splits BE-length-delimited protobuf segments, and routes them.
    ///
    /// Efficiency:
    /// - Zero-copy slicing with `Bytes::slice` per segment.
    /// - Bounded MPSC to decouple IO from decode/route; backpressure prevents unbounded growth.
    /// - Processor never clones payload; it appends `Bytes` by value to `SmallVec`.
    /// - Final detection uses a cheap tag scan; if uncertain, we re-check in the processor.
    ///
    /// Implementation notes: per-segment logging is at TRACE; Bytes are sliced zero-copy; we pre-reserve frame bytes to avoid reallocs; SmallVec in the rendezvous cache uses inline capacity 8 to avoid heap in common multipart replies.
    pub fn handle_rithmic_responses(
        self: &Arc<Self>,
        plant: SysInfraType,
        mut reader: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) {
        // (template_id, payload, cid, is_final_hint). `payload` is owned (moved), no clones in hot path.
        let (tx, mut rx) = mpsc::channel::<(i32, Bytes, Option<u64>, bool)>(10_000);

        // Spawn message processor task
        let process_plant = plant.clone();
        let self_clone = self.clone();
        tokio::spawn(async move {
            while let Some((template_id, payload, cid_opt, is_final_hint)) = rx.recv().await {
                if let Some(cid) = cid_opt {
                    let key = (process_plant, cid);

                    // Prefer the reader's hint; if absent, fall back to scanning again here.
                    let final_now = if is_final_hint { true } else { fast_detect_final(&payload).unwrap_or(false) };

                    if let Some(mut entry_ref) = self_clone.pending.get_mut(&key) {
                        // We already have a waiter; just accumulate.
                        entry_ref.chunks.push(payload);
                        if final_now {
                            // Complete the oneshot now.
                            drop(entry_ref);
                            if let Some((_, mut entry)) = self_clone.pending.remove(&key) {
                                let chunks = std::mem::take(&mut entry.chunks);
                                let _ = entry.tx.send(chunks);
                            }
                        }
                    } else {
                        // No waiter yet — stage it in rendezvous to defeat races.
                        let mut staged = self_clone.rendezvous.entry(key).or_default();
                        {
                            let s = staged.value_mut();
                            s.chunks.push(payload);
                            if final_now { s.final_seen = true; }
                        }
                    }
                    continue;
                } else {
                    // No CID: route by plant/template_id as you already do
                    match process_plant {
                        SysInfraType::TickerPlant  => match_ticker_plant_id(template_id, payload, &self_clone.hub).await,
                        SysInfraType::HistoryPlant => match_history_plant_id(template_id, payload, &self_clone).await,
                        SysInfraType::OrderPlant   => { /* ... */ }
                        SysInfraType::PnlPlant     => { /* ... */ }
                        SysInfraType::RepositoryPlant => match_repo_plant_id(template_id, payload).await,
                    }
                }
            }
        });

        let client = self.clone();
        tokio::spawn(async move {
            let mut acc = BytesMut::with_capacity(64 * 1024);
            'main_loop: loop {
                tokio::select! {
                    Some(Ok(message)) = reader.next() => {
                        match message {
                            Message::Binary(frame) => {
                                {
                                    // Accumulate bytes across frames; segments are BE-length-delimited
                                    acc.reserve(frame.len());
                                    acc.extend_from_slice(&frame);

                                    loop {
                                        if acc.len() < 4 { break; }
                                        let len = {
                                            let mut hdr = [0u8; 4];
                                            hdr.copy_from_slice(&acc[..4]);
                                            u32::from_be_bytes(hdr) as usize
                                        };
                                        if acc.len() < 4 + len { break; }

                                        // Drop header and extract one complete protobuf payload
                                        let _ = acc.split_to(4);
                                        let payload_buf = acc.split_to(len);
                                        let payload: Bytes = payload_buf.freeze();

                                        // fast scans (no allocs)
                                        let template_id = match fast_extract_template_id(&payload) {
                                            Some(t) => t,
                                            None => {
                                                error!("Missing template_id in message segment");
                                                continue;
                                            }
                                        };
                                        let cid = fast_extract_cid(&payload);
                                        let is_final_hint = fast_detect_final(&payload).unwrap_or(false);

                                        if let Err(e) = tx.send((template_id, payload, cid, is_final_hint)).await {
                                            error!("Fatal: processor channel closed: {}", e);
                                            break 'main_loop;
                                        }
                                    }
                                }
                            }
                            Message::Close(close_frame) => {
                                warn!("Received close message: {:?}. Attempting reconnection.", close_frame);
                                client.attempt_reconnect(plant.clone()).await;
                                break 'main_loop;
                            }
                            Message::Frame(frame) => {
                                if format!("{:?}", frame).contains("CloseFrame") {
                                    warn!("Received close frame. Attempting reconnection.");
                                    client.attempt_reconnect(plant.clone()).await;
                                    break 'main_loop;
                                }
                            }
                            Message::Text(text) => println!("{}", text),
                            Message::Ping(_) | Message::Pong(_) => {}
                        }
                    },
                }
            }

            // Cleanup
            info!("Closing Rithmic response handler for plant: {:?}", plant);
            drop(tx);
        });
    }

    pub async fn replay_bars(self: &Arc<RithmicApiClient>, symbol: String, exchange: Exchange, resolution: Resolution, window_start: DateTime<Utc>, window_end: DateTime<Utc>) ->  Result<ChunkVec, RithmicApiError> {
        const PLANT: SysInfraType = SysInfraType::HistoryPlant;
        // Send the request based on data type
        const TID: i32 = 206;
        match resolution {
            Resolution::TickBars(num) => {
                let req = RequestTickBarReplay {
                    template_id: TID,
                    user_msg: vec![],
                    symbol: Some(symbol),
                    exchange: Some(exchange.to_string()),
                    bar_type: Some(crate::client::rithmic_proto_objects::rti::request_tick_bar_replay::BarType::TickBar.into()),
                    bar_sub_type: Some(1),
                    bar_type_specifier: Some(num.to_string()),
                    start_index: Some(window_start.timestamp() as i32),
                    finish_index: Some(window_end.timestamp() as i32),
                    user_max_count: Some(2000),
                    custom_session_open_ssm: None,
                    custom_session_close_ssm: None,
                    direction: Some(crate::client::rithmic_proto_objects::rti::request_tick_bar_replay::Direction::First.into()),
                    time_order: Some(crate::client::rithmic_proto_objects::rti::request_tick_bar_replay::TimeOrder::Forwards.into()),
                    resume_bars: Some(false),
                };
                self.send_with_reply(PLANT, req).await
            }
            _ => {
                let (num, res_type) = match resolution {
                    Resolution::Seconds(num) => (Some(num as i32), crate::client::rithmic_proto_objects::rti::request_time_bar_replay::BarType::SecondBar.into()),
                    Resolution::Minutes(num) =>
                        if num == 1 {
                            (Some(60), crate::client::rithmic_proto_objects::rti::request_time_bar_replay::BarType::SecondBar.into())
                        }else {
                            (Some(num as i32), crate::client::rithmic_proto_objects::rti::request_time_bar_replay::BarType::MinuteBar.into())
                        }
                    Resolution::Daily => {
                        (None, crate::client::rithmic_proto_objects::rti::request_time_bar_replay::BarType::DailyBar.into())
                    }
                    Resolution::Weekly => {
                        (None, crate::client::rithmic_proto_objects::rti::request_time_bar_replay::BarType::WeeklyBar.into())
                    }
                    _ => return Err(RithmicApiError::EngineErrorDebug("Incrorrect resolution for candles".to_string()))
                };
                //println!("Requesting candles for {} {} {} {} {} {}", symbol_name, exchange, window_start, window_end, num, res_type);
                let req = RequestTimeBarReplay {
                    template_id: 202,
                    user_msg: vec![],
                    symbol: Some(symbol),
                    exchange: Some(exchange.to_string()),
                    bar_type: Some(res_type),
                    bar_type_period: num,
                    start_index: Some(window_start.timestamp() as i32),
                    finish_index: Some(window_end.timestamp() as i32),
                    user_max_count: Some(2000),
                    direction: Some(Direction::First.into()),
                    time_order: Some(TimeOrder::Forwards.into()),
                    resume_bars: Some(false),
                };

                self.send_with_reply(PLANT, req).await

            }
        }
    }
}

#[async_trait::async_trait]
impl MarketDataProvider for RithmicApiClient {
    fn md_vendor_name(&self) -> &str {
        &self.name
    }

    async fn ensure_connected(self: Arc<Self>) -> anyhow::Result<()> {
        // Fast path if already initialized.
        {
            let is_ready = self.live_ready.read().await;
            if *is_ready == true { return Ok(()); }
        }

        self.connect_and_login(SysInfraType::HistoryPlant).await?;
        self.connect_and_login(SysInfraType::TickerPlant).await?;

        Ok(())
    }

    async fn subscribe(self: Arc<Self>, req: &MarketDataRequest) -> Result<Arc<dyn SubscriptionHandle>> {
        // --- subscribe paths (same as you have) ---
        do_live_req(&self, req.clone(), 1).await?;

        Ok(Arc::new(LiveSub {
            client: self.clone(),
            req: req.clone(),
            id: fastrand::u64(..),
            closed: AtomicBool::new(false),
        }))
    }

    fn resolver(self: Arc<Self>) -> Arc<dyn SecurityResolver> {
        Arc::new(ProviderResolver::new(self.name().to_string(), self.clone()))
    }
}

/// Small helper to DRY the subscribe/unsubscribe logic.
/// Mirrors your existing branching for TICKS/BBO/BOOK/CANDLES.
async fn do_live_req(client: &Arc<RithmicApiClient>, req: MarketDataRequest, action: i32) -> Result<()> {
    // Ticks (LastTrade via Market Data Update 100) or tick bars
    if req.kinds.intersects(FeedKind::TICKS) {
        if let Some(res) = req.resolution.clone() {
            match res {
                Resolution::TickBars(_n) => {
                    // TODO: Tick Bar Update / Replay (204/206)
                    // client.request_tick_bar_update(req.symbol.clone(), req.exchange.clone(), action, UpdateBits::TickBars).await?;
                }
                _ => {
                    client.request_market_data_update(
                        req.symbol.clone(),
                        req.exchange.to_string(),
                        action,
                        UpdateBits::LastTrade
                    ).await?;
                }
            }
        } else {
            client.request_market_data_update(
                req.symbol.clone(),
                req.exchange.to_string(),
                action,
                UpdateBits::LastTrade
            ).await?;
        }
    }

    // BBO
    if req.kinds.intersects(FeedKind::BBO) {
        client.request_market_data_update(
            req.symbol.clone(),
            req.exchange.to_string(),
            action,
            UpdateBits::Bbo
        ).await?;
    }

    // Order Book
    if req.kinds.contains(FeedKind::BOOK) {
        client.request_market_data_update(
            req.symbol.clone(),
            req.exchange.to_string(),
            action,
            UpdateBits::OrderBook
        ).await?;
    }

    // Time candles (HistoryPlant Time Bar Update 200)
    if let Some(res) = req.resolution.clone() {
        match res {
            Resolution::TickBars(_n) => {
                // TODO: if you support tick-bar updates natively, mirror above with Unsubscribe
                // client.request_tick_bar_update(..., action, UpdateBits::TickBars).await?;
            }
            _ => {
                // Note: HistoryPlant endpoint; your `request_time_bar_update` should route there
                // and encode subscribe/unsubscribe semantics.
                client.request_time_bar_update(
                    req.symbol.clone(),
                    req.exchange.to_string(),
                    res,
                    action, // <-- ensure your function accepts Request to toggle sub/unsub
                ).await?;
            }
        }
    }

    Ok(())
}

/// RAII subscription; on drop or explicit `unsubscribe()` sends Unsubscribe requests once.
struct LiveSub {
    client: Arc<RithmicApiClient>,
    req: MarketDataRequest,
    id: u64,
    closed: AtomicBool,
}

impl SubscriptionHandle for LiveSub {
    fn unsubscribe(&self) -> Result<(), anyhow::Error> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(anyhow!("Already unsubscribed"));
        }
        let client = self.client.clone();
        let req = self.req.clone();
        // Fire-and-forget; if you want backpressure, return a JoinHandle or make this async.
        tokio::spawn(async move {
            if let Err(e) = do_live_req(&client, req, 2).await {
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
            if let Err(e) = do_live_req(&client, req, 2).await {
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
    symbol: String,
    exchange: String,
    resolution: Option<Resolution>,
    kind: SubType,
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

fn bits_for(kind: &SubType) -> Option<UpdateBits> {
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
    chunks: SmallVec<Bytes, 8>,
    final_seen: bool,
}


/// Fast varint decoder; returns (value, bytes_read) or None.
#[inline]
pub(crate) fn read_varint(buf: &[u8], mut i: usize) -> Option<(u64, usize)> {
    let mut x: u64 = 0;
    let mut s: u32 = 0;
    let start = i;
    while i < buf.len() {
        let b = buf[i] as u64;
        i += 1;
        x |= (b & 0x7F) << s;
        if b & 0x80 == 0 { return Some((x, i - start)); }
        s += 7;
        if s >= 64 { return None; }
    }
    None
}

/// Minimal protobuf scan to decide if this payload is FINAL (has `rp_code` = field #132766)
/// or a CONTINUATION (has `rq_handler_rp_code` = field #132764). Returns Some(true)=final, Some(false)=cont, None=unknown.
#[inline]
fn fast_detect_final(payload: &bytes::Bytes) -> Option<bool> {
    const RQ_HANDLER_RP_CODE: u64 = 132_764;
    const RP_CODE: u64 = 132_766;

    let buf = payload.as_ref();
    let mut i = 0usize;

    while i < buf.len() {
        let (key, klen) = read_varint(buf, i)?;
        i += klen;

        let field = key >> 3;
        let wt = (key & 7) as u8;

        // Early exits: as soon as we identify the field, we can return.
        if field == RP_CODE {
            return Some(true);
        }
        if field == RQ_HANDLER_RP_CODE {
            return Some(false);
        }

        // Not a target field — skip its value by wire type.
        match wt {
            0 => { let (_, n) = read_varint(buf, i)?; i += n; }                 // varint
            1 => { i += 8; }                                                    // 64-bit
            2 => { let (l, n) = read_varint(buf, i)?; i += n + (l as usize); }  // len-delimited
            5 => { i += 4; }                                                    // 32-bit
            _ => return None,
        }
    }
    None
}

/// Extract `cid` from any `user_msg` string (field #132760, len-delimited). Returns None if not present.
#[inline]
fn fast_extract_cid(payload: &Bytes) -> Option<u64> {
    const USER_MSG: u64 = 132_760;
    let buf = payload.as_ref();
    let mut i = 0usize;
    while i < buf.len() {
        let (key, klen) = read_varint(buf, i)?; i += klen;
        let field = key >> 3; let wt = (key & 7) as u8;
        match wt {
            0 => { let (_, n) = read_varint(buf, i)?; i += n; },
            1 => { i += 8; },
            2 => {
                let (l, n) = read_varint(buf, i)?; i += n;
                if field == USER_MSG {
                    let s = &buf[i..i + (l as usize)];
                    // Look for ascii "cid=" and parse following digits
                    if let Some(pos) = memchr::memmem::find(s, b"cid=") {
                        let mut j = pos + 4; // after "cid="
                        let mut val: u64 = 0;
                        let mut seen = false;
                        while j < s.len() {
                            let c = s[j];
                            if (b'0'..=b'9').contains(&c) {
                                val = val * 10 + (c - b'0') as u64; seen = true; j += 1;
                            } else { break; }
                        }
                        if seen { return Some(val); }
                    }
                }
                i += l as usize;
            },
            5 => { i += 4; },
            _ => return None,
        }
    }
    None
}
/// Attempt to read template_id quickly without full decode.
/// We handle common cases:
///   - field #11 (many RTI messages)
///   - field #154_467 (some repository/product messages)
/// Both are `varint` wire type. We fall back to the slower `extract_template_id` if not found.
#[inline]
fn fast_extract_template_id(payload: &bytes::Bytes) -> Option<i32> {
    const TID_A: u64 = 11;
    const TID_B: u64 = 154_467;

    let buf = payload.as_ref();
    let mut i = 0usize;

    while i < buf.len() {
        let (key, klen) = read_varint(buf, i)?; i += klen;
        let field = key >> 3;
        let wt = (key & 7) as u8;

        if wt == 0 && (field == TID_A || field == TID_B) {
            let (val, _n) = read_varint(buf, i)?; // we don't need to advance after returning
            return i32::try_from(val).ok();
        }

        match wt {
            0 => { let (_, n) = read_varint(buf, i)?; i += n; }
            1 => { i += 8; }
            2 => { let (l, n) = read_varint(buf, i)?; i += n + l as usize; }
            5 => { i += 4; }
            _ => return None,
        }
    }
    None
}