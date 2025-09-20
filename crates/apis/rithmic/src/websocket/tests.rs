use std::env;
use std::sync::Arc;
use standard_lib::engine_core::event_hub::EventHub;
use crate::websocket::api_base::RithmicApiClient;
use crate::websocket::errors::RithmicApiError;
use crate::websocket::server_models::{RithmicCredentials, RithmicServer, RithmicSystem};

#[allow(dead_code)]
async fn await_buffers_drained(
    client: &Arc<RithmicApiClient>,
    plant: crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType,
    max_ms: u64,
) {
    use tokio::time::{sleep, Duration};
    use std::time::Instant;

    fn counts(client: &Arc<RithmicApiClient>, plant: crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType) -> (usize, usize) {
        let pending = client
            .pending
            .iter()
            .filter(|kv| kv.key().0 == plant)
            .count();
        let staged = client
            .rendezvous
            .iter()
            .filter(|kv| kv.key().0 == plant)
            .count();
        (pending, staged)
    }

    let deadline = Instant::now() + Duration::from_millis(max_ms);
    let mut last_report = Instant::now();
    let mut last_counts = (usize::MAX, usize::MAX);

    loop {
        // Snapshot state without holding locks across await
        let pending_empty = client.pending.iter().all(|kv| kv.key().0 != plant);
        let rendezvous_empty = client.rendezvous.iter().all(|kv| kv.key().0 != plant);

        if pending_empty && rendezvous_empty {
            break;
        }

        // Periodic progress log (every ~200ms) if counts change
        if last_report.elapsed() >= Duration::from_millis(200) {
            let now_counts = counts(client, plant);
            if now_counts != last_counts {
                println!(
                    "await_buffers_drained: plant={:?} pending={} staged={} ({}ms left)",
                    plant,
                    now_counts.0,
                    now_counts.1,
                    deadline.saturating_duration_since(Instant::now()).as_millis()
                );
                last_counts = now_counts;
            }
            last_report = Instant::now();
        }

        if Instant::now() >= deadline {
            let (p, s) = counts(client, plant);
            eprintln!(
                "await_buffers_drained TIMEOUT: plant={:?} pending={} staged={}",
                plant, p, s
            );
            break;
        }

        // Small backoff; keeps CPU low but responsive.
        sleep(Duration::from_millis(20)).await;
    }
}

#[allow(dead_code)]
fn test_client() -> Result<Arc<RithmicApiClient>, RithmicApiError> {
    let username = env::var("RITHMIC_USERNAME").expect("Missing RITHMIC_USERNAME");
    let app_name = env::var("RITHMIC_APPNAME").expect("Missing RITHMIC_APPNAME");
    let app_version = env::var("RITHMIC_APPVER").unwrap_or_else(|_| "1.0".into());
    let password = env::var("RITHMIC_PASSWORD").expect("Missing RITHMIC_PASSWORD");

    let rithmic_credentials = RithmicCredentials::new(
        username,
        RithmicServer::Chicago, // could parse `server` if needed
        RithmicSystem::Apex,    // could parse `system` if needed
        app_name,
        app_version,
        password,
        Some("Apex".to_string()),
        Some("Apex".to_string()),
        Some(3)
    );

    let hub = Arc::new(EventHub::new());
    RithmicApiClient::new(None, rithmic_credentials, hub)
}

#[tokio::test]
#[ignore] // don't run on `cargo test` unless explicitly requested
async fn test_live_rithmic_connection() {
    use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;
    use std::time::Duration;
    use dotenvy::dotenv;
    use tokio::time::sleep;
    use crate::websocket::rithmic_proto_objects::rti::RequestTradeRoutes;
    dotenv().ok(); // load from .env if available

    let client = test_client().unwrap();

    let systems = vec![
        SysInfraType::TickerPlant,
        SysInfraType::HistoryPlant,
        SysInfraType::OrderPlant,
        SysInfraType::PnlPlant,
    ];

    for system in &systems {
        match client.connect_and_login(*system).await {
            Ok(_) => println!("✅ Connected to {:?}", system),
            Err(e) => eprintln!("❌ Failed to connect {:?}: {:?}", system, e),
        }
    }

    for system in &systems {
        await_buffers_drained(&client, *system, 1500).await;
    }

    sleep(Duration::from_secs(90)).await;

    // now force a message send, which should trigger reconnect
    let req = RequestTradeRoutes {
        template_id: 310,
        user_msg: vec!["test".to_string()],
        subscribe_for_updates: Some(true),
    };
    let system = SysInfraType::OrderPlant;
    match client.send_message(system, req).await {
        Ok(_) => println!("✅ OrderPlant request sent post-idle"),
        Err(e) => eprintln!("❌ Failed to maintain connection after heartbeat expiry {:?}: {:?}", system, e),
    }
}

#[tokio::test]
#[ignore] // don't run on `cargo test` unless explicitly requested
async fn test_live_rithmic_reconnection() {
    use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;
    use crate::websocket::rithmic_proto_objects::rti::RequestTradeRoutes;
    use dotenvy::dotenv;

    dotenv().ok(); // load from .env if available

    let client = test_client().unwrap();
    let system = SysInfraType::OrderPlant;

    match client.connect_and_login(system).await {
        Ok(_) => println!("✅ Connected to {:?}", system),
        Err(e) => eprintln!("❌ Failed to connect {:?}: {:?}", system, e),
    }

    await_buffers_drained(&client, system, 1500).await;

    // drop the writer & readerout_queues
    client.out_queues.remove(&system);

    // now force a message send, which should trigger reconnect
    let req = RequestTradeRoutes {
        template_id: 310,
        user_msg: vec!["test".to_string()],
        subscribe_for_updates: Some(true),
    };

    match client.send_message(system, req).await {
        Ok(_) => println!("✅ Reconnected to {:?}", system),
        Err(e) => eprintln!("❌ Failed to reconnect {:?}: {:?}", system, e),
    }
}

#[tokio::test]
#[ignore] // don't run on `cargo test` unless explicitly requested
async fn test_live_rithmic_callbacks() {
    use crate::websocket::rithmic_proto_objects::rti::request_login::SysInfraType;
    use crate::websocket::rithmic_proto_objects::rti::ResponseReferenceData;
    use prost::Message;
    use dotenvy::dotenv;

    dotenv().ok(); // load from .env if available

    let client = test_client().unwrap();
    let system = SysInfraType::TickerPlant;

    match client.connect_and_login(system).await {
        Ok(_) => println!("✅ Connected to {:?}", system),
        Err(e) => eprintln!("❌ Failed to connect {:?}: {:?}", system, e),
    }

    // Using your existing helper, per your example main:
    let raw = client
        .request_reference_data(system, "MNQ".to_string(), "CME".to_string())
        .await
        .expect("reference data bytes");

    // `raw` is a Vec/SmallVec of `bytes::Bytes` chunks; prost expects a single Buf or &[u8].
    let total: usize = raw.iter().map(|b| b.len()).sum();
    let mut flat = Vec::with_capacity(total);
    for b in &raw { flat.extend_from_slice(&b[..]); }

    let msg = ResponseReferenceData::decode(&flat[..])
        .expect("decode ResponseReferenceData");
    println!("reference data: {:?}", msg);

    // Optional sanity assertions (adjust to your schema)
    assert_eq!(msg.template_id, 15); // if present in response
    assert!(msg.user_msg.iter().any(|s| s.starts_with("cid=")));

    // Allow the background processor to flush any staged continuation chunks and
    // move rendezvous->pending->completed. Then assert buffers are drained for this plant.
    await_buffers_drained(&client, system, 1500).await;
    // One more quick settle pass to catch any late FINAL frames
    await_buffers_drained(&client, system, 300).await;
    assert!(
        client.pending.iter().all(|kv| kv.key().0 != system),
        "pending still contains entries for TickerPlant after reply (processor slow?)"
    );
    assert!(
        client.rendezvous.iter().all(|kv| kv.key().0 != system),
        "rendezvous still contains staged entries for TickerPlant after reply (final not observed?)"
    );
}