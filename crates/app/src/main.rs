use std::env;
use std::sync::Arc;
use rithmic::websocket::api_base::RithmicApiClient;
use rithmic::websocket::server_models::{RithmicCredentials, RithmicServer, RithmicSystem};
use standard_lib::engine_core::event_hub::EventHub;
use standard_lib::engine_core::strategy_manager::StrategyManager;
use tick_trade::user_strategies::data_feed_strategy::DataSubscriptionExampleStrategy;

/// ------------------------------
/// main: build Engine + run
/// ------------------------------
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter("info").init();

    let username    = env::var("RITHMIC_USERNAME")?;
    let app_name    = env::var("RITHMIC_APPNAME")?;
    let app_version = env::var("RITHMIC_APPVER").unwrap_or_else(|_| "1.0".into());
    let password    = env::var("RITHMIC_PASSWORD")?;

    // 1) Create a canonical registry and the manager
    let mut sm = StrategyManager::new();

    // 2) Make sure a LIVE context exists so we can reuse its ONE hub end-to-end
    //    (this returns a reference; we clone the Arc<EventHub> for the provider ctor)
    let live_ctx = sm.get_live_ctx();                // <- add this helper if you don’t have it yet
    let hub_for_provider: Arc<EventHub> = live_ctx.hub.clone();

    // 3) Build the Rithmic provider with the engine’s hub
    let creds = RithmicCredentials::new(
        username.clone(),
        RithmicServer::Chicago,
        RithmicSystem::Apex,
        app_name.clone(),
        app_version.clone(),
        password.clone(),
        Some("Apex".to_string()),
        Some("Apex".to_string()),
        Some(3),
    );
    let rith = RithmicApiClient::new(Some("Rithmic".to_string()), creds, hub_for_provider.clone())?;

    // 5) Register + connect provider on the SAME live MdRouter the manager owns
    sm.register_provider(rith).await?;

    // 6) Add your strategy (its mode() decides Live vs Backtest)
    let strat = Arc::new(DataSubscriptionExampleStrategy::new());
    sm.add_strategy_instance("DataSubExample", strat).await?;

    // 7) Drive the manager (it uses the right clock per context internally)
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        tick.tick().await;
        if let Err(e) = sm.step().await {
            tracing::error!(?e, "manager step failed");
        }
    }
}

