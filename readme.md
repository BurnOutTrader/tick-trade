# Tick-Trade

A Rust workspace for building a low-latency trading engine with unified market data routing, vendor integrations (Rithmic), and reusable strategy components.  
The architecture is designed so that the **same strategy code** runs in both **live** and **backtest** modes with minimal branching.

Planned vendor support:
- Rithmic
- ProjectX
- Databento

Planned broker support:
- ProjectX Prop firms
- Rithmic Brokers or prop firms
- Designed to allow future integrations of any market type.

Future Proof
- dynamic `MarketDataProvider` traits that can be implemented by any vendor.
- dynamic `HistoricalDataProvider` traits that can be implemented by any vendor.
- dynamic UniverseSelection to allow development of more complex strategies in the future.
---


# Design
- Strategies run as libraries from launched into the strategy manager from main.
- multiple strategies can run simultaneously.  
- future hot loading
- backtest strategies get a unique engine context and market data 
- live strategies share the same engine context and market data
- 

# Next Steps
- Make history serialization database using DuckDb
- Make live order handling for ProjectX and Rithmic
- Make data vendor impl for projectX and databento
- Make Historical data feed
- Make backtesting engine

## Stack
- **Language:** Rust (workspace, edition 2024)
- **Async runtime:** tokio
- **Concurrency/data:** dashmap, parking_lot, atomics
- **Serialization:** serde
- **Time:** chrono, chrono-tz
- **Logging/telemetry:** tracing, tracing-subscriber
- **Networking/WebSocket:** tokio-tungstenite, tungstenite
- **Protobuf:** prost

---

## Workspace Crates
- **crates/app** (`tick-trade`)
  - Library exposing user strategies (`user_strategies/`).
  - No binary entrypoint; integration binaries can be added separately.
- **crates/standard_lib**
  - Core engine:
    - StrategyManager, EngineCtx, StrategyEngine
    - EventHub (pub/sub for normalized events)
    - MdRouter (subscription deduplication)
    - API traits (MarketDataProvider, FuturesUniverseProvider, etc.)
    - Market data models (ticks, candles, books, consolidators, universes, securities)
    - Clocks (WallClock, SimClock)
- **crates/apis/rithmic**
  - Vendor integration: RithmicApiClient implements MarketDataProvider.
  - Plant handlers for tick, history, order, and pnl streams.
  - Protocol layer (prost-generated + custom readers).

---

## Engine Architecture

### StrategyManager
- Manages all strategies across live and backtest contexts.
- Creates and reuses EngineCtx per live run. or a unique context per backtest.
- Applies `UniversePlan`s and schedules symbol selection.
- Spawns strategy runners and routes `StrategyEvent`s.

### EngineCtx
- Shared root environment for each run.
- Wires together:
  - **EventHub** — pub/sub bus for normalized data/events.
  - **SecurityRegistry** — canonical metadata store.
  - **MdRouter** — manages vendor subscriptions.
- Provides mode-aware view (`StrategyEngine`) for strategies.
- Has dual clocks:
  - `live_clock` (WallClock)
  - `sim_clock` (SimClock, controllable)

### MdRouter
- Deduplicates market data subscriptions by `SubKey` (vendor, symbol, exchange, kinds, resolution, depth).
- Providers are selected via `VendorPolicy` (Exact/Prefer/Any).
- Subscriptions return `SubGuard`s; when all guards drop, unsubscribes automatically.
- Live mode implemented; backtest mode stubs to a future simulator.

### EventHub
- Broadcasts typed streams (ticks, candles, books, BBO, etc.).
- Global + per-symbol topics.
- Strategies subscribe with lightweight channels.

### Consolidators
- Aggregate ticks or lower-resolution candles into higher bars (e.g., 5m).
- Handle subtlety of bar end-times (patch includes subtracting 1ns to avoid overlap).
- Designed to work in both live and backtest feeds.

### Registry
- `SecurityRegistry` holds instruments keyed by (SymbolId, Exchange).
- Emits `RegistryEvent` (Added, Updated, Removed) via broadcast.
- Optional `SecurityResolver` auto-fills missing securities (e.g., via vendor API).

### StrategyEngine
The **StrategyEngine** is the façade object that strategies interact with.  
It wraps an [`EngineCtx`] but is **scoped to a specific mode** (`Live` or `Backtest`).  
This ensures all subscriptions, time queries, and feed helpers automatically use the correct clock and router without the strategy needing to care.
---

## Rithmic Integration (`apis/rithmic`)
- **RithmicApiClient**
  - Connects over secure WebSocket.
  - Logs in, maintains heartbeats, handles reconnect/resubscribe.
  - Implements MarketDataProvider trait.
- **Plant Handlers**
  - `tick_plant` – real-time market data.
  - `history_plant` – historical requests/responses.
  - `order_plant` – TODO: lifecycle.
  - `pnl_plant` – TODO: specifics.
- **Server Models**
  - `RithmicServer` / `RithmicSystem` enums.
  - Endpoint mappings.
  - `RithmicCredentials` for auth.

---

## Setup
1. Install Rust: [https://rustup.rs](https://rustup.rs)
2. Clone:
   ```bash
   git clone <repo>
   cd tick-trade


RITHMIC_USER=...
RITHMIC_PASSWORD=...
RITHMIC_SERVER=Chicago
RITHMIC_SYSTEM="Rithmic 01" or "APEX" etc
RITHMIC_APP_NAME=...
RITHMIC_APP_VERSION=...
RITHMIC_FCM_ID=... 
RITHMIC_IB_ID=...  
RITHMIC_USER_TYPE=...