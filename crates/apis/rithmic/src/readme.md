# Deep Dive into Data handling

## ProstMessage Types: These types are vendor-specific and only used inside RithmicApiClient.
***Prost wire messages (from Rithmic)***

•	LastTrade (150): raw trade prints — trade_price, trade_size, aggressor (1=Buy, 2=Sell), ssboe/usecs time.

•	BestBidOffer (151): top-of-book snapshot — bid_price/bid_size, ask_price/ask_size, ssboe/usecs.

•	OrderBook (156): L2 book updates/snapshots — arrays of prices/sizes + update_type (Begin/Middle/End/Solo/...) and presence bits (Bid/Ask).

•	TimeBar (250): time-based bars — type (Second/Minute/Daily/Weekly), period (e.g. “1”), open/high/low/close, volume (u64).

## Standardized core types
**Everything outside the client speaks these types only.**
```rust
Tick: {symbol, exchange, price, size, time, aggressor, side}
Bbo: {symbol, exchange, bid, bid_size, ask, ask_size, time}
OrderBook: {symbol, exchange, bids: Vec<(price,size)>, asks: Vec<(price,size)>, time}
Candle: {symbol, exchange, time_start, time_end, open, high, low, close, volume, resolution}
```

EventHub (fan-out bus)
•	One global + many per-symbol topics per type (broadcast for stream + watch for snapshot).
•	publish_* from the client to fan out; subscribe_* from strategies/widgets.
•	Lets N consumers read the same feed without extra vendor connections.

MdBus (ref-counted subscription manager)
•	API: acquire(MarketDataRequest) -> SubGuard.
•	Tracks active vendor feeds keyed by {vendor, symbol, exchange, kinds, resolution, depth}.
•	First acquire → calls the provider’s subscribe() (now: RithmicApiClient::subscribe()).
Next acquires just bump a refcount. Last drop → unsubscribe() (optionally with a debounce).

This guarantees one Rithmic feed per unique request regardless of how many strategies/widgets are listening.

How they tie together with RithmicApiClient as provider

1) Inbound bytes → decode → map → publish

Inside RithmicApiClient you already read frames (template_id, bytes). For each frame:
•	match template_id → decode prost (LastTrade/BestBidOffer/OrderBook/TimeBar)
•	map to your core type (map_last_trade → Tick, etc.)
•	hub.publish_* the standardized object

That demux can live on an internal task started by the client (e.g., in new() or after login).

2) Subscriptions (MarketDataProvider impl on RithmicApiClient)
```rust
#[async_trait]
impl MarketDataProvider for RithmicApiClient {
    async fn subscribe(&self, req: MarketDataRequest) -> anyhow::Result<Arc<dyn SubscriptionHandle>> {
        // Translate `FeedKind | Resolution | depth` → Rithmic request templates:
        //   - TICKS/BBO/Stats → Market Data Update (100)
        //   - BOOK           → Snapshot (115) + Updates (117)
        //   - CANDLES        → TimeBar Update/Replay (200/202), based on Resolution
        // Send requests over your existing socket; return a handle that can later send STOPs if supported.
    }
}
```
MdBus calls this subscribe() on first demand; your client sends the right request templates.
When the last SubGuard drops, MdBus calls the handle’s unsubscribe(); the client sends the corresponding stop/unsubscribe messages (or just decrements local interest if Rithmic doesn’t have explicit stops).

3) Strategies / widgets
   •	Acquire feeds via MdBus (so they don’t force duplicate vendor subs).
   •	Subscribe to EventHub per symbol/type:
```rust
let guard = md_bus.acquire(MarketDataRequest { symbol, exchange, kinds: FeedKind::BBO|FeedKind::BOOK, book_depth: Some(10), resolution: None }).await?;
let rx_bbo  = hub.subscribe_bbo_symbol(&symbol).await;
let rx_book = hub.subscribe_book_symbol(&symbol).await;
```
Drop the guard when done; MdBus handles teardown via your client.

Mapping specifics (inside RithmicApiClient)
•	map_last_trade(LastTrade) -> Tick
•	aggressor: Option<i32> → Aggressor::{Buy,Sell}; set side from aggressor.
•	Time: ssboe/usecs → Utc.timestamp_opt(ssboe, usecs*1000).
•	map_bbo(BestBidOffer) -> Bbo
•	Prices f64 → Decimal, sizes i32 → Decimal.
•	map_order_book(OrderBook) -> OrderBook
•	Zip bid_price[] with bid_size[] and ask_*[] → (Decimal, Decimal) vectors.
•	Optionally keep a running ladder internally if you want always-full depth snapshots.
•	map_time_bar(TimeBar) -> Candle
•	type + period → Resolution::{Seconds(u8), Minutes(u8), Daily, Weekly}.
•	OHLC f64 → Decimal, volume u64 → Decimal.
•	time_start/time_end: placeholders until you expose proper fields.

TL;DR
•	RithmicApiClient is both the wire adapter and the MarketDataProvider.
•	It decodes prost messages, maps to your core types, and publishes into the EventHub.
•	MdBus sits in front to ensure one vendor sub per feed, ref-counted across all strategies/widgets.
•	Consumers just use EventHub receivers; they never touch vendor objects.

## Consolidators 
All live strategies and widgets can share a single consolidator for the same (symbol, resolution). 
MdBus dedupes the upstream vendor subscription; the DashMap registry dedupes the consolidator task; and the consolidator publishes to EventHub, which everyone subscribes to. 
One upstream feed, many downstream listeners. ✅
### Example
```rust
// 1) Ensure a 1-minute consolidator from ticks (shared if one exists)
let (tick_guard, rx_tick) = ctx.tick_feed(&sec).await?;
consolidators::spawn_ticks_to_timebar(
    Resolution::Minutes(1),
    rx_tick,
    sec.symbol().to_string(),
    format!("{:?}", sec.exchange),
    ctx.hub.clone(),
);

// 2) Roll up 1m candles → 5m candles (shared too)
let mut rx_1m = ctx.candle_rx(sec.symbol());
consolidators::spawn_candles_to_timebar(
    Resolution::Minutes(5),
    rx_1m,
    sec.symbol().to_string(),
    format!("{:?}", sec.exchange),
    ctx.hub.clone(),
);

// 3) Strategy subscribes to the target stream
let mut rx_5m = ctx.candle_rx(sec.symbol());
```
