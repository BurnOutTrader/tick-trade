use std::sync::Arc;
use chrono::{DateTime, Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use crate::engine_core::event_hub::EventHub;
use crate::market_data::base_data::{Side, Bbo, Candle, Resolution, Tick};
use crate::securities::market_hours::{next_session_after, session_bounds, MarketHours};
use crate::securities::symbols::Exchange;
//todo! we need to consider has_daily_close and has weekend_close for daily and weekly candles, in the event we add forex or crypto markets

// ========================================================
// BBO -> Candles
// ========================================================
#[allow(dead_code, unused_assignments)]
pub fn spawn_bbo_to_candles(
    res: Resolution,
    mut rx_bbo: tokio::sync::broadcast::Receiver<Bbo>,
    out_symbol: String,
    out_exchange: Exchange,
    hub: Arc<EventHub>,
    hours: Option<Arc<MarketHours>>,   // <-- NEW
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut o=None; let mut h=None; let mut l=None; let mut c=None; let mut v = Decimal::ZERO;

        // timebox state (either fixed window or session)
        enum Win { Fixed{ start:DateTime<Utc>, end:DateTime<Utc>, len:Duration }, Session{ open:DateTime<Utc>, close:DateTime<Utc> } }
        let mut win: Option<Win> = None;

        // helpers
        let fixed_len = |r: &Resolution| -> Duration {
            match r {
                Resolution::Seconds(s) => Duration::seconds(*s as i64),
                Resolution::Minutes(m) => Duration::minutes(*m as i64),
                Resolution::Hours(h)   => Duration::hours(*h as i64),
                _ => Duration::seconds(1),
            }
        };
        let init_win = |t: DateTime<Utc>, res: &Resolution, hours: &Option<Arc<MarketHours>>| -> Win {
            match res {
                Resolution::Daily | Resolution::Weekly => {
                    let mh = hours.as_ref().expect("Daily/Weekly requires MarketHours");
                    let (open, close) = session_bounds(mh, t);
                    Win::Session{ open, close }
                }
                _ => {
                    let len = fixed_len(res);
                    // floor to boundary
                    let (start, end) = {
                        match res {
                            Resolution::Seconds(s) => {
                                let step = *s as i64;
                                let floored = t.timestamp() - t.timestamp().rem_euclid(step);
                                let s = Utc.timestamp_opt(floored, 0).unwrap();
                                (s, s + len)
                            }
                            Resolution::Minutes(m) => {
                                let step = (*m as i64) * 60;
                                let floored = t.timestamp() - t.timestamp().rem_euclid(step);
                                let s = Utc.timestamp_opt(floored, 0).unwrap();
                                (s, s + len)
                            }
                            Resolution::Hours(h) => {
                                let step = (*h as i64) * 3600;
                                let floored = t.timestamp() - t.timestamp().rem_euclid(step);
                                let s = Utc.timestamp_opt(floored, 0).unwrap();
                                (s, s + len)
                            }
                            _ => (t, t + len),
                        }
                    };
                    Win::Fixed { start, end, len }
                }
            }
        };
        let inside = |t: DateTime<Utc>, w: &Win| -> bool {
            match w {
                Win::Fixed {  end, .. } => t < *end,
                Win::Session { open, close }  => t < *close && t >= *open,
            }
        };
        let advance = |t_last: DateTime<Utc>, w: &Win, _res:&Resolution, hours:&Option<Arc<MarketHours>>| -> Win {
            match w {
                Win::Fixed { start:_, end, len } => {
                    // hop in multiples of len until t_last < new_end
                    let mut s = *end;
                    while t_last >= s { s = s + *len; }
                    Win::Fixed{ start: s - *len, end: s, len: *len }
                }
                Win::Session { open:_, close } => {
                    let mh = hours.as_ref().expect("session advance needs MarketHours");
                    let (nopen, nclose) = next_session_after(mh, *close);
                    Win::Session{ open: nopen, close: nclose }
                }
            }
        };

        while let Ok(bbo) = rx_bbo.recv().await {
            let t = bbo.time;
            if win.is_none() { win = Some(init_win(t, &res, &hours)); }
            // roll window forward if needed
            while !inside(t, win.as_ref().unwrap()) {
                // flush if we have content
                if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (o,h,l,c) {
                    let (start, end) = match win.as_ref().unwrap() {
                        Win::Fixed{ start, end, .. } => (*start, *end),
                        Win::Session{ open, close }  => (*open, *close),
                    };
                    let end_inclusive = end - Duration::nanoseconds(1);

                    hub.publish_candle(Candle {
                        symbol: out_symbol.clone(), exchange: out_exchange.clone(),
                        time_start: start, time_end: end_inclusive,
                        open: oo, high: hh, low: ll, close: cc,
                        volume: v,
                        ask_volume: dec!(0),
                        bid_volume: dec!(0),
                        num_of_trades: dec!(0),
                        resolution: res.clone(),
                    });
                }
                // reset and advance
                o=None; h=None; l=None; c=None; v=Decimal::ZERO;
                let cur = win.take().unwrap();
                win = Some(advance(t, &cur, &res, &hours));
            }

            // update OHLC
            let mid = (bbo.bid + bbo.ask) / Decimal::from(2);
            if o.is_none() { o = Some(mid); }
            h = Some(h.map_or(mid, |x| std::cmp::max(x, mid)));
            l = Some(l.map_or(mid, |x| std::cmp::min(x, mid)));
            c = Some(mid);
            // v: from BBO we don’t know prints; keep as ZERO
        }
    })
}

// ========================================================
// Ticks -> time-based Candles (Seconds/Minutes/Hours/Daily/Weekly)
// ========================================================
#[allow(dead_code, unused_assignments)]
pub fn spawn_ticks_to_candles(
    res: Resolution,
    mut rx_tick: tokio::sync::broadcast::Receiver<Tick>,
    out_symbol: String,
    out_exchange: Exchange,
    hub: Arc<EventHub>,
    hours: Option<Arc<MarketHours>>,   // <-- NEW
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut o=None; let mut h=None; let mut l=None; let mut c=None;
        let mut vol = Decimal::ZERO;
        let mut bid_vol = Decimal::ZERO;
        let mut ask_vol = Decimal::ZERO;
        let mut trades  = Decimal::ZERO;

        enum Win { Fixed{ start:DateTime<Utc>, end:DateTime<Utc>, len:Duration }, Session{ open:DateTime<Utc>, close:DateTime<Utc> } }
        let mut win: Option<Win> = None;

        let fixed_len = |r: &Resolution| -> Duration {
            match r {
                Resolution::Seconds(s) => Duration::seconds(*s as i64),
                Resolution::Minutes(m) => Duration::minutes(*m as i64),
                Resolution::Hours(h)   => Duration::hours(*h as i64),
                _ => Duration::seconds(1),
            }
        };
        let init_win = |t: DateTime<Utc>, res: &Resolution, hours: &Option<Arc<MarketHours>>| -> Win {
            match res {
                Resolution::Daily | Resolution::Weekly => {
                    let mh = hours.as_ref().expect("Daily/Weekly requires MarketHours");
                    let (open, close) = session_bounds(mh, t);
                    Win::Session{ open, close }
                }
                _ => {
                    let len = fixed_len(res);
                    let (start, end) = {
                        match res {
                            Resolution::Seconds(s) => {
                                let step = *s as i64;
                                let floored = t.timestamp() - t.timestamp().rem_euclid(step);
                                let s = Utc.timestamp_opt(floored, 0).unwrap();
                                (s, s + len)
                            }
                            Resolution::Minutes(m) => {
                                let step = (*m as i64) * 60;
                                let floored = t.timestamp() - t.timestamp().rem_euclid(step);
                                let s = Utc.timestamp_opt(floored, 0).unwrap();
                                (s, s + len)
                            }
                            Resolution::Hours(h) => {
                                let step = (*h as i64) * 3600;
                                let floored = t.timestamp() - t.timestamp().rem_euclid(step);
                                let s = Utc.timestamp_opt(floored, 0).unwrap();
                                (s, s + len)
                            }
                            _ => (t, t + len),
                        }
                    };
                    Win::Fixed { start, end, len }
                }
            }
        };
        let inside = |t: DateTime<Utc>, w: &Win| -> bool {
            match w {
                Win::Fixed { start:_, end, .. } => t < *end,
                Win::Session { open, close }  => t < *close && t >= *open,
            }
        };
        let advance = |t_last: DateTime<Utc>, w: &Win, _res:&Resolution, hours:&Option<Arc<MarketHours>>| -> Win {
            match w {
                Win::Fixed { start:_, end, len } => {
                    let mut s = *end;
                    while t_last >= s { s = s + *len; }
                    Win::Fixed{ start: s - *len, end: s, len: *len }
                }
                Win::Session { open:_, close } => {
                    let mh = hours.as_ref().expect("session advance needs MarketHours");
                    let (nopen, nclose) = next_session_after(mh, *close);
                    Win::Session{ open: nopen, close: nclose }
                }
            }
        };

        while let Ok(tk) = rx_tick.recv().await {
            let t = tk.time;
            if win.is_none() { win = Some(init_win(t, &res, &hours)); }
            while !inside(t, win.as_ref().unwrap()) {
                if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (o,h,l,c) {
                    let (start, end) = match win.as_ref().unwrap() {
                        Win::Fixed{ start, end, .. } => (*start, *end),
                        Win::Session{ open, close }  => (*open, *close),
                    };
                    let end_inclusive = end - Duration::nanoseconds(1);

                    hub.publish_candle(Candle {
                        symbol: out_symbol.clone(),
                        exchange: out_exchange.clone(),
                        time_start: start, time_end: end_inclusive,
                        open: oo, high: hh, low: ll, close: cc,
                        volume: vol,
                        ask_volume: ask_vol,
                        bid_volume: bid_vol,
                        num_of_trades: trades,
                        resolution: res.clone(),
                    });
                }
                // reset + advance
                o=None; h=None; l=None; c=None;
                vol=Decimal::ZERO; bid_vol=Decimal::ZERO; ask_vol=Decimal::ZERO; trades=Decimal::ZERO;
                let cur = win.take().unwrap();
                win = Some(advance(t, &cur, &res, &hours));
            }

            // update OHLC
            if o.is_none() { o = Some(tk.price); }
            h = Some(h.map_or(tk.price, |x| std::cmp::max(x, tk.price)));
            l = Some(l.map_or(tk.price, |x| std::cmp::min(x, tk.price)));
            c = Some(tk.price);

            // accum
            vol += tk.size;
            trades += dec!(1);
            match tk.side {
                Side::Buy  => ask_vol += tk.size,
                Side::Sell => bid_vol += tk.size,
                Side::None => {}
            }
        }
    })
}

// ========================================================
// Ticks -> N-Tick Candles
// ========================================================
#[allow(dead_code, unused_assignments)]
pub fn spawn_ticks_to_tick_candles(
    ticks_per_bar: u32,
    mut rx_tick: tokio::sync::broadcast::Receiver<Tick>,
    out_symbol: String,
    out_exchange: Exchange,
    hub: Arc<EventHub>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut count: u32 = 0;
        let mut start_ts: Option<DateTime<Utc>> = None;
        let mut o=None; let mut h=None; let mut l=None; let mut c=None;
        let mut vol = Decimal::ZERO;
        let mut bid_vol = Decimal::ZERO;
        let mut ask_vol = Decimal::ZERO;
        let mut trades  = Decimal::ZERO;

        while let Ok(tk) = rx_tick.recv().await {
            if start_ts.is_none() { start_ts = Some(tk.time); }
            if o.is_none() { o = Some(tk.price); }
            h = Some(h.map_or(tk.price, |x| std::cmp::max(x, tk.price)));
            l = Some(l.map_or(tk.price, |x| std::cmp::min(x, tk.price)));
            c = Some(tk.price);

            vol += tk.size;
            trades += dec!(1);
            match tk.side {
                Side::Buy  => ask_vol += tk.size,
                Side::Sell => bid_vol += tk.size,
                Side::None => {}
            }

            count += 1;
            if count >= ticks_per_bar {
                let start = start_ts.unwrap();
                let end = tk.time;
                let end_inclusive = end - Duration::nanoseconds(1);
                if let (Some(oo), Some(hh), Some(ll), Some(cc)) = (o,h,l,c) {

                    hub.publish_candle(Candle {
                        symbol: out_symbol.clone(),
                        exchange: out_exchange.clone(),
                        time_start: start,
                        time_end: end_inclusive,
                        open: oo, high: hh, low: ll, close: cc,
                        volume: vol,
                        ask_volume: ask_vol,
                        bid_volume: bid_vol,
                        num_of_trades: trades,
                        resolution: Resolution::TickBars(ticks_per_bar),
                    });
                }
                count = 0; start_ts=None; o=None; h=None; l=None; c=None;
                vol=Decimal::ZERO; bid_vol=Decimal::ZERO; ask_vol=Decimal::ZERO; trades=Decimal::ZERO;
            }
        }
    })
}

// ========================================================
// Candles -> higher-TF Candles (1m→5m / 1h / Daily / Weekly)
// ========================================================
pub fn spawn_candles_to_candles(
    dst: Resolution,
    mut rx_candle: tokio::sync::broadcast::Receiver<Candle>,
    out_symbol: String,
    out_exchange: Exchange,
    hub: Arc<EventHub>,
    hours: Option<Arc<MarketHours>>,   // <-- NEW
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut b_start: Option<DateTime<Utc>> = None;
        let mut b_end:   Option<DateTime<Utc>> = None;
        let mut o=None; let mut h=None; let mut l=None; let mut c=None;
        let mut vol = Decimal::ZERO;
        let mut bid_vol = Decimal::ZERO;
        let mut ask_vol = Decimal::ZERO;
        let mut trades  = Decimal::ZERO;

        enum Win { Fixed{ start:DateTime<Utc>, end:DateTime<Utc>, len:Duration }, Session{ open:DateTime<Utc>, close:DateTime<Utc> } }
        let mut win: Option<Win> = None;

        let fixed_len = |r: &Resolution| -> Duration {
            match r {
                Resolution::Seconds(s) => Duration::seconds(*s as i64),
                Resolution::Minutes(m) => Duration::minutes(*m as i64),
                Resolution::Hours(h)   => Duration::hours(*h as i64),
                _ => Duration::seconds(1),
            }
        };
        let floor_fixed = |ts: DateTime<Utc>, r: &Resolution| -> DateTime<Utc> {
            match r {
                Resolution::Seconds(s) => {
                    let step = *s as i64;
                    let floored = ts.timestamp() - ts.timestamp().rem_euclid(step);
                    Utc.timestamp_opt(floored, 0).unwrap()
                }
                Resolution::Minutes(m) => {
                    let step = (*m as i64) * 60;
                    let floored = ts.timestamp() - ts.timestamp().rem_euclid(step);
                    Utc.timestamp_opt(floored, 0).unwrap()
                }
                Resolution::Hours(h) => {
                    let step = (*h as i64) * 3600;
                    let floored = ts.timestamp() - ts.timestamp().rem_euclid(step);
                    Utc.timestamp_opt(floored, 0).unwrap()
                }
                _ => ts,
            }
        };
        let init_win = |t: DateTime<Utc>, res: &Resolution, hours: &Option<Arc<MarketHours>>| -> Win {
            match res {
                Resolution::Daily | Resolution::Weekly => {
                    let mh = hours.as_ref().expect("Daily/Weekly requires MarketHours");
                    let (open, close) = session_bounds(mh, t);
                    Win::Session{ open, close }
                }
                _ => {
                    let len = fixed_len(res);
                    let start = floor_fixed(t, res);
                    Win::Fixed { start, end: start + len, len }
                }
            }
        };
        let contains = |ts: DateTime<Utc>, w: &Win| -> bool {
            match w {
                Win::Fixed{ start, end, .. } => ts >= *start && ts < *end,
                Win::Session{ open, close }  => ts >= *open && ts < *close,
            }
        };
        let advance = |t_last: DateTime<Utc>, w: &Win, _res:&Resolution, hours:&Option<Arc<MarketHours>>| -> Win {
            match w {
                Win::Fixed{ start:_, end, len } => {
                    let mut s = *end;
                    while t_last >= s { s = s + *len; }
                    Win::Fixed{ start: s - *len, end: s, len:*len }
                }
                Win::Session{ open:_, close } => {
                    let mh = hours.as_ref().expect("session advance needs MarketHours");
                    let (nopen, nclose) = next_session_after(mh, *close);
                    Win::Session{ open:nopen, close:nclose }
                }
            }
        };

        while let Ok(bar) = rx_candle.recv().await {
            // use bar.start to place it
            let ts = bar.time_start;
            if win.is_none() { win = Some(init_win(ts, &dst, &hours)); }
            while !contains(ts, win.as_ref().unwrap()) {
                if let (Some(bs), Some(be), Some(oo), Some(hh), Some(ll), Some(cc)) = (b_start, b_end, o,h,l,c) {
                    let be_inclusive = be - Duration::nanoseconds(1);

                    hub.publish_candle(Candle {
                        symbol: out_symbol.clone(),
                        exchange: out_exchange.clone(),
                        time_start: bs, time_end: be_inclusive,
                        open: oo, high: hh, low: ll, close: cc,
                        volume: vol,
                        ask_volume: ask_vol,
                        bid_volume: bid_vol,
                        num_of_trades: trades,
                        resolution: dst.clone(),
                    });
                }
                // reset and advance
                b_start=None; b_end=None; o=None; h=None; l=None; c=None;
                vol=Decimal::ZERO; ask_vol=Decimal::ZERO; bid_vol=Decimal::ZERO; trades=Decimal::ZERO;
                let cur = win.take().unwrap();
                win = Some(advance(ts, &cur, &dst, &hours));
            }

            // init bucket meta
            let (w_start, w_end) = match win.as_ref().unwrap() {
                Win::Fixed{ start, end, .. } => (*start, *end),
                Win::Session{ open, close }  => (*open, *close),
            };
            if b_start.is_none() { b_start = Some(w_start); }
            b_end = Some(w_end.max(bar.time_end));

            // roll-up aggregates
            if o.is_none() { o = Some(bar.open); }
            h = Some(h.map_or(bar.high, |x| std::cmp::max(x, bar.high)));
            l = Some(l.map_or(bar.low,  |x| std::cmp::min(x, bar.low)));
            c = Some(bar.close);
            vol      += bar.volume;
            ask_vol  += bar.ask_volume;
            bid_vol  += bar.bid_volume;
            trades   += bar.num_of_trades;
        }

        // flush last
        if let (Some(bs), Some(be), Some(oo), Some(hh), Some(ll), Some(cc)) = (b_start, b_end, o,h,l,c) {
            let be_inclusive = be - Duration::nanoseconds(1);
            hub.publish_candle(Candle {
                symbol: out_symbol,
                exchange: out_exchange,
                time_start: bs, time_end: be_inclusive,
                open: oo, high: hh, low: ll, close: cc,
                volume: vol,
                ask_volume: ask_vol,
                bid_volume: bid_vol,
                num_of_trades: trades,
                resolution: dst,
            });
        }
    })
}