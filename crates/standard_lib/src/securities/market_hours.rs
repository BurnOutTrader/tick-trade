use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Tz;
use chrono_tz::US::Central;
use serde::{Deserialize, Serialize};
use tracing::warn;
use crate::market_data::base_data::Resolution;
use crate::securities::symbols::Exchange;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRule { pub days: [bool;7], pub open_ssm: u32, pub close_ssm: u32 }

#[derive(Debug, Clone)]
pub struct MarketHours {
    pub exchange: Exchange,
    pub tz: Tz,
    /// Primary/pit ("regular") trading sessions
    pub regular: Vec<SessionRule>,
    /// Electronic/overnight and other non-regular sessions
    pub extended: Vec<SessionRule>,
    pub holidays: Vec<chrono::NaiveDate>,
    pub has_daily_close: bool,
    pub has_weekend_close: bool,
}

/// Which session set to consult when querying hours
#[derive(Debug, Clone, Copy)]
pub enum SessionKind { Regular, Extended, Both }

impl MarketHours {
    #[inline]
    fn iter_rules<'a>(&'a self, kind: SessionKind) -> impl Iterator<Item = &'a SessionRule> + 'a {
        let reg = self.regular.iter();
        let ext = self.extended.iter();
        match kind {
            SessionKind::Regular => reg.take(usize::MAX).chain(ext.take(0)),
            SessionKind::Extended => reg.take(0).chain(ext.take(usize::MAX)),
            SessionKind::Both => reg.take(usize::MAX).chain(ext.take(usize::MAX)),
        }
    }

    /// True if **any** (regular or extended) session is open at `t`.
    pub fn is_open(&self, t: DateTime<Utc>) -> bool { self.is_open_with(t, SessionKind::Both) }

    /// True if a session of the requested kind is open at `t`.
    pub fn is_open_with(&self, t: DateTime<Utc>, kind: SessionKind) -> bool {
        let local = t.with_timezone(&self.tz);
        if self.holidays.iter().any(|d| *d == local.date_naive()) { return false; }
        let w = local.weekday().num_days_from_monday() as usize;
        let ssm = local.num_seconds_from_midnight();
        self.iter_rules(kind).any(|r| {
            if !r.days[w] { return false; }
            if r.open_ssm <= r.close_ssm { ssm >= r.open_ssm && ssm < r.close_ssm }
            else { ssm >= r.open_ssm || ssm < r.close_ssm } // wraps midnight
        })
    }

    /// Convenience wrappers
    pub fn is_open_regular(&self, t: DateTime<Utc>) -> bool { self.is_open_with(t, SessionKind::Regular) }
    pub fn is_open_extended(&self, t: DateTime<Utc>) -> bool { self.is_open_with(t, SessionKind::Extended) }
    /// Next bar end strictly after `now_utc`, skipping closed periods.
    pub fn next_bar_end(&self, now_utc: DateTime<Utc>, res: Resolution) -> DateTime<Utc> {
        match res {
            Resolution::Seconds(_) | Resolution::Minutes(_) | Resolution::Hours(_) => {
                let step = match res {
                    Resolution::Seconds(s) => s as i64,
                    Resolution::Minutes(m) => (m as i64) * 60,
                    Resolution::Hours(h)   => (h as i64) * 3600,
                    _ => unreachable!(),
                };
                let mut t = now_utc;
                loop {
                    let secs = t.timestamp();
                    let next = secs - (secs.rem_euclid(step)) + step;
                    let cand = Utc.timestamp_opt(next, 0).single().unwrap_or(t);
                    if self.is_open(cand) { return cand; }
                    // if closed at that boundary, jump to next session open
                    let (open, _close) = next_session_after(self, cand);
                    t = open;
                }
            }
            Resolution::Daily | Resolution::Weekly => {
                // Use session bounds so “daily” ends at session close, not midnight
                let (_open, close) = session_bounds(self, now_utc);
                close
            }
            _ => now_utc, // ticks/quote/tickbars: engine-specific
        }
    }

    pub fn is_maintenance(&self, t: DateTime<Utc>) -> bool {
        // “Maintenance” := closed now but next session is within ~90 minutes.
        if self.is_open(t) { return false; }
        let (open, _close) = next_session_after(self, t);
        (open - t) <= chrono::Duration::minutes(90)
    }
}

/// Compute the inclusive candle end timestamp for a given `time_open`, `resolution` and `exchange`.
/// The result is **one nanosecond before** the next bar's open (i.e., end-exclusive boundary minus 1ns),
/// so adjacent bars do not collide.
///
/// Rules:
/// - Intraday (Seconds/Minutes/Hours): use the exchange's trading hours to find the next bar boundary,
///   then subtract 1ns.
/// - Daily: if the exchange has a distinct daily close (`has_daily_close = true`), end at the *session close* - 1ns;
///   otherwise, end at the next local midnight - 1ns (in the exchange's timezone).
/// - Weekly: if the exchange has a distinct weekend close (`has_weekend_close = true`), end at the next
///   Monday 00:00 local - 1ns; otherwise, end at the next Sunday 17:00 local - 1ns (common futures reopen).
pub fn candle_end(time_open: DateTime<Utc>, resolution: Resolution, exchange: Exchange) -> Option<DateTime<Utc>> {
    let hours = hours_for_exchange(exchange);

    // helper: 1ns subtraction (avoid underflow)
    let minus_1ns = |t: DateTime<Utc>| {
        t.checked_sub_signed(Duration::nanoseconds(1)).unwrap_or(t)
    };

    match resolution {
        Resolution::Seconds(_) | Resolution::Minutes(_) | Resolution::Hours(_) => {
            // Use next bar boundary from market hours (skips closed periods)
            let end_exclusive = hours.next_bar_end(time_open, resolution);
            Some(minus_1ns(end_exclusive))
        }
        Resolution::Daily => {
            if hours.has_daily_close {
                let (_open, close) = session_bounds(&hours, time_open);
                Some(minus_1ns(close))
            } else {
                // No explicit daily close → use next local midnight in the exchange TZ
                let local = time_open.with_timezone(&hours.tz);
                let tomorrow = local.date_naive() + Duration::days(1);
                let next_midnight_local = hours.tz
                    .with_ymd_and_hms(tomorrow.year(), tomorrow.month(), tomorrow.day(), 0, 0, 0)
                    .earliest()
                    .unwrap()
                    .with_timezone(&Utc);
                Some(minus_1ns(next_midnight_local))
            }
        }
        Resolution::Weekly => {
            use chrono::Weekday;
            let local = time_open.with_timezone(&hours.tz);

            // Find the next Monday 00:00 local
            let mut d = local.date_naive();
            let mut dow = local.weekday();
            while dow != Weekday::Mon {
                d = d + Duration::days(1);
                dow = d.weekday();
            }
            let next_monday_00_local = hours.tz
                .with_ymd_and_hms(d.year(), d.month(), d.day(), 0, 0, 0)
                .earliest()
                .unwrap()
                .with_timezone(&Utc);

            if hours.has_weekend_close {
                Some(minus_1ns(next_monday_00_local))
            } else {
                // Continuous products without a true weekend close:
                // Use “Sunday night” as the weekly boundary (commonly 17:00 local on many futures venues).
                // If the computed Monday is today (already Monday), move back to the prior week’s Sunday.
                let mut d_sun = d;
                // step back to Sunday corresponding to that Monday
                d_sun = d_sun - Duration::days(1);
                let sunday_17_local = hours.tz
                    .with_ymd_and_hms(d_sun.year(), d_sun.month(), d_sun.day(), 17, 0, 0)
                    .earliest()
                    .unwrap()
                    .with_timezone(&Utc);
                Some(minus_1ns(sunday_17_local))
            }
        }
        Resolution::TickBars(_) => None,
        _ => None,
    }
}

/// Back-compat shim: old name pointing to `candle_end`.
#[inline]
pub fn time_end_of_day(time_open: DateTime<Utc>, resolution: Resolution, exchange: Exchange) -> Option<DateTime<Utc>> {
    candle_end(time_open, resolution, exchange)
}

/// Build default futures trading hours per exchange.
/// NOTE: These are exchange-level defaults. Product-level variations may differ.
pub fn hours_for_exchange(exch: Exchange) -> MarketHours {
    match exch {
        // CME equity-index style (Globex nearly 24x5 with maintenance). Keep existing helper.
        Exchange::CME => {
            MarketHours {
                exchange: Exchange::CME,
                tz: Central,
                // CME equity index regular day session ~08:30–15:15 CT (simplified)
                regular: vec![ SessionRule { days: [false,true,true,true,true,true,false], open_ssm: 8*3600 + 30*60, close_ssm: 15*3600 + 15*60 } ],
                // Extended Globex windows: 17:00–08:30 and 15:30–16:00 (maintenance 16:00–17:00 not modeled as open)
                extended: vec![
                    SessionRule { days: [true,true,true,true,true,true,false],  open_ssm: 17*3600, close_ssm: 8*3600 + 30*60 },
                    SessionRule { days: [false,true,true,true,true,true,false], open_ssm: 15*3600 + 30*60, close_ssm: 16*3600 },
                ],
                holidays: vec![],
                has_daily_close: true,
                has_weekend_close: true
            }
        }

        // CBOT (Ags): Globex split session commonly referenced as
        // Sun–Fri 19:00–07:45 CT and Mon–Fri 08:30–13:20 CT
        Exchange::CBOT => MarketHours {
            exchange: Exchange::CBOT,
            tz: chrono_tz::US::Central,
            // Day session (regular)
            regular: vec![ SessionRule { days: [false, true, true, true, true, true, false], open_ssm: 8*3600 + 30*60, close_ssm: 13*3600 + 20*60 } ],
            // Globex overnight (extended)
            extended: vec![ SessionRule { days: [true, true, true, true, true, true, false], open_ssm: 19*3600, close_ssm: 7*3600 + 45*60 } ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // COMEX: map to CME Globex-style continuous with daily break
        Exchange::COMEX => MarketHours {
            exchange: Exchange::COMEX,
            tz: chrono_tz::US::Central,
            regular: vec![],
            extended: vec![
                // Simple model: 17:00 – 16:00 CT with 60m break (implicit via next-day open)
                SessionRule { days: [true, true, true, true, true, true, false], open_ssm: 17*3600, close_ssm: 16*3600 },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // NYMEX: similar Globex profile
        Exchange::NYMEX => MarketHours {
            exchange: Exchange::NYMEX,
            tz: chrono_tz::US::Central,
            regular: vec![],
            extended: vec![ SessionRule { days: [true, true, true, true, true, true, false], open_ssm: 17*3600, close_ssm: 16*3600 } ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // EUREX: Core trading ~01:10–22:00 CET (simplified window)
        Exchange::EUREX => MarketHours {
            exchange: Exchange::EUREX,
            tz: chrono_tz::Europe::Berlin,
            regular: vec![ SessionRule { days: [false, true, true, true, true, true, false], open_ssm: 1*3600 + 10*60, close_ssm: 22*3600 } ],
            extended: vec![],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // ICE Futures U.S.: simplified 20:00–18:00 ET (varies by product)
        Exchange::ICEUS => MarketHours {
            exchange: Exchange::ICEUS,
            tz: chrono_tz::America::New_York,
            regular: vec![ SessionRule { days: [true, true, true, true, true, true, false], open_ssm: 20*3600, close_ssm: 18*3600 } ],
            extended: vec![],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // ICE Futures Europe: simplified 01:00–23:00 London time
        Exchange::ICEEU => MarketHours {
            exchange: Exchange::ICEEU,
            tz: chrono_tz::Europe::London,
            regular: vec![ SessionRule { days: [false, true, true, true, true, true, false], open_ssm: 1*3600, close_ssm: 23*3600 } ],
            extended: vec![],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // SGX Derivatives: simplified two blocks (T session 08:15–18:00 SGT; T+1 18:40–05:15)
        Exchange::SGX => MarketHours {
            exchange: Exchange::SGX,
            tz: chrono_tz::Asia::Singapore,
            regular: vec![
                SessionRule { days: [false, true, true, true, true, true, false], open_ssm: 8*3600 + 15*60, close_ssm: 18*3600 },
            ],
            extended: vec![
                // T+1 spans midnight; model as two rules: 18:40–24:00 and 00:00–05:15 next day
                SessionRule { days: [false, true, true, true, true, true, false], open_ssm: 18*3600 + 40*60, close_ssm: 24*3600 },
                SessionRule { days: [true, true, true, true, true, true, false],  open_ssm: 0, close_ssm: 5*3600 + 15*60 },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // CFE (Cboe Futures – e.g., VIX): simplified 17:00–8:30 & 8:30–15:15 CT
        Exchange::CFE => MarketHours {
            exchange: Exchange::CFE,
            tz: chrono_tz::US::Central,
            regular: vec![
                SessionRule { days: [false, true, true, true, true, true, false], open_ssm: 8*3600 + 30*60, close_ssm: 15*3600 + 15*60 },
            ],
            extended: vec![
                SessionRule { days: [true, true, true, true, true, true, false], open_ssm: 17*3600, close_ssm: 8*3600 + 30*60 },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true
        },

        // Fallback: 24x5 in UTC
        _ => {
            warn!("No market hours found for : {}", exch);
            MarketHours {
                exchange: exch,
                tz: chrono_tz::UTC,
                regular: vec![ SessionRule { days: [true, true, true, true, true, true, true], open_ssm: 0, close_ssm: 24*3600 } ],
                extended: vec![],
                holidays: vec![],
                has_daily_close: true,
                has_weekend_close: true
            }
        },
    }
}


// ---------------------------
// Session helpers (public API)
// ---------------------------

/// Return the trading session [open, close) that **contains** `t` (if any) for the given kind.
/// If `t` is outside any session (e.g., weekend/holiday), this falls back to:
/// 1) previous day's wrap session (if it spans into today), else
/// 2) the next valid session after `t`.
pub fn session_bounds_with(kind: SessionKind, hours: &MarketHours, t: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let local = t.with_timezone(&hours.tz);
    let day   = local.date_naive();
    let ssm   = local.num_seconds_from_midnight() as i64;

    // Try today's rule first across the chosen rule set(s)
    if let Some(r) = rule_for_date_in(hours, day, kind) {
        if r.open_ssm as i64 <= r.close_ssm as i64 {
            // non-wrap
            if ssm >= r.open_ssm as i64 && ssm < r.close_ssm as i64 {
                let open  = mk_local(hours.tz, day, r.open_ssm);
                let close = mk_local(hours.tz, day, r.close_ssm);
                return (open.with_timezone(&Utc), close.with_timezone(&Utc));
            }
        } else {
            // wrap (open today, close tomorrow) or (open yesterday, close today)
            if ssm >= r.open_ssm as i64 {
                // opened today, closes tomorrow
                let open  = mk_local(hours.tz, day, r.open_ssm);
                let close = mk_local(hours.tz, day + Duration::days(1), r.close_ssm);
                return (open.with_timezone(&Utc), close.with_timezone(&Utc));
            }
            if ssm < r.close_ssm as i64 {
                // opened yesterday, closes today
                if let Some(r_y) = rule_for_date_in(hours, day - Duration::days(1), kind) {
                    if r_y.open_ssm > r_y.close_ssm {
                        let open  = mk_local(hours.tz, day - Duration::days(1), r_y.open_ssm);
                        let close = mk_local(hours.tz, day, r_y.close_ssm);
                        return (open.with_timezone(&Utc), close.with_timezone(&Utc));
                    }
                }
            }
        }
    }

    // Fallback 1: yesterday wrap that spans into today
    if let Some(r_y) = rule_for_date_in(hours, day - Duration::days(1), kind) {
        if r_y.open_ssm > r_y.close_ssm {
            let before_close_today = ssm < r_y.close_ssm as i64;
            if before_close_today {
                let open  = mk_local(hours.tz, day - Duration::days(1), r_y.open_ssm);
                let close = mk_local(hours.tz, day, r_y.close_ssm);
                return (open.with_timezone(&Utc), close.with_timezone(&Utc));
            }
        }
    }

    // Fallback 2: pick the next session after `t`
    next_session_after_with(kind, hours, t)
}

/// Backwards-compatible wrapper using Both (regular+extended)
pub fn session_bounds(hours: &MarketHours, t: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    session_bounds_with(SessionKind::Both, hours, t)
}

/// Next session bounds strictly after `end_excl` (searches forward, skipping holidays).
pub fn next_session_after_with(kind: SessionKind, hours: &MarketHours, end_excl: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let local = end_excl.with_timezone(&hours.tz);
    let base_day = local.date_naive();
    let ssm_now  = local.num_seconds_from_midnight() as i64;

    for dd in 0..14 {
        let d = base_day + Duration::days(dd);
        if is_holiday(hours, d) { continue; }
        if let Some(r) = rule_for_date_in(hours, d, kind) {
            if r.open_ssm as i64 <= r.close_ssm as i64 {
                if dd > 0 || ssm_now < r.open_ssm as i64 {
                    let open  = mk_local(hours.tz, d, r.open_ssm);
                    let close = mk_local(hours.tz, d, r.close_ssm);
                    return (open.with_timezone(&Utc), close.with_timezone(&Utc));
                }
            } else {
                if dd > 0 || ssm_now < r.open_ssm as i64 {
                    let open  = mk_local(hours.tz, d, r.open_ssm);
                    let close = mk_local(hours.tz, d + Duration::days(1), r.close_ssm);
                    return (open.with_timezone(&Utc), close.with_timezone(&Utc));
                }
                continue;
            }
        }
    }
    (end_excl, end_excl)
}

pub fn next_session_after(hours: &MarketHours, end_excl: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    next_session_after_with(SessionKind::Both, hours, end_excl)
}

// ---------------------------
// Internals
// ---------------------------

fn is_holiday(hours: &MarketHours, d: chrono::NaiveDate) -> bool {
    hours.holidays.iter().any(|h| *h == d)
}

fn rule_for_date_in<'a>(hours: &'a MarketHours, d: chrono::NaiveDate, kind: SessionKind) -> Option<&'a SessionRule> {
    if is_holiday(hours, d) { return None; }
    let w = d.weekday().num_days_from_monday() as usize;
    hours.iter_rules(kind).find(|r| r.days[w])
}

fn mk_local(tz: Tz, day: chrono::NaiveDate, ssm: u32) -> chrono::DateTime<Tz> {
    let base: NaiveDateTime = day.and_hms_opt(0,0,0).unwrap() + Duration::seconds(ssm as i64);
    // Resolve TZ (handle DST transitions); prefer `.single()` and fallback safely
    tz.from_local_datetime(&base).single().unwrap_or_else(|| tz.from_utc_datetime(&base))
}

#[inline]
pub fn next_session_open_after(mh: &MarketHours, after_utc: DateTime<Utc>) -> DateTime<Utc> {
    // Search up to 14 days ahead (safety bound)
    let tz: Tz = mh.tz;
    let after_local = after_utc.with_timezone(&tz);

    // quick holiday check
    let is_holiday = |d: NaiveDate| mh.holidays.iter().any(|h| *h == d);

    // combine regular + extended for “is open” calendar
    let mut rules: Vec<&SessionRule> = Vec::new();
    rules.extend(mh.regular.iter());
    rules.extend(mh.extended.iter());

    // Walk day-by-day until we find the first session whose OPEN is after `after_local`
    for day_offset in 0..14 {
        let day_local = after_local.date_naive() + chrono::Days::new(day_offset);
        if is_holiday(day_local) {
            continue;
        }

        // Which weekday is this?
        let wd: Weekday = tz
            .with_ymd_and_hms(day_local.year(), day_local.month(), day_local.day(), 0, 0, 0)
            .single()
            .expect("valid local midnight")
            .weekday();
        let wd_idx = wd.num_days_from_sunday() as usize; // 0=Sun..6=Sat

        // Evaluate all rules for this day; pick the earliest open strictly after `after_local`
        let mut candidate: Option<DateTime<Tz>> = None;

        for r in &rules {
            if !r.days[wd_idx] { continue; }

            // Construct local open time
            let open_ssm = r.open_ssm.max(0).min(24*3600);
            let open_h = (open_ssm / 3600) as u32;
            let open_m = ((open_ssm % 3600) / 60) as u32;
            let open_s = (open_ssm % 60) as u32;

            let open_local = tz
                .with_ymd_and_hms(day_local.year(), day_local.month(), day_local.day(), open_h, open_m, open_s)
                .single();
            let Some(open_dt) = open_local else { continue }; // skip ambiguous/skipped local-midnight edges

            // Only accept opens strictly after our local time reference
            if open_dt <= after_local { continue; }

            candidate = match candidate {
                None => Some(open_dt),
                Some(best) => Some(best.min(open_dt)),
            };
        }

        if let Some(open_dt) = candidate {
            return open_dt.with_timezone(&Utc);
        }
    }

    // Fallback: if we didn’t find anything (odd calendar), nudge by 1 day in UTC
    after_utc + Duration::days(1)
}