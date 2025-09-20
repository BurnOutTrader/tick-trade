use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};
use bytes::Bytes;

pub(crate) fn is_weekend_or_off_hours(chicago_time: DateTime<chrono_tz::Tz>) -> bool {
    let weekday = chicago_time.weekday();
    let hour = chicago_time.hour();

    // Market closes Friday at 5 PM and reopens Sunday at 5 PM (Chicago time).
    let after_friday_close = weekday == Weekday::Fri && hour >= 17;
    let before_sunday_open = weekday == Weekday::Sun && hour < 16;
    let is_weekend = weekday == Weekday::Sat;

    after_friday_close || before_sunday_open || is_weekend
}

pub(crate) fn next_chicago_market_open(chicago_time: DateTime<chrono_tz::Tz>) -> DateTime<Utc> {
    let mut next_open = chicago_time.date_naive().and_hms_opt(17, 0, 0).unwrap(); // 5 PM

    // If it's Friday or Saturday, the next open is Sunday 5 PM.
    match chicago_time.weekday() {
        Weekday::Fri | Weekday::Sat => {
            next_open += chrono::Duration::days((7 - chicago_time.weekday().num_days_from_monday()) as i64 + 1);
        }
        _ => {
            // Otherwise, market opens the next day at 5 PM.
            next_open += chrono::Duration::days(1);
        }
    }

    let tz = chrono_tz::America::Chicago;
    let next_open = tz.from_local_datetime(&next_open).unwrap();
    next_open.to_utc() // Convert to UTC for consistent time comparison
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
pub(crate) fn fast_detect_final(payload: &bytes::Bytes) -> Option<bool> {
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
pub(crate) fn fast_extract_cid(payload: &Bytes) -> Option<u64> {
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
pub(crate) fn fast_extract_template_id(payload: &bytes::Bytes) -> Option<i32> {
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