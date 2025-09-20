use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};

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

