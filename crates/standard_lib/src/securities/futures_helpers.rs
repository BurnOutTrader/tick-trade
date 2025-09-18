use std::borrow::Cow;
use chrono::{Datelike, NaiveDate};

#[inline]
fn sanitize_code(s: &str) -> Cow<'_, str> {
    if s.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Cow::Borrowed(s)
    }
    Cow::Owned(s.chars().filter(|c| c.is_ascii_alphanumeric()).collect())
}

#[inline]
fn month_from_code(c: char) -> Option<u32> {
    Some(match c {
        'F' => 1,  // Jan
        'G' => 2,  // Feb
        'H' => 3,  // Mar
        'J' => 4,  // Apr
        'K' => 5,  // May
        'M' => 6,  // Jun
        'N' => 7,  // Jul
        'Q' => 8,  // Aug
        'U' => 9,  // Sep
        'V' => 10, // Oct
        'X' => 11, // Nov
        'Z' => 12, // Dec
        _ => return None,
    })
}

pub fn parse_expiry_from_contract_code(code: &str) -> Option<NaiveDate> {
    let s = sanitize_code(code).to_ascii_uppercase();
    let bytes = s.as_bytes();

    // 1) Collect 1–2 trailing digits as the year
    let mut i = bytes.len();
    let mut year_digits = 0usize;
    while i > 0 && bytes[i - 1].is_ascii_digit() && year_digits < 2 {
        i -= 1;
        year_digits += 1;
    }
    if year_digits == 0 {
        return None; // no trailing year
    }

    // 2) The character before the digits must be the month letter
    if i == 0 {
        return None; // there was no month letter before digits
    }
    let month_char = s.chars().nth(i - 1)?; // preceding char
    let month = month_from_code(month_char)?;

    // 3) Parse year (1- or 2-digit)
    let year_str = &s[i..];
    let now = chrono::Utc::now().date_naive();
    let year = if year_digits == 1 {
        // Single-digit year → pivot to current decade, roll forward if needed
        let d = year_str.chars().next()?.to_digit(10)? as i32;
        let mut y = (now.year() / 10) * 10 + d; // e.g., 2025 → 2025 when d=5
        if y < now.year() - 2 { y += 10; }      // avoid picking too far in the past
        y
    } else {
        // Two-digit year. Assume 2000..2079 to cover current trading horizons.
        // (If you want broader, choose a different pivot.)
        let yy = year_str.parse::<i32>().ok()?;
        if yy <= 79 { 2000 + yy } else { 1900 + yy }
    };

    NaiveDate::from_ymd_opt(year, month, 1)
}