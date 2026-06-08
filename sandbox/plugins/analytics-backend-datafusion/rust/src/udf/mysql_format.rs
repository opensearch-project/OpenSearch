/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared MySQL format-token translator (date_format / time_format / str_to_date);
//! mirrors PPL's `DateTimeFormatterUtil`. `%V`/`%v` use chrono's ISO week; `%U`/`%u`
//! use simple Sun-/Mon-first counting. These match MySQL modes 0/1 except when
//! Jan 1 falls in week 52/53 of the prior year — matches PPL's observed output.

use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc, Weekday};

// Token variants map 1:1 to MySQL `%X` directives; `Literal` wraps non-directive chars.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token {
    Literal(char),
    A, B, C, D, DLower, E, F,
    HUpper, HLower, ILower, IUpper, J, K, L,
    MUpper, MLower, P, R, SUpper, SLower, T,
    UUpper, ULower, VUpper, VLower, W, WLower,
    XUpper, XLower, YUpper, YLower,
}

fn tokenize(format: &str) -> Vec<Token> {
    let bytes = format.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'%' {
            out.push(Token::Literal(bytes[i] as char));
            i += 1;
            continue;
        }
        if i + 1 >= bytes.len() {
            out.push(Token::Literal('%'));
            break;
        }
        let tok = match bytes[i + 1] {
            b'%' => Some(Token::Literal('%')),
            b'a' => Some(Token::A),
            b'b' => Some(Token::B),
            b'c' => Some(Token::C),
            b'D' => Some(Token::D),
            b'd' => Some(Token::DLower),
            b'e' => Some(Token::E),
            b'f' => Some(Token::F),
            b'H' => Some(Token::HUpper),
            b'h' => Some(Token::HLower),
            b'I' => Some(Token::IUpper),
            b'i' => Some(Token::ILower),
            b'j' => Some(Token::J),
            b'k' => Some(Token::K),
            b'l' => Some(Token::L),
            b'M' => Some(Token::MUpper),
            b'm' => Some(Token::MLower),
            b'p' => Some(Token::P),
            b'r' => Some(Token::R),
            b'S' => Some(Token::SUpper),
            b's' => Some(Token::SLower),
            b'T' => Some(Token::T),
            b'U' => Some(Token::UUpper),
            b'u' => Some(Token::ULower),
            b'V' => Some(Token::VUpper),
            b'v' => Some(Token::VLower),
            b'W' => Some(Token::W),
            b'w' => Some(Token::WLower),
            b'X' => Some(Token::XUpper),
            b'x' => Some(Token::XLower),
            b'Y' => Some(Token::YUpper),
            b'y' => Some(Token::YLower),
            _ => None,
        };
        match tok {
            Some(t) => {
                out.push(t);
                i += 2;
            }
            None => {
                out.push(Token::Literal('%'));
                out.push(Token::Literal(bytes[i + 1] as char));
                i += 2;
            }
        }
    }
    out
}

/// In `Time` mode, date-only numeric tokens emit MySQL's literal padding
/// (`%d`→"00", `%Y`→"0000", `%c`/`%e`→"0") and date name-tokens collapse the
/// whole render to `None` — matches PPL's `getFormattedString` catching the
/// null-handler NPE and surfacing `ExprNullValue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FormatMode {
    Date,
    Time,
}

pub(crate) fn format_datetime(dt: DateTime<Utc>, format: &str, mode: FormatMode) -> Option<String> {
    let tokens = tokenize(format);
    let mut out = String::with_capacity(format.len() + 16);
    for tok in tokens {
        match render_token(tok, dt, mode)? {
            Rendered::Str(s) => out.push_str(&s),
            Rendered::Char(c) => out.push(c),
        }
    }
    Some(out)
}

enum Rendered {
    Str(String),
    Char(char),
}

fn render_token(tok: Token, dt: DateTime<Utc>, mode: FormatMode) -> Option<Rendered> {
    use Rendered::*;
    let time_mode = matches!(mode, FormatMode::Time);
    // Helper: date-only tokens in time-mode → whole render collapses to NULL.
    macro_rules! date_only {
        ($body:expr) => {{
            if time_mode {
                return None;
            }
            $body
        }};
    }
    let r = match tok {
        Token::Literal(c) => Char(c),
        Token::A => date_only!(Str(weekday_short(dt.weekday()).into())),
        Token::B => date_only!(Str(month_short(dt.month()).into())),
        Token::MUpper => date_only!(Str(month_full(dt.month()).into())),
        Token::W => date_only!(Str(weekday_full(dt.weekday()).into())),
        Token::D => date_only!({
            let d = dt.day();
            Str(format!("{}{}", d, ordinal_suffix(d)))
        }),
        // PPL bug-for-bug: %w uses Mon=1..Sun=7 despite MySQL docs claiming Sun=0.
        Token::WLower => date_only!(Str(dt.weekday().number_from_monday().to_string())),
        Token::J => date_only!(Str(format!("{:03}", dt.ordinal()))),
        Token::UUpper => date_only!(Str(format!("{:02}", week_number_sunday_first(dt)))),
        Token::ULower => date_only!(Str(format!("{:02}", week_number_monday_first(dt)))),
        Token::VUpper => date_only!(Str(format!("{:02}", week_number_sunday_first_01(dt)))),
        Token::VLower => date_only!(Str(format!("{:02}", dt.iso_week().week()))),
        Token::XUpper => date_only!(Str(format!("{:04}", week_year_sunday_first(dt)))),
        Token::XLower => date_only!(Str(format!("{:04}", dt.iso_week().year()))),
        // Numeric date tokens emit zero-literal padding in time mode.
        Token::C => Str(if time_mode { "0".into() } else { dt.month().to_string() }),
        Token::DLower => Str(if time_mode { "00".into() } else { format!("{:02}", dt.day()) }),
        Token::E => Str(if time_mode { "0".into() } else { dt.day().to_string() }),
        Token::MLower => Str(if time_mode { "00".into() } else { format!("{:02}", dt.month()) }),
        Token::YUpper => Str(if time_mode { "0000".into() } else { format!("{:04}", dt.year()) }),
        Token::YLower => Str(if time_mode { "00".into() } else { format!("{:02}", dt.year() % 100) }),
        Token::F => Str(format!("{:06}", dt.nanosecond() / 1_000)),
        Token::HUpper => Str(format!("{:02}", dt.hour())),
        Token::HLower | Token::IUpper => Str(format!("{:02}", twelve_hour(dt.hour()))),
        Token::ILower => Str(format!("{:02}", dt.minute())),
        Token::K => Str(dt.hour().to_string()),
        Token::L => Str(twelve_hour(dt.hour()).to_string()),
        Token::P => Str(if dt.hour() < 12 { "AM".into() } else { "PM".into() }),
        Token::R => Str(format!(
            "{:02}:{:02}:{:02} {}",
            twelve_hour(dt.hour()),
            dt.minute(),
            dt.second(),
            if dt.hour() < 12 { "AM" } else { "PM" }
        )),
        Token::SUpper | Token::SLower => Str(format!("{:02}", dt.second())),
        Token::T => Str(format!("{:02}:{:02}:{:02}", dt.hour(), dt.minute(), dt.second())),
    };
    Some(r)
}

fn twelve_hour(h: u32) -> u32 {
    let h = h % 12;
    if h == 0 { 12 } else { h }
}

fn ordinal_suffix(day: u32) -> &'static str {
    if (11..=13).contains(&(day % 100)) {
        return "th";
    }
    match day % 10 {
        1 => "st",
        2 => "nd",
        3 => "rd",
        _ => "th",
    }
}

fn weekday_short(w: Weekday) -> &'static str {
    match w {
        Weekday::Mon => "Mon", Weekday::Tue => "Tue", Weekday::Wed => "Wed",
        Weekday::Thu => "Thu", Weekday::Fri => "Fri", Weekday::Sat => "Sat", Weekday::Sun => "Sun",
    }
}

fn weekday_full(w: Weekday) -> &'static str {
    match w {
        Weekday::Mon => "Monday", Weekday::Tue => "Tuesday", Weekday::Wed => "Wednesday",
        Weekday::Thu => "Thursday", Weekday::Fri => "Friday",
        Weekday::Sat => "Saturday", Weekday::Sun => "Sunday",
    }
}

fn month_short(m: u32) -> &'static str {
    ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        .get(m as usize)
        .copied()
        .unwrap_or("")
}

fn month_full(m: u32) -> &'static str {
    ["", "January", "February", "March", "April", "May", "June",
     "July", "August", "September", "October", "November", "December"]
        .get(m as usize)
        .copied()
        .unwrap_or("")
}

fn week_number_sunday_first(dt: DateTime<Utc>) -> u32 {
    week_number_with_start(dt, Weekday::Sun)
}

fn week_number_monday_first(dt: DateTime<Utc>) -> u32 {
    week_number_with_start(dt, Weekday::Mon)
}

/// `%V` — MySQL mode 2: Sunday-first, 01..53; week 0 remaps to prior year's last week.
fn week_number_sunday_first_01(dt: DateTime<Utc>) -> u32 {
    let wn = week_number_sunday_first(dt);
    if wn == 0 {
        let last = chrono::NaiveDate::from_ymd_opt(dt.year() - 1, 12, 31).unwrap();
        let last_dt = Utc.from_utc_datetime(&last.and_hms_opt(0, 0, 0).unwrap());
        week_number_sunday_first(last_dt)
    } else {
        wn
    }
}

fn week_year_sunday_first(dt: DateTime<Utc>) -> i32 {
    if week_number_sunday_first(dt) == 0 {
        dt.year() - 1
    } else {
        dt.year()
    }
}

fn week_number_with_start(dt: DateTime<Utc>, start: Weekday) -> u32 {
    let jan1 = chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1).unwrap();
    let offset = days_until_start(jan1.weekday(), start);
    let first_start_doy = 1 + offset;
    let doy = dt.ordinal();
    if doy < first_start_doy {
        0
    } else {
        (doy - first_start_doy) / 7 + 1
    }
}

fn days_until_start(from: Weekday, start: Weekday) -> u32 {
    (weekday_idx(start) + 7 - weekday_idx(from)) % 7
}

fn weekday_idx(w: Weekday) -> u32 {
    match w {
        Weekday::Mon => 0, Weekday::Tue => 1, Weekday::Wed => 2, Weekday::Thu => 3,
        Weekday::Fri => 4, Weekday::Sat => 5, Weekday::Sun => 6,
    }
}

// str_to_date parser: minimal recursive-descent mirroring PPL's
// DateTimeFormatterUtil.parseStringWithDateOrTime (lenient widths, trailing
// input tolerated, MySQL defaults year=2000/month=1/day=1).

#[derive(Default, Debug)]
pub(crate) struct Parsed {
    pub year: Option<i32>,
    pub month: Option<u32>,
    pub day: Option<u32>,
    pub day_of_year: Option<u32>,
    pub hour: Option<u32>,
    pub hour_12: Option<u32>,
    pub minute: Option<u32>,
    pub second: Option<u32>,
    pub micros: Option<u32>,
    pub pm: Option<bool>,
}

impl Parsed {
    pub(crate) fn to_naive(&self) -> Option<chrono::NaiveDateTime> {
        let (y, mo, d) = (self.year, self.month, self.day);
        let has_time = self.hour.is_some() || self.hour_12.is_some();
        let has_date_parts = y.is_some() || mo.is_some() || d.is_some() || self.day_of_year.is_some();
        if !has_date_parts && !has_time {
            return None;
        }
        let year = y.unwrap_or(2000);
        let (month, day) = if let Some(doy) = self.day_of_year {
            let date = chrono::NaiveDate::from_yo_opt(year, doy)?;
            (date.month(), date.day())
        } else {
            (mo.unwrap_or(1), d.unwrap_or(1))
        };
        let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
        let mut hour = match (self.hour, self.hour_12, self.pm) {
            (Some(h), _, _) => h,
            (None, Some(h12), Some(true)) => (h12 % 12) + 12,
            (None, Some(h12), _) => h12 % 12,
            (None, None, _) => 0,
        };
        if hour > 23 {
            hour = 23;
        }
        let time = chrono::NaiveTime::from_hms_micro_opt(
            hour,
            self.minute.unwrap_or(0),
            self.second.unwrap_or(0),
            self.micros.unwrap_or(0),
        )?;
        Some(chrono::NaiveDateTime::new(date, time))
    }
}

pub(crate) fn parse_mysql_format(input: &str, format: &str) -> Option<Parsed> {
    let tokens = tokenize(format);
    let input_bytes = input.as_bytes();
    let mut pos = 0;
    let mut f = Parsed::default();
    for tok in tokens {
        if pos > input_bytes.len() {
            return None;
        }
        while pos < input_bytes.len() && (input_bytes[pos] as char).is_whitespace() {
            pos += 1;
        }
        match tok {
            Token::Literal(c) => {
                if c.is_whitespace() {
                    continue;
                }
                if pos >= input_bytes.len() || input_bytes[pos] as char != c {
                    return None;
                }
                pos += 1;
            }
            Token::YUpper | Token::XUpper | Token::XLower => {
                let (v, np) = read_digits(input_bytes, pos, 1, 4)?;
                f.year = Some(v as i32);
                pos = np;
            }
            Token::YLower => {
                // MySQL pivots: 00-69→2000-2069, 70-99→1970-1999.
                let (v, np) = read_digits(input_bytes, pos, 1, 2)?;
                let yy = v as i32;
                f.year = Some(if yy < 70 { 2000 + yy } else { 1900 + yy });
                pos = np;
            }
            Token::MLower | Token::C => {
                let (v, np) = read_digits(input_bytes, pos, 1, 2)?;
                if !(1..=12).contains(&v) {
                    return None;
                }
                f.month = Some(v);
                pos = np;
            }
            Token::DLower | Token::E | Token::D => {
                let (v, np) = read_digits(input_bytes, pos, 1, 2)?;
                if !(1..=31).contains(&v) {
                    return None;
                }
                f.day = Some(v);
                pos = np;
                if matches!(tok, Token::D) {
                    pos = consume_while(input_bytes, pos, |b| b.is_ascii_alphabetic());
                }
            }
            Token::J => {
                let (v, np) = read_digits(input_bytes, pos, 1, 3)?;
                if !(1..=366).contains(&v) {
                    return None;
                }
                f.day_of_year = Some(v);
                pos = np;
            }
            Token::HUpper | Token::K => {
                let (v, np) = read_digits(input_bytes, pos, 1, 2)?;
                if v > 23 {
                    return None;
                }
                f.hour = Some(v);
                pos = np;
            }
            Token::HLower | Token::IUpper | Token::L => {
                let (v, np) = read_digits(input_bytes, pos, 1, 2)?;
                if !(1..=12).contains(&v) {
                    return None;
                }
                f.hour_12 = Some(v);
                pos = np;
            }
            Token::ILower => {
                let (v, np) = read_digits(input_bytes, pos, 1, 2)?;
                if v > 59 {
                    return None;
                }
                f.minute = Some(v);
                pos = np;
            }
            Token::SUpper | Token::SLower => {
                let (v, np) = read_digits(input_bytes, pos, 1, 2)?;
                if v > 59 {
                    return None;
                }
                f.second = Some(v);
                pos = np;
            }
            Token::F => {
                let (v, np) = read_digits(input_bytes, pos, 1, 6)?;
                f.micros = Some(v);
                pos = np;
            }
            Token::P => {
                pos = read_am_pm(input_bytes, pos, &mut f)?;
            }
            Token::R => {
                let (h, np) = read_digits(input_bytes, pos, 1, 2)?;
                if !(1..=12).contains(&h) {
                    return None;
                }
                pos = expect_literal(input_bytes, np, b':')?;
                let (m, np) = read_digits(input_bytes, pos, 1, 2)?;
                if m > 59 {
                    return None;
                }
                pos = expect_literal(input_bytes, np, b':')?;
                let (s, np) = read_digits(input_bytes, pos, 1, 2)?;
                if s > 59 {
                    return None;
                }
                pos = np;
                while pos < input_bytes.len() && (input_bytes[pos] as char).is_whitespace() {
                    pos += 1;
                }
                pos = read_am_pm(input_bytes, pos, &mut f)?;
                f.hour_12 = Some(h);
                f.minute = Some(m);
                f.second = Some(s);
            }
            Token::T => {
                let (h, np) = read_digits(input_bytes, pos, 1, 2)?;
                if h > 23 {
                    return None;
                }
                pos = expect_literal(input_bytes, np, b':')?;
                let (m, np) = read_digits(input_bytes, pos, 1, 2)?;
                if m > 59 {
                    return None;
                }
                pos = expect_literal(input_bytes, np, b':')?;
                let (s, np) = read_digits(input_bytes, pos, 1, 2)?;
                if s > 59 {
                    return None;
                }
                pos = np;
                f.hour = Some(h);
                f.minute = Some(m);
                f.second = Some(s);
            }
            // Name tokens: opportunistically consume letters (PPL doesn't validate).
            Token::A | Token::B | Token::W | Token::MUpper => {
                pos = consume_while(input_bytes, pos, |b| b.is_ascii_alphabetic());
            }
            // Week-based tokens: consume digits, ignore value (PPL doesn't back-solve either).
            Token::UUpper | Token::ULower | Token::VUpper | Token::VLower | Token::WLower => {
                let (_, np) = read_digits(input_bytes, pos, 1, 2)?;
                pos = np;
            }
        }
    }
    Some(f)
}

fn read_am_pm(bytes: &[u8], pos: usize, f: &mut Parsed) -> Option<usize> {
    if pos + 2 > bytes.len() {
        return None;
    }
    let s: &str = std::str::from_utf8(&bytes[pos..pos + 2]).ok()?;
    match s.to_ascii_uppercase().as_str() {
        "AM" => f.pm = Some(false),
        "PM" => f.pm = Some(true),
        _ => return None,
    }
    Some(pos + 2)
}

fn read_digits(bytes: &[u8], pos: usize, min: usize, max: usize) -> Option<(u32, usize)> {
    let mut end = pos;
    while end < bytes.len() && end - pos < max && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end - pos < min {
        return None;
    }
    let s = std::str::from_utf8(&bytes[pos..end]).ok()?;
    Some((s.parse().ok()?, end))
}

fn expect_literal(bytes: &[u8], pos: usize, expected: u8) -> Option<usize> {
    if pos >= bytes.len() || bytes[pos] != expected {
        return None;
    }
    Some(pos + 1)
}

fn consume_while<F: Fn(u8) -> bool>(bytes: &[u8], pos: usize, pred: F) -> usize {
    let mut p = pos;
    while p < bytes.len() && pred(bytes[p]) {
        p += 1;
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sample() -> DateTime<Utc> {
        // 2020-03-15 10:30:45.123456 UTC (Sunday)
        Utc.timestamp_micros(1_584_268_245_123_456).single().unwrap()
    }

    fn fmt(dt: DateTime<Utc>, f: &str, m: FormatMode) -> Option<String> {
        format_datetime(dt, f, m)
    }

    #[test]
    fn date_mode_common_tokens() {
        let dt = sample();
        assert_eq!(fmt(dt, "%Y-%m-%d %H:%i:%S", FormatMode::Date).unwrap(), "2020-03-15 10:30:45");
        assert_eq!(fmt(dt, "%W, %M %D, %Y", FormatMode::Date).unwrap(), "Sunday, March 15th, 2020");
        assert_eq!(fmt(dt, "%h:%i:%s %p", FormatMode::Date).unwrap(), "10:30:45 AM");
    }

    #[test]
    fn ordinal_suffix_edges() {
        for (d, want) in [
            (1, "1st"), (2, "2nd"), (3, "3rd"), (4, "4th"),
            (11, "11th"), (12, "12th"), (13, "13th"),
            (21, "21st"), (22, "22nd"), (23, "23rd"), (31, "31st"),
        ] {
            let dt = Utc.with_ymd_and_hms(2020, 1, d, 0, 0, 0).unwrap();
            assert_eq!(fmt(dt, "%D", FormatMode::Date).unwrap(), want, "day={d}");
        }
    }

    #[test]
    fn time_mode_masks_date_tokens() {
        let dt = sample();
        assert_eq!(fmt(dt, "%Y-%m-%d %H:%i:%S", FormatMode::Time).unwrap(), "0000-00-00 10:30:45");
        assert!(fmt(dt, "%W", FormatMode::Time).is_none());
        assert!(fmt(dt, "%a %H:%i", FormatMode::Time).is_none());
    }

    #[test]
    fn twelve_hour_tokens() {
        let midnight = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let noon = Utc.with_ymd_and_hms(2020, 1, 1, 12, 0, 0).unwrap();
        assert_eq!(fmt(midnight, "%h %p", FormatMode::Date).unwrap(), "12 AM");
        assert_eq!(fmt(noon, "%h %p", FormatMode::Date).unwrap(), "12 PM");
    }

    #[test]
    fn week_tokens_iso_and_first_day() {
        let dt = sample(); // 2020-03-15 = Sunday, ISO week 11
        assert_eq!(fmt(dt, "%v", FormatMode::Date).unwrap(), "11");
        assert_eq!(fmt(dt, "%U", FormatMode::Date).unwrap(), "11");
    }

    #[test]
    fn unknown_directive_passes_through() {
        assert_eq!(fmt(sample(), "%Y-%q", FormatMode::Date).unwrap(), "2020-%q");
    }

    #[test]
    fn parse_roundtrip_and_defaults() {
        for (input, format, expected) in [
            ("2020-03-15 10:30:45", "%Y-%m-%d %H:%i:%S", "2020-03-15 10:30:45"),
            ("2020-03-15 extra", "%Y-%m-%d", "2020-03-15 00:00:00"),
            ("2020", "%Y", "2020-01-01 00:00:00"),
            // PPL uses `today`; we emit the MySQL-compatible 2000-01-01 default for determinism.
            ("10:30:45", "%H:%i:%S", "2000-01-01 10:30:45"),
        ] {
            let ndt = parse_mysql_format(input, format).unwrap().to_naive().unwrap();
            assert_eq!(ndt.to_string(), expected, "input={input}");
        }
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert!(parse_mysql_format("not-a-date", "%Y-%m-%d").is_none());
        assert!(parse_mysql_format("2020-13-01", "%Y-%m-%d").is_none());
        assert!(parse_mysql_format("", "%Y").is_none());
    }

    #[test]
    fn parse_12hr_with_am_pm() {
        let p = parse_mysql_format("01:30:45 PM", "%h:%i:%s %p").unwrap();
        assert_eq!(p.to_naive().unwrap().time().to_string(), "13:30:45");
        let p = parse_mysql_format("12:00:00 AM", "%h:%i:%s %p").unwrap();
        assert_eq!(p.to_naive().unwrap().time().to_string(), "00:00:00");
    }

    #[test]
    fn parse_fractional_seconds() {
        let p = parse_mysql_format("2020-03-15 10:30:45.123456", "%Y-%m-%d %H:%i:%S.%f").unwrap();
        assert_eq!(p.to_naive().unwrap().and_utc().timestamp_micros(), 1_584_268_245_123_456);
    }
}
