/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! conversion functions: `num`, `auto`, `memk`, `rmcomma`, `rmunit`, `dur2sec`, `mstime`
//!
//! Each UDF takes a single `Utf8` / `LargeUtf8` input and returns `Float64` (nullable)
//!
//! | Function | Accepts | Rejects | Notes |
//! |----------|---------|---------|-------|
//! | `num`    | `"3.14"`, `"1,234"`, `"100MB"` (→ 100), `"3e2"` | `"abc"`, `"1abc"` (no unit separator), `".mb"` | Strict numeric; leading-number-plus-unit fallback requires the suffix to start with a non-digit/non-dot character. |
//! | `auto`   | `"1.5g"` (→ 1 572 864), everything `num` accepts | same rejection set as num for non-memory inputs | Tries memk first, falls back to num. |
//! | `memk`   | `"500k"`, `"1.5m"`, `"2g"`, `"500"` (bare number → KB) | `"1.5t"`, `"abc"` | k / bare → value as-is, m → ×1024, g → ×1024². |
//! | `rmcomma`| `"12,345.67"` (→ 12345.67) | `"12abc"` (letter present) | Strips every comma, then parses; letters short-circuit to NULL. |
//! | `rmunit` | `"100MB"` (→ 100), `"-3.14cm"`, `"1.5e2kg"` | `"abc"`, `".mb"` (no leading number) | Regex-extracts the leading numeric token and parses. |

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use once_cell::sync::Lazy;
use regex::Regex;

/// Register every conversion UDF (`num`, `auto`, `memk`, `rmcomma`, `rmunit`,
/// `dur2sec`, `mstime`) on a session.
pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ConversionUdf::new(ConversionFn::Num)));
    ctx.register_udf(ScalarUDF::from(ConversionUdf::new(ConversionFn::Auto)));
    ctx.register_udf(ScalarUDF::from(ConversionUdf::new(ConversionFn::Memk)));
    ctx.register_udf(ScalarUDF::from(ConversionUdf::new(ConversionFn::Rmcomma)));
    ctx.register_udf(ScalarUDF::from(ConversionUdf::new(ConversionFn::Rmunit)));
    ctx.register_udf(ScalarUDF::from(ConversionUdf::new(ConversionFn::Dur2sec)));
    ctx.register_udf(ScalarUDF::from(ConversionUdf::new(ConversionFn::Mstime)));
}

/// Which conversion function this UDF instance implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ConversionFn {
    Num,
    Auto,
    Memk,
    Rmcomma,
    Rmunit,
    Dur2sec,
    Mstime,
}

impl ConversionFn {
    fn name(self) -> &'static str {
        match self {
            ConversionFn::Num => "num",
            ConversionFn::Auto => "auto",
            ConversionFn::Memk => "memk",
            ConversionFn::Rmcomma => "rmcomma",
            ConversionFn::Rmunit => "rmunit",
            ConversionFn::Dur2sec => "dur2sec",
            ConversionFn::Mstime => "mstime",
        }
    }

    /// Dispatch the per-row implementation. `None` output propagates through as NULL.
    fn apply(self, input: &str) -> Option<f64> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return None;
        }
        match self {
            ConversionFn::Num => convert_num(trimmed),
            ConversionFn::Auto => convert_memk(trimmed).or_else(|| convert_num(trimmed)),
            ConversionFn::Memk => convert_memk(trimmed),
            ConversionFn::Rmcomma => convert_rmcomma(trimmed),
            ConversionFn::Rmunit => convert_rmunit(trimmed),
            ConversionFn::Dur2sec => convert_dur2sec(trimmed),
            ConversionFn::Mstime => convert_mstime(trimmed),
        }
    }
}

/// Shared UDF type — one instance per function, keyed by `ConversionFn`.
#[derive(Debug)]
pub struct ConversionUdf {
    kind: ConversionFn,
    signature: Signature,
}

impl ConversionUdf {
    fn new(kind: ConversionFn) -> Self {
        Self {
            kind,
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

// ScalarUDFImpl requires DynEq+DynHash. Function identity is `kind` — two instances of the
// same ConversionFn are semantically interchangeable.
impl PartialEq for ConversionUdf {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}
impl Eq for ConversionUdf {}
impl Hash for ConversionUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
    }
}

impl ScalarUDFImpl for ConversionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!(
                "{} expects exactly 1 argument, got {}",
                self.kind.name(),
                args.args.len()
            );
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(opt)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Float64(opt.as_ref().and_then(|s| self.kind.apply(s))),
            )),
            ColumnarValue::Scalar(other) => {
                exec_err!(
                    "{}: expected Utf8/LargeUtf8 input, got {other:?}",
                    self.kind.name()
                )
            }
            ColumnarValue::Array(arr) => {
                let out: Float64Array = match arr.data_type() {
                    DataType::Utf8 => arr
                        .as_string::<i32>()
                        .iter()
                        .map(|opt| opt.and_then(|s| self.kind.apply(s)))
                        .collect(),
                    DataType::LargeUtf8 => arr
                        .as_string::<i64>()
                        .iter()
                        .map(|opt| opt.and_then(|s| self.kind.apply(s)))
                        .collect(),
                    other => {
                        return exec_err!("{}: expected Utf8 array, got {other:?}", self.kind.name());
                    }
                };
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }
        }
    }
}

// ── Per-function implementations ───────────────────────────────────────────────────────────

/// Gatekeeper for `num`.
static STARTS_WITH_SIGN_OR_DIGIT: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[+\-]?[\d.].*").unwrap());

/// Captures an optional sign, the numeric body (one of `digits`, `digits.digits`, `digits.`, or `.digits`),
/// and an optional exponent.
static LEADING_NUMBER_WITH_UNIT: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^([+\-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+\-]?\d+)?)(.*)$").unwrap()
});

/// Optional sign, number body, optional unit in {k,m,g} (case-insensitive). Bare number with no unit is accepted.
static MEMK: Lazy<Regex> = Lazy::new(|| Regex::new(r"^([+\-]?\d+\.?\d*)([kmgKMG])?$").unwrap());

/// Fast-reject gate for `rmcomma`.
static CONTAINS_LETTER: Lazy<Regex> = Lazy::new(|| Regex::new(r"[a-zA-Z]").unwrap());

/// `[D+]HH:MM:SS` — optional day count prefix separated by `+`.
static DUR2SEC: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?:(\d+)\+)?(\d{1,2}):(\d{1,2}):(\d{1,2})$").unwrap()
});

/// Optional `MM:` prefix, required `SS`, optional `.SSS`.
static MSTIME: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?:(\d{1,2}):)?(\d{1,2})(?:\.(\d{1,3}))?$").unwrap()
});

const MB_TO_KB: f64 = 1024.0;
const GB_TO_KB: f64 = 1024.0 * 1024.0;

/// `num(x)` — strict parse with comma-strip + leading-number-plus-unit-suffix fallback.
fn convert_num(s: &str) -> Option<f64> {
    if !STARTS_WITH_SIGN_OR_DIGIT.is_match(s) {
        return None;
    }
    if let Ok(n) = s.parse::<f64>() {
        return Some(n);
    }
    if s.contains(',') {
        if let Ok(n) = s.replace(',', "").parse::<f64>() {
            return Some(n);
        }
    }
    // Leading-number-plus-unit-suffix fallback: extract the numeric prefix only when the suffix
    // starts with a non-digit, non-dot character.
    if let Some(caps) = LEADING_NUMBER_WITH_UNIT.captures(s) {
        let leading = caps.get(1)?.as_str();
        let suffix = caps.get(2)?.as_str().trim();
        if suffix.is_empty() {
            return None;
        }
        let first_byte = suffix.as_bytes()[0];
        let has_unit = !first_byte.is_ascii_digit() && first_byte != b'.';
        if has_unit {
            return leading.parse::<f64>().ok();
        }
    }
    None
}

/// `memk(x)` — memory-unit conversion. Returns KB.
fn convert_memk(s: &str) -> Option<f64> {
    let caps = MEMK.captures(s)?;
    let number: f64 = caps.get(1)?.as_str().parse().ok()?;
    let multiplier = match caps.get(2).map(|m| m.as_str()) {
        None => 1.0,
        Some(unit) => match unit.to_ascii_lowercase().as_str() {
            "k" => 1.0,
            "m" => MB_TO_KB,
            "g" => GB_TO_KB,
            _ => 1.0,
        },
    };
    Some(number * multiplier)
}

/// `rmcomma(x)` — strip commas, then parse. NULL if letters are present.
fn convert_rmcomma(s: &str) -> Option<f64> {
    if CONTAINS_LETTER.is_match(s) {
        return None;
    }
    s.replace(',', "").parse::<f64>().ok()
}

/// `rmunit(x)` — extract the leading numeric token and parse.
fn convert_rmunit(s: &str) -> Option<f64> {
    let caps = LEADING_NUMBER_WITH_UNIT.captures(s)?;
    let leading = caps.get(1)?.as_str();
    leading.parse::<f64>().ok()
}

/// `dur2sec(x)` — `[D+]HH:MM:SS` duration → total seconds as double. Also accepts a pre-parsed
/// numeric string (e.g. `"3600"`) and returns it verbatim.
fn convert_dur2sec(s: &str) -> Option<f64> {
    if let Ok(n) = s.parse::<f64>() {
        return Some(n);
    }
    let caps = DUR2SEC.captures(s)?;
    let days: u64 = caps.get(1).map(|m| m.as_str()).unwrap_or("0").parse().ok()?;
    let hours: u64 = caps.get(2)?.as_str().parse().ok()?;
    let minutes: u64 = caps.get(3)?.as_str().parse().ok()?;
    let seconds: u64 = caps.get(4)?.as_str().parse().ok()?;
    if hours >= 24 || minutes >= 60 || seconds >= 60 {
        return None;
    }
    Some((days * 86_400 + hours * 3_600 + minutes * 60 + seconds) as f64)
}

/// `mstime(x)` — `[MM:]SS.SSS` time-of-day → total seconds (double) with millisecond
/// precision. Also accepts a pre-parsed numeric string.
fn convert_mstime(s: &str) -> Option<f64> {
    if let Ok(n) = s.parse::<f64>() {
        return Some(n);
    }
    let caps = MSTIME.captures(s)?;
    let minutes: u64 = caps.get(1).map(|m| m.as_str()).unwrap_or("0").parse().ok()?;
    let seconds: u64 = caps.get(2)?.as_str().parse().ok()?;
    if seconds >= 60 {
        return None;
    }
    let millis = caps
        .get(3)
        .map(|m| {
            let raw = m.as_str();
            // zero-pads to 3 digits before parsing
            let mut padded = String::with_capacity(3);
            padded.push_str(raw);
            for _ in raw.len()..3 {
                padded.push('0');
            }
            padded[..3].parse::<f64>().unwrap_or(0.0) / 1_000.0
        })
        .unwrap_or(0.0);
    Some((minutes * 60 + seconds) as f64 + millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── num ────────────────────────────────────────────────────────────────
    #[test]
    fn num_accepts_plain_decimal() {
        assert_eq!(convert_num("3.14"), Some(3.14));
    }

    #[test]
    fn num_accepts_scientific_notation() {
        assert_eq!(convert_num("1e3"), Some(1000.0));
    }

    #[test]
    fn num_strips_commas_when_needed() {
        assert_eq!(convert_num("1,234.5"), Some(1234.5));
    }

    #[test]
    fn num_extracts_leading_number_from_unit_suffix() {
        assert_eq!(convert_num("100MB"), Some(100.0));
        assert_eq!(convert_num("-3.5%"), Some(-3.5));
    }

    #[test]
    fn num_rejects_letter_prefix() {
        assert_eq!(convert_num("abc"), None);
        assert_eq!(convert_num("a123"), None);
    }

    #[test]
    fn num_rejects_space_separated_digit_run() {
        assert_eq!(convert_num("123 456"), None);
    }

    // ── memk ───────────────────────────────────────────────────────────────
    #[test]
    fn memk_bare_number_is_kb() {
        assert_eq!(convert_memk("500"), Some(500.0));
    }

    #[test]
    fn memk_k_suffix_is_pass_through() {
        assert_eq!(convert_memk("500k"), Some(500.0));
        assert_eq!(convert_memk("500K"), Some(500.0));
    }

    #[test]
    fn memk_m_suffix_multiplies_by_1024() {
        assert_eq!(convert_memk("1.5m"), Some(1536.0));
        assert_eq!(convert_memk("1.5M"), Some(1536.0));
    }

    #[test]
    fn memk_g_suffix_multiplies_by_1024_squared() {
        assert_eq!(convert_memk("2g"), Some(2.0 * GB_TO_KB));
    }

    #[test]
    fn memk_rejects_unknown_suffix() {
        assert_eq!(convert_memk("1.5t"), None);
        assert_eq!(convert_memk("abc"), None);
    }

    // ── auto ───────────────────────────────────────────────────────────────
    #[test]
    fn auto_prefers_memk_over_num() {
        assert_eq!(ConversionFn::Auto.apply("1.5m"), Some(1536.0));
    }

    #[test]
    fn auto_falls_back_to_num() {
        assert_eq!(ConversionFn::Auto.apply("1,234"), Some(1234.0));
        assert_eq!(ConversionFn::Auto.apply("3.14"), Some(3.14));
    }

    #[test]
    fn auto_rejects_when_both_paths_fail() {
        assert_eq!(ConversionFn::Auto.apply("abc"), None);
    }

    // ── rmcomma ────────────────────────────────────────────────────────────
    #[test]
    fn rmcomma_strips_and_parses() {
        assert_eq!(convert_rmcomma("12,345.67"), Some(12345.67));
    }

    #[test]
    fn rmcomma_rejects_letters() {
        assert_eq!(convert_rmcomma("12,345abc"), None);
        assert_eq!(convert_rmcomma("1a"), None);
    }

    #[test]
    fn rmcomma_accepts_negative() {
        assert_eq!(convert_rmcomma("-1,000"), Some(-1000.0));
    }

    // ── rmunit ─────────────────────────────────────────────────────────────
    #[test]
    fn rmunit_extracts_leading_number() {
        assert_eq!(convert_rmunit("100MB"), Some(100.0));
        assert_eq!(convert_rmunit("-3.14cm"), Some(-3.14));
    }

    #[test]
    fn rmunit_handles_pure_number() {
        assert_eq!(convert_rmunit("42"), Some(42.0));
    }

    #[test]
    fn rmunit_handles_scientific_notation_before_unit() {
        assert_eq!(convert_rmunit("1.5e2kg"), Some(150.0));
    }

    #[test]
    fn rmunit_rejects_no_leading_number() {
        assert_eq!(convert_rmunit("abc"), None);
        assert_eq!(convert_rmunit(".abc"), None);
    }

    // ── dur2sec ────────────────────────────────────────────────────────────
    #[test]
    fn dur2sec_accepts_simple_hhmmss() {
        // 1h 1m 1s = 3661 seconds
        assert_eq!(convert_dur2sec("01:01:01"), Some(3661.0));
    }

    #[test]
    fn dur2sec_accepts_days_prefix() {
        // 1 day, 2h 3m 4s = 86400 + 7200 + 180 + 4 = 93784
        assert_eq!(convert_dur2sec("1+02:03:04"), Some(93784.0));
    }

    #[test]
    fn dur2sec_accepts_pre_parsed_number() {
        assert_eq!(convert_dur2sec("3600"), Some(3600.0));
    }

    #[test]
    fn dur2sec_rejects_out_of_range_components() {
        assert_eq!(convert_dur2sec("24:00:00"), None);
        assert_eq!(convert_dur2sec("00:60:00"), None);
        assert_eq!(convert_dur2sec("00:00:60"), None);
    }

    #[test]
    fn dur2sec_rejects_malformed() {
        assert_eq!(convert_dur2sec("not-a-duration"), None);
        assert_eq!(convert_dur2sec("1:2"), None);
    }

    // ── mstime ─────────────────────────────────────────────────────────────
    #[test]
    fn mstime_accepts_ss_only() {
        assert_eq!(convert_mstime("45"), Some(45.0));
    }

    #[test]
    fn mstime_accepts_mm_ss() {
        assert_eq!(convert_mstime("02:30"), Some(150.0));
    }

    #[test]
    fn mstime_accepts_ss_with_millis() {
        assert_eq!(convert_mstime("00:00.5"), Some(0.5));
        assert_eq!(convert_mstime("00:00.500"), Some(0.5));
        assert_eq!(convert_mstime("00:00.123"), Some(0.123));
    }

    #[test]
    fn mstime_accepts_pre_parsed_double() {
        assert_eq!(convert_mstime("3.14"), Some(3.14));
    }

    #[test]
    fn mstime_rejects_seconds_over_59_in_regex_branch() {
        assert_eq!(convert_mstime("00:60"), None);
    }

    #[test]
    fn mstime_accepts_bare_seconds_over_59_via_parse_shortcut() {
        assert_eq!(convert_mstime("60"), Some(60.0));
    }

    #[test]
    fn mstime_rejects_malformed() {
        assert_eq!(convert_mstime("abc"), None);
        assert_eq!(convert_mstime("1:2:3"), None);
    }

    // ── empty / whitespace / null — dispatched at the ConversionFn level ──
    #[test]
    fn empty_string_returns_none_for_every_fn() {
        for kind in [
            ConversionFn::Num,
            ConversionFn::Auto,
            ConversionFn::Memk,
            ConversionFn::Rmcomma,
            ConversionFn::Rmunit,
            ConversionFn::Dur2sec,
            ConversionFn::Mstime,
        ] {
            assert_eq!(kind.apply(""), None, "{:?} should return None for empty", kind);
            assert_eq!(
                kind.apply("   "),
                None,
                "{:?} should return None for whitespace-only",
                kind
            );
        }
    }
}
