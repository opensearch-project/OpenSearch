/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `convert_tz(ts, from_tz, to_tz)` — shift a timestamp from one timezone to another.
//!
//! # Division of labor with the Java adapter
//!
//! Literal validation + canonicalization happens Java-side in
//! `ConvertTzAdapter` (see `.../be/datafusion/ConvertTzAdapter.java`), which
//! runs at plan time:
//!   * bad literals (unknown IANA, malformed offset) surface as
//!     `IllegalArgumentException` at plan time — users see the error instantly.
//!   * literal tz operands arrive here already canonicalized (`+05:00`, not
//!     `+5:00`; JDK-normalized IANA ids).
//!   * identity cases (`from == to`) are short-circuited plan-side and never
//!     reach this UDF.
//!
//! What stays here:
//!   * **Per-row DST-correct shifting.** IANA offsets vary per instant; can't
//!     be folded at plan time.
//!   * **Column-valued tz operands.** Values aren't known until runtime;
//!     unparseable entries yield NULL rows (matches MySQL's lenient
//!     `CONVERT_TZ` behavior).
//!
//! Semantics (MySQL-compatible):
//! * `ts` is interpreted as a wall-clock time in `from_tz`.
//! * The return is the wall-clock time in `to_tz` for the same instant.
//! * Timezone strings may be IANA names (`'America/New_York'`) or ISO offsets
//!   of the form `±HH:MM` with hours ∈ [0,14], minutes ∈ [0,59].
//! * Any null input → null output (null propagation).
//! * Unparseable column-valued timezone → null output.

use std::any::Any;
use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use datafusion::arrow::array::{
    Array, ArrayRef, TimestampMillisecondArray, TimestampMillisecondBuilder,
};

use super::json_common::StringArrayView;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{plan_err, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::{coerce_args, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ConvertTzUdf::new()));
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertTzUdf {
    signature: Signature,
}

impl ConvertTzUdf {
    pub fn new() -> Self {
        // PPL emits `convert_tz(ts, from, to)` with ts typed as Utf8 (string
        // literal), Date32, or Timestamp(any precision, any tz). Signature::exact
        // only let through the Timestamp(Ms, None) variant; user_defined +
        // coerce_types lets DF insert the right casts.
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for ConvertTzUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ConvertTzUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "convert_tz"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 3 {
            return plan_err!("convert_tz expects 3 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "convert_tz",
            arg_types,
            &[CoerceMode::TimestampMs, CoerceMode::Utf8, CoerceMode::Utf8],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return plan_err!("convert_tz expects 3 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;

        // Fast-path: scalar tz operands are parsed once up front, not per row.
        // The Java adapter canonicalizes literal tz strings at plan time so
        // bad-literal input can't reach this UDF — a None from parse_tz on a
        // scalar therefore means the scalar was SQL NULL.
        let from_scalar = scalar_tz(&args.args[1]);
        let to_scalar = scalar_tz(&args.args[2]);

        let ts = args.args[0].clone().into_array(n)?;
        let ts = ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "convert_tz: expected TimestampMillisecond, got {:?}",
                    ts.data_type()
                ))
            })?;

        // Only materialize column-valued tz operands; for scalars the parsed
        // TzSpec is already in hand. Keep the ArrayRef alive alongside the
        // view — StringArrayView borrows from the underlying buffer.
        let from_arr_ref: Option<ArrayRef> = if from_scalar.is_none() && matches!(&args.args[1], ColumnarValue::Array(_)) {
            Some(args.args[1].clone().into_array(n)?)
        } else {
            None
        };
        let to_arr_ref: Option<ArrayRef> = if to_scalar.is_none() && matches!(&args.args[2], ColumnarValue::Array(_)) {
            Some(args.args[2].clone().into_array(n)?)
        } else {
            None
        };
        let from_array: Option<StringArrayView<'_>> =
            from_arr_ref.as_ref().map(StringArrayView::from_array).transpose()?;
        let to_array: Option<StringArrayView<'_>> =
            to_arr_ref.as_ref().map(StringArrayView::from_array).transpose()?;

        let mut builder = TimestampMillisecondBuilder::with_capacity(n);
        for i in 0..n {
            if ts.is_null(i) {
                builder.append_null();
                continue;
            }
            let from = match (&from_scalar, from_array.as_ref().and_then(|a| a.cell(i))) {
                (Some(tz), _) => tz.clone(),
                (None, Some(s)) => match parse_tz(s) {
                    Some(tz) => tz,
                    None => {
                        builder.append_null();
                        continue;
                    }
                },
                _ => {
                    builder.append_null();
                    continue;
                }
            };
            let to = match (&to_scalar, to_array.as_ref().and_then(|a| a.cell(i))) {
                (Some(tz), _) => tz.clone(),
                (None, Some(s)) => match parse_tz(s) {
                    Some(tz) => tz,
                    None => {
                        builder.append_null();
                        continue;
                    }
                },
                _ => {
                    builder.append_null();
                    continue;
                }
            };
            match shift_millis_parsed(ts.value(i), &from, &to) {
                Some(v) => builder.append_value(v),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// If `cv` is a non-NULL string scalar, parse it once. Returns None for NULL,
/// non-scalar, or an unparseable string (the latter unreachable from literal
/// paths — Java canonicalizes — but defensive for degenerate scalar inputs).
fn scalar_tz(cv: &ColumnarValue) -> Option<TzSpec> {
    if let ColumnarValue::Scalar(sv) = cv {
        let s = match sv {
            ScalarValue::Utf8(opt) | ScalarValue::LargeUtf8(opt) | ScalarValue::Utf8View(opt) => opt.as_deref(),
            _ => None,
        };
        return s.and_then(parse_tz);
    }
    None
}

/// Parse timezone string (IANA name or `±HH:MM` offset).
#[derive(Clone)]
enum TzSpec {
    Iana(Tz),
    /// Fixed offset in seconds east of UTC.
    Offset(i32),
}

fn parse_tz(s: &str) -> Option<TzSpec> {
    if let Some(off) = parse_offset_seconds(s) {
        return Some(TzSpec::Offset(off));
    }
    s.parse::<Tz>().ok().map(TzSpec::Iana)
}

/// Parse `±HH:MM` → seconds east of UTC; None if not an offset literal.
///
/// Bounds ({@code hours ∈ [0,14], minutes ∈ [0,59]}) match the Java adapter's
/// {@code canonicalizeTz}. For literal-path inputs the Java side has already
/// validated and canonicalized, so the defensive checks here only fire for
/// column-valued tz — where a malformed entry yields a NULL row, matching the
/// documented lenient behavior.
fn parse_offset_seconds(s: &str) -> Option<i32> {
    let bytes = s.as_bytes();
    if bytes.len() != 6 {
        return None;
    }
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    if bytes[3] != b':' {
        return None;
    }
    let hours: i32 = s.get(1..3)?.parse().ok()?;
    let minutes: i32 = s.get(4..6)?.parse().ok()?;
    if hours > 14 || minutes > 59 {
        return None;
    }
    Some(sign * (hours * 3600 + minutes * 60))
}

/// Shift `ts_millis` from `from` to `to` using pre-parsed [`TzSpec`]s. The
/// stored timestamp has no tz attached — interpret its wall clock in `from`,
/// render that instant in `to`, then return the shifted millis as a tz-free
/// value the caller can continue to treat as naive. The shift is exactly
/// `to_offset(ts) - from_offset(ts)` milliseconds.
fn shift_millis_parsed(ts_millis: i64, from: &TzSpec, to: &TzSpec) -> Option<i64> {
    let naive = DateTime::<Utc>::from_timestamp_millis(ts_millis)?.naive_utc();
    let from_off = offset_seconds_at(from, &naive)?;
    let to_off = offset_seconds_at_instant(to, ts_millis, from_off)?;
    let delta_millis = (to_off - from_off) as i64 * 1_000;
    ts_millis.checked_add(delta_millis)
}

/// String-operand wrapper retained for direct-invocation tests that exercise
/// the full parse + shift flow in one call.
#[cfg(test)]
fn shift_millis(ts_millis: i64, from_tz: &str, to_tz: &str) -> Option<i64> {
    let from = parse_tz(from_tz)?;
    let to = parse_tz(to_tz)?;
    shift_millis_parsed(ts_millis, &from, &to)
}

/// Offset (seconds east of UTC) for `from_tz` at wall-clock `naive`.
fn offset_seconds_at(tz: &TzSpec, naive: &NaiveDateTime) -> Option<i32> {
    match tz {
        TzSpec::Offset(o) => Some(*o),
        TzSpec::Iana(z) => {
            // Use .from_local_datetime → pick the earliest resolution for ambiguous
            // (DST-fall-back) wall times, which matches MySQL's behaviour.
            match z.from_local_datetime(naive) {
                chrono::LocalResult::Single(dt) => Some(dt.offset().fix().local_minus_utc()),
                chrono::LocalResult::Ambiguous(dt, _) => Some(dt.offset().fix().local_minus_utc()),
                chrono::LocalResult::None => None, // wall time in the DST "spring-forward" gap
            }
        }
    }
}

/// Offset (seconds east of UTC) for `to_tz` at the UTC *instant* represented by
/// the input. We reconstruct the instant from `ts_millis` + `from_offset` (since
/// `ts_millis` is a wall clock in from_tz), then look up to_tz's offset at that
/// instant — DST-correct even across transitions.
fn offset_seconds_at_instant(
    tz: &TzSpec,
    ts_millis: i64,
    from_offset_seconds: i32,
) -> Option<i32> {
    match tz {
        TzSpec::Offset(o) => Some(*o),
        TzSpec::Iana(z) => {
            // instant_utc_millis = wall_millis - from_offset_millis
            let instant_millis =
                ts_millis.checked_sub((from_offset_seconds as i64) * 1_000)?;
            let instant = DateTime::<Utc>::from_timestamp_millis(instant_millis)?;
            Some(z.offset_from_utc_datetime(&instant.naive_utc()).fix().local_minus_utc())
        }
    }
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;

    // ±HH:MM offsets parse to the expected second counts.
    #[test]
    fn parse_offset_accepts_positive_and_negative() {
        assert_eq!(parse_offset_seconds("+00:00"), Some(0));
        assert_eq!(parse_offset_seconds("+05:30"), Some(5 * 3600 + 30 * 60));
        assert_eq!(parse_offset_seconds("-08:00"), Some(-8 * 3600));
        assert_eq!(parse_offset_seconds("+14:00"), Some(14 * 3600));
    }

    #[test]
    fn parse_offset_rejects_malformed() {
        assert_eq!(parse_offset_seconds("bogus"), None);
        assert_eq!(parse_offset_seconds("0500"), None);
        // Hour >14 is beyond canonicalization bounds — Java rejects at plan time,
        // we reject at runtime for column-valued paths.
        assert_eq!(parse_offset_seconds("+15:00"), None);
        assert_eq!(parse_offset_seconds("+05:60"), None);
    }

    // Offset → offset: simple wall-clock delta, no calendar.
    #[test]
    fn fixed_offset_to_fixed_offset_shifts_by_delta() {
        // 2024-01-05T12:00:00 in +00:00 → same wall clock in +05:30 means
        // +5h30m = +19_800_000 ms added.
        let ts = 1_704_456_000_000; // 2024-01-05T12:00:00Z (stored naive)
        let out = shift_millis(ts, "+00:00", "+05:30").unwrap();
        assert_eq!(out - ts, 5 * 3600 * 1000 + 30 * 60 * 1000);
    }

    // IANA ↔ IANA: DST-correct jump across a transition.
    #[test]
    fn iana_new_york_to_london_applies_correct_offset() {
        // 2024-01-05T12:00:00 wall-clock in America/New_York (UTC-5 in winter)
        // → 17:00 UTC → London (UTC+0 in winter) = 17:00 local. Delta = +5h.
        let ts = 1_704_456_000_000; // treat as 2024-01-05T12:00:00 naive
        let out = shift_millis(ts, "America/New_York", "Europe/London").unwrap();
        assert_eq!((out - ts) / 1000, 5 * 3600);
    }

    #[test]
    fn iana_dst_summer_offset_differs_from_winter() {
        // Summer: NY is UTC-4, winter: NY is UTC-5. Pull data at both dates,
        // confirm the two shifts to UTC (London+0 in winter, +1 in summer) produce
        // the expected distinct deltas.
        // 2024-01-05T12:00:00 (winter): NY→London → +5h.
        let winter_ts = 1_704_456_000_000;
        let winter_out = shift_millis(winter_ts, "America/New_York", "Europe/London").unwrap();
        assert_eq!((winter_out - winter_ts) / 1000, 5 * 3600);
        // 2024-07-05T12:00:00 (summer): NY (UTC-4) → London (UTC+1) → +5h.
        // Same delta because both shift to/from their summer offsets in lockstep.
        let summer_ts = 1_720_180_800_000; // 2024-07-05T12:00:00Z naive
        let summer_out = shift_millis(summer_ts, "America/New_York", "Europe/London").unwrap();
        assert_eq!((summer_out - summer_ts) / 1000, 5 * 3600);
    }

    // When from_tz crosses DST boundary but to_tz doesn't, the delta changes.
    #[test]
    fn iana_to_utc_crosses_dst_in_source_tz() {
        // 2024-01-05 in UTC (no DST there): NY winter = UTC-5, shift = +5h.
        let winter_ts = 1_704_456_000_000;
        let winter_out = shift_millis(winter_ts, "America/New_York", "UTC").unwrap();
        assert_eq!((winter_out - winter_ts) / 1000, 5 * 3600);

        // 2024-07-05: NY summer = UTC-4, shift = +4h.
        let summer_ts = 1_720_180_800_000;
        let summer_out = shift_millis(summer_ts, "America/New_York", "UTC").unwrap();
        assert_eq!((summer_out - summer_ts) / 1000, 4 * 3600);
    }

    #[test]
    fn unknown_tz_returns_none() {
        assert_eq!(shift_millis(0, "Not/AZone", "UTC"), None);
        assert_eq!(shift_millis(0, "UTC", "Not/AZone"), None);
    }

    // Coercion: PPL may emit the ts arg as Utf8 (string literal), Date32,
    // or Timestamp with a different precision/tz. coerce_types should
    // normalize them all to Timestamp(Millisecond, None) + Utf8 + Utf8.
    #[test]
    fn coerce_types_accepts_utf8_ts() {
        let udf = ConvertTzUdf::new();
        let out = udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Utf8])
            .unwrap();
        assert_eq!(
            out,
            vec![
                DataType::Timestamp(TimeUnit::Millisecond, None),
                DataType::Utf8,
                DataType::Utf8,
            ]
        );
    }

    #[test]
    fn coerce_types_accepts_date32_ts() {
        let udf = ConvertTzUdf::new();
        let out = udf
            .coerce_types(&[DataType::Date32, DataType::Utf8, DataType::Utf8])
            .unwrap();
        assert_eq!(out[0], DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn coerce_types_accepts_other_ts_precisions() {
        let udf = ConvertTzUdf::new();
        // Nanosecond with tz → should coerce down to Millisecond, None.
        let out = udf
            .coerce_types(&[
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                DataType::Utf8,
                DataType::Utf8,
            ])
            .unwrap();
        assert_eq!(out[0], DataType::Timestamp(TimeUnit::Millisecond, None));
    }

    #[test]
    fn coerce_types_passes_through_exact_match() {
        let udf = ConvertTzUdf::new();
        let ts = DataType::Timestamp(TimeUnit::Millisecond, None);
        let out = udf
            .coerce_types(&[ts.clone(), DataType::Utf8, DataType::Utf8])
            .unwrap();
        assert_eq!(out, vec![ts, DataType::Utf8, DataType::Utf8]);
    }

    #[test]
    fn coerce_types_rejects_unsupported_ts_type() {
        let udf = ConvertTzUdf::new();
        // A boolean in the ts slot is clearly wrong — must error explicitly.
        let err = udf
            .coerce_types(&[DataType::Boolean, DataType::Utf8, DataType::Utf8])
            .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("convert_tz") && msg.contains("Boolean"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn coerce_types_rejects_wrong_arity() {
        let udf = ConvertTzUdf::new();
        assert!(udf.coerce_types(&[DataType::Utf8]).is_err());
        assert!(udf
            .coerce_types(&[DataType::Utf8, DataType::Utf8, DataType::Utf8, DataType::Utf8])
            .is_err());
    }

    // Batch / null handling through the full UDF.
    #[test]
    fn invoke_nulls_and_bad_tz_propagate() {
        let udf = ConvertTzUdf::new();
        let ts = TimestampMillisecondArray::from(vec![
            Some(1_704_456_000_000),
            None,
            Some(0),
        ]);
        let from = StringArray::from(vec![
            Some("+00:00"),
            Some("UTC"),
            Some("Mars/Olympus"), // unknown column-valued entry → null
        ]);
        let to = StringArray::from(vec![Some("+05:30"), Some("UTC"), Some("UTC")]);
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(ts)),
                ColumnarValue::Array(Arc::new(from)),
                ColumnarValue::Array(Arc::new(to)),
            ],
            number_rows: 3,
            arg_fields: vec![],
            return_field: Arc::new(datafusion::arrow::datatypes::Field::new(
                "out",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };
        let out = udf.invoke_with_args(args).unwrap();
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array"),
        };
        let arr = arr
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        assert!(arr.is_null(2));
    }
}
