/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! OpenSearch scalar UDFs that aren't in DataFusion's built-in registry. Each
//! must have a matching YAML entry in `extensions/opensearch_scalar.yaml` so
//! the substrait converter on the Java side can route to it by name.
//!
//! Functions registered here:
//! - `convert_tz(ts, from_tz, to_tz)` — DST-aware timezone shift (chrono-tz)

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;

/// Categories of input type a UDF slot can accept. Each mode declares a
/// canonical target arrow type plus the set of sources that coerce to it.
/// UDFs use `Signature::user_defined()` and call [`coerce_slot`] per
/// argument position to produce the `coerce_types` output.
///
/// Invalid sources produce an explicit `plan_err!` — no silent fallback. The
/// failure message names the UDF, the slot index, the observed type and the
/// expected canonical type so planning errors are actionable.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub(crate) enum CoerceMode {
    /// Accept Utf8 / Date32 / Timestamp(any precision, any tz) → canonicalize
    /// to Timestamp(Millisecond, None). DF has built-in casts for each source.
    TimestampMs,
    /// Accept Utf8 / Date32 / Timestamp(any, any) → canonicalize to Date32.
    Date32,
    /// Accept any integer or float → Int64.
    Int64,
    /// Accept any integer or float → Float64.
    Float64,
    /// Accept Utf8 / LargeUtf8 / Utf8View → Utf8.
    Utf8,
}

/// Coerce a single argument slot. Returns the canonical target type for this
/// slot when the input is compatible, or a planning error otherwise.
pub(crate) fn coerce_slot(
    udf_name: &str,
    slot_index: usize,
    observed: &DataType,
    mode: CoerceMode,
) -> Result<DataType> {
    use DataType::*;
    match mode {
        CoerceMode::TimestampMs => match observed {
            Timestamp(_, _) | Date32 | Date64 | Utf8 | LargeUtf8 | Utf8View => {
                Ok(Timestamp(TimeUnit::Millisecond, None))
            }
            other => plan_err!(
                "{udf_name}: arg {slot_index} expected timestamp/date/string, got {other:?}"
            ),
        },
        CoerceMode::Date32 => match observed {
            Date32 | Date64 | Timestamp(_, _) | Utf8 | LargeUtf8 | Utf8View => Ok(Date32),
            other => plan_err!(
                "{udf_name}: arg {slot_index} expected date/timestamp/string, got {other:?}"
            ),
        },
        CoerceMode::Int64 => match observed {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                Ok(Int64)
            }
            other => plan_err!(
                "{udf_name}: arg {slot_index} expected integer or float, got {other:?}"
            ),
        },
        CoerceMode::Float64 => match observed {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                Ok(Float64)
            }
            other => plan_err!(
                "{udf_name}: arg {slot_index} expected integer or float, got {other:?}"
            ),
        },
        CoerceMode::Utf8 => match observed {
            Utf8 | LargeUtf8 | Utf8View => Ok(Utf8),
            other => plan_err!(
                "{udf_name}: arg {slot_index} expected string, got {other:?}"
            ),
        },
    }
}

/// Coerce an entire argument vector against a fixed template. Enforces arity
/// and delegates per-slot coercion to [`coerce_slot`].
pub(crate) fn coerce_args(
    udf_name: &str,
    observed: &[DataType],
    template: &[CoerceMode],
) -> Result<Vec<DataType>> {
    if observed.len() != template.len() {
        return plan_err!(
            "{udf_name} expects {} arguments, got {}",
            template.len(),
            observed.len()
        );
    }
    template
        .iter()
        .enumerate()
        .map(|(i, mode)| coerce_slot(udf_name, i, &observed[i], *mode))
        .collect()
}

pub mod convert_tz;
pub mod tonumber;
pub mod tostring;

pub fn register_all(ctx: &SessionContext) {
    convert_tz::register_all(ctx);
    tonumber::register_all(ctx);
    tostring::register_all(ctx);
    log::info!("OpenSearch UDF register_all: convert_tz, tonumber, tostring registered");
}

#[cfg(test)]
mod tests {
    //! Direct tests for the [`CoerceMode`] helper library. `convert_tz` exercises
    //! `TimestampMs` and `Utf8` through its public `coerce_types`; these tests
    //! cover every mode's accept + reject paths so future UDFs that pick up
    //! `Date32`, `Int64`, or `Float64` inherit a proven helper rather than being
    //! the first caller.
    use super::{coerce_args, coerce_slot, CoerceMode};
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    fn ts_ms() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    }

    // ── TimestampMs ────────────────────────────────────────────────────────
    #[test]
    fn timestampms_accepts_every_temporal_source() {
        for observed in [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ] {
            let result = coerce_slot("t", 0, &observed, CoerceMode::TimestampMs).unwrap();
            assert_eq!(result, ts_ms(), "TimestampMs should canonicalize {observed:?}");
        }
    }

    #[test]
    fn timestampms_rejects_numeric() {
        let err = coerce_slot("t", 0, &DataType::Int64, CoerceMode::TimestampMs).unwrap_err();
        assert!(err.to_string().contains("expected timestamp/date/string"));
    }

    // ── Date32 ─────────────────────────────────────────────────────────────
    #[test]
    fn date32_accepts_date_and_string_sources() {
        for observed in [
            DataType::Date32,
            DataType::Date64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ] {
            let result = coerce_slot("d", 0, &observed, CoerceMode::Date32).unwrap();
            assert_eq!(result, DataType::Date32);
        }
    }

    #[test]
    fn date32_rejects_numeric() {
        let err = coerce_slot("d", 0, &DataType::Float64, CoerceMode::Date32).unwrap_err();
        assert!(err.to_string().contains("expected date/timestamp/string"));
    }

    // ── Int64 ──────────────────────────────────────────────────────────────
    #[test]
    fn int64_accepts_every_number() {
        for observed in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
        ] {
            let result = coerce_slot("i", 0, &observed, CoerceMode::Int64).unwrap();
            assert_eq!(result, DataType::Int64);
        }
    }

    #[test]
    fn int64_rejects_strings() {
        let err = coerce_slot("i", 0, &DataType::Utf8, CoerceMode::Int64).unwrap_err();
        assert!(err.to_string().contains("expected integer or float"));
    }

    // ── Float64 ────────────────────────────────────────────────────────────
    #[test]
    fn float64_accepts_every_number() {
        for observed in [
            DataType::Int32,
            DataType::Int64,
            DataType::UInt32,
            DataType::Float32,
            DataType::Float64,
        ] {
            let result = coerce_slot("f", 0, &observed, CoerceMode::Float64).unwrap();
            assert_eq!(result, DataType::Float64);
        }
    }

    #[test]
    fn float64_rejects_strings() {
        let err = coerce_slot("f", 0, &DataType::Utf8, CoerceMode::Float64).unwrap_err();
        assert!(err.to_string().contains("expected integer or float"));
    }

    // ── Utf8 ───────────────────────────────────────────────────────────────
    #[test]
    fn utf8_accepts_every_string_variant() {
        for observed in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let result = coerce_slot("s", 0, &observed, CoerceMode::Utf8).unwrap();
            assert_eq!(result, DataType::Utf8);
        }
    }

    #[test]
    fn utf8_rejects_numeric_and_temporal() {
        for observed in [DataType::Int64, DataType::Float64, DataType::Date32] {
            let err = coerce_slot("s", 0, &observed, CoerceMode::Utf8).unwrap_err();
            assert!(err.to_string().contains("expected string"));
        }
    }

    // ── coerce_args ────────────────────────────────────────────────────────
    #[test]
    fn coerce_args_maps_each_slot_through_its_mode() {
        let observed = [DataType::Utf8, DataType::Int32];
        let template = [CoerceMode::TimestampMs, CoerceMode::Int64];
        let result = coerce_args("multi", &observed, &template).unwrap();
        assert_eq!(result, vec![ts_ms(), DataType::Int64]);
    }

    #[test]
    fn coerce_args_rejects_arity_mismatch() {
        let observed = [DataType::Utf8];
        let template = [CoerceMode::Utf8, CoerceMode::Utf8];
        let err = coerce_args("arity", &observed, &template).unwrap_err();
        assert!(err.to_string().contains("expects 2 arguments, got 1"));
    }

    #[test]
    fn coerce_args_propagates_slot_errors() {
        let observed = [DataType::Utf8, DataType::Utf8];
        let template = [CoerceMode::Utf8, CoerceMode::Int64];
        let err = coerce_args("slot", &observed, &template).unwrap_err();
        assert!(
            err.to_string().contains("arg 1"),
            "error must name the failing slot index, got: {err}"
        );
    }
}
