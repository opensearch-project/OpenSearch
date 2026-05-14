/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! OpenSearch scalar UDFs not in DataFusion's builtins. Each registered UDF must
//! have a matching YAML entry in `opensearch_scalar_functions.yaml`.

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;

/// Emit the `Default`, `PartialEq`, `Eq`, and `Hash` impls every stateless
/// UDF needs. All instances of the same UDF type are semantically identical
/// — they compare equal and hash to a single name-derived stable value.
macro_rules! udf_identity {
    ($udf:ident, $name:literal) => {
        impl Default for $udf {
            fn default() -> Self {
                Self::new()
            }
        }
        impl PartialEq for $udf {
            fn eq(&self, _: &Self) -> bool {
                true
            }
        }
        impl Eq for $udf {}
        impl std::hash::Hash for $udf {
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                $name.hash(state);
            }
        }
    };
}
pub(crate) use udf_identity;

/// Input-type categories for UDF argument slots. Each mode declares a canonical
/// arrow target type plus the set of sources that coerce to it; invalid sources
/// produce a `plan_err!` naming the UDF, slot index, and observed type.
#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub(crate) enum CoerceMode {
    TimestampMs,
    Date32,
    Int64,
    Float64,
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
            other => {
                plan_err!("{udf_name}: arg {slot_index} expected integer or float, got {other:?}")
            }
        },
        CoerceMode::Float64 => match observed {
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64 => {
                Ok(Float64)
            }
            other => {
                plan_err!("{udf_name}: arg {slot_index} expected integer or float, got {other:?}")
            }
        },
        CoerceMode::Utf8 => match observed {
            Utf8 | LargeUtf8 | Utf8View => Ok(Utf8),
            other => plan_err!("{udf_name}: arg {slot_index} expected string, got {other:?}"),
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
pub mod crc32;
pub mod date_format;
pub mod extract;
pub mod from_unixtime;
pub mod json_append;
pub mod json_array_length;
pub(crate) mod json_common;
pub mod json_delete;
pub mod json_extend;
pub mod json_extract;
pub mod json_keys;
pub mod json_set;
pub mod makedate;
pub mod maketime;
pub mod mvappend;
pub mod mvfind;
pub mod mvzip;
pub(crate) mod mysql_format;
pub mod sha1;
pub mod str_to_date;
pub mod strftime;
pub mod time_format;
pub mod tonumber;
pub mod tostring;

// Dev note: if a freshly added UDF here fails at runtime with
// "Unsupported function name: <X>" despite the Java side being wired, the
// native dylib is stale. `sandbox/libs/dataformat-native/build.gradle` tracks
// only common/ + lib/ as inputs, so plugin-side Rust edits leave gradle
// UP-TO-DATE. Workaround: run
// `./gradlew :sandbox:libs:dataformat-native:buildRustLibrary --rerun-tasks`
// and restart the OpenSearch JVM (the loaded dylib is JVM-cached).
pub fn register_all(ctx: &SessionContext) {
    convert_tz::register_all(ctx);
    crc32::register_all(ctx);
    date_format::register_all(ctx);
    extract::register_all(ctx);
    from_unixtime::register_all(ctx);
    json_append::register_all(ctx);
    json_array_length::register_all(ctx);
    json_delete::register_all(ctx);
    json_extend::register_all(ctx);
    json_extract::register_all(ctx);
    json_keys::register_all(ctx);
    json_set::register_all(ctx);
    makedate::register_all(ctx);
    maketime::register_all(ctx);
    mvappend::register_all(ctx);
    mvfind::register_all(ctx);
    mvzip::register_all(ctx);
    sha1::register_all(ctx);
    str_to_date::register_all(ctx);
    strftime::register_all(ctx);
    time_format::register_all(ctx);
    tonumber::register_all(ctx);
    tostring::register_all(ctx);
    log::info!(
        "OpenSearch UDF register_all: convert_tz, crc32, date_format, extract, from_unixtime, json_append, json_array_length, json_delete, json_extend, json_extract, json_keys, json_set, makedate, maketime, mvappend, mvfind, mvzip, sha1, str_to_date, strftime, time_format, tonumber, tostring registered"
    );
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
            assert_eq!(
                result,
                ts_ms(),
                "TimestampMs should canonicalize {observed:?}"
            );
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
