/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! conv(n, fromBase, toBase) — base conversion. Mirrors PPL's `ConvFunction`:
//! `Long.toString(Long.parseLong(n, fromBase), toBase)`. See
//! `sql/core/.../ConvFunction.java`. NULL for invalid input / unsupported base.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::json_common::StringArrayView;
use super::{coerce_args, udf_identity, CoerceMode};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(ConvUdf::new()));
}

/// `conv(varchar, integer, integer)` → `varchar`.
#[derive(Debug)]
pub struct ConvUdf {
    signature: Signature,
}

impl ConvUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(ConvUdf, "conv");

impl ScalarUDFImpl for ConvUdf {
    fn name(&self) -> &str {
        "conv"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_args(
            "conv",
            arg_types,
            &[CoerceMode::Utf8, CoerceMode::Int64, CoerceMode::Int64],
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return exec_err!("conv expects exactly 3 arguments, got {}", args.args.len());
        }
        let n = args.number_rows;
        let n_arr = args.args[0].clone().into_array(n)?;
        let from_arr = args.args[1].clone().into_array(n)?;
        let to_arr = args.args[2].clone().into_array(n)?;

        // n is a string slot. CoerceMode::Utf8 passes the variant through unchanged, so the
        // array may arrive as Utf8, LargeUtf8, or (on the data-node → coordinator wire)
        // Utf8View. Read it through the variant-agnostic view rather than downcasting to a
        // single concrete array type.
        let n_str = StringArrayView::from_array(&n_arr)?;
        let from_int = downcast_i64(&from_arr, "fromBase")?;
        let to_int = downcast_i64(&to_arr, "toBase")?;

        let mut builder = StringBuilder::with_capacity(n, n * 16);
        for i in 0..n {
            let n_val = match n_str.cell(i) {
                Some(v) if !from_int.is_null(i) && !to_int.is_null(i) => v,
                _ => {
                    builder.append_null();
                    continue;
                }
            };
            let from_base = from_int.value(i);
            let to_base = to_int.value(i);
            // PPL's ConvFunction surfaces Java's Long.parseLong NumberFormatException for
            // out-of-range radix. testConvWithInvalidRadix asserts the message text matches
            // verbatim — keep the Java phrasing so the rest layer's 5xx message contains it.
            if from_base < 2 {
                return exec_err!("radix {from_base} less than Character.MIN_RADIX");
            }
            if from_base > 36 {
                return exec_err!("radix {from_base} greater than Character.MAX_RADIX");
            }
            if to_base < 2 {
                return exec_err!("radix {to_base} less than Character.MIN_RADIX");
            }
            if to_base > 36 {
                return exec_err!("radix {to_base} greater than Character.MAX_RADIX");
            }
            match conv_one(n_val, from_base, to_base) {
                Some(s) => builder.append_value(&s),
                None => builder.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn downcast_i64<'a>(arr: &'a ArrayRef, slot: &str) -> Result<&'a Int64Array> {
    arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "conv: slot '{slot}' expected Int64 post-coerce, got {:?}",
            arr.data_type()
        ))
    })
}

/// Parse `s` as an integer in `from_base`, format in `to_base`. NULL on unparseable input.
/// Caller is responsible for validating the base range (so invalid radix surfaces as a
/// hard error, matching Java's `NumberFormatException`).
fn conv_one(s: &str, from_base: i64, to_base: i64) -> Option<String> {
    let parsed = i64::from_str_radix(s.trim(), from_base as u32).ok()?;
    Some(format_radix(parsed, to_base as u32))
}

fn format_radix(value: i64, base: u32) -> String {
    if value == 0 {
        return "0".to_string();
    }
    let negative = value < 0;
    // i128 path so i64::MIN's absolute value doesn't overflow. Lowercase digits to
    // match Java's `Long.toString(long, int radix)` — uppercase here would fail
    // tests that assert against lowercased base-36 strings ("snoopy", "hello").
    let mut abs = (value as i128).unsigned_abs();
    let mut digits = String::new();
    while abs > 0 {
        let d = (abs % base as u128) as u32;
        digits.push(char::from_digit(d, base).unwrap());
        abs /= base as u128;
    }
    let mut out = String::with_capacity(digits.len() + 1);
    if negative {
        out.push('-');
    }
    out.extend(digits.chars().rev());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_to_binary() {
        assert_eq!(conv_one("11", 10, 2), Some("1011".to_string()));
    }

    #[test]
    fn hex_to_decimal_uppercase_input() {
        // i64::from_str_radix accepts both cases for input digits.
        assert_eq!(conv_one("FF", 16, 10), Some("255".to_string()));
    }

    #[test]
    fn output_is_lowercase_to_match_java_long_tostring() {
        // 29234652 base 10 → base 36 = "hello" (matches testConvAndLower).
        assert_eq!(conv_one("29234652", 10, 36), Some("hello".to_string()));
        // 1732835878 base 10 → base 36 = "snoopy" (matches testConvNegateValue).
        assert_eq!(conv_one("1732835878", 10, 36), Some("snoopy".to_string()));
    }

    #[test]
    fn negative_round_trip() {
        assert_eq!(conv_one("-100", 10, 16), Some("-64".to_string()));
    }

    #[test]
    fn zero_round_trip() {
        assert_eq!(conv_one("0", 10, 36), Some("0".to_string()));
    }

    #[test]
    fn unparseable_yields_none() {
        assert_eq!(conv_one("xyz", 10, 2), None);
    }

    #[test]
    fn invoke_accepts_utf8view_input() {
        use datafusion::arrow::array::{Int64Array, StringArray, StringViewArray};
        use datafusion::arrow::datatypes::Field;

        // Strings cross the data-node → coordinator wire as Utf8View; CoerceMode::Utf8
        // passes the variant through, so conv must read the StringViewArray directly
        // rather than downcasting to StringArray (Utf8).
        let n: ArrayRef = Arc::new(StringViewArray::from(vec![Some("11"), Some("FF"), None]));
        let from: ArrayRef = Arc::new(Int64Array::from(vec![10, 16, 10]));
        let to: ArrayRef = Arc::new(Int64Array::from(vec![2, 10, 2]));

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(n),
                ColumnarValue::Array(from),
                ColumnarValue::Array(to),
            ],
            arg_fields: vec![
                Arc::new(Field::new("n", DataType::Utf8View, true)),
                Arc::new(Field::new("from", DataType::Int64, true)),
                Arc::new(Field::new("to", DataType::Int64, true)),
            ],
            number_rows: 3,
            return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
            config_options: Arc::new(datafusion::config::ConfigOptions::new()),
        };

        let out = ConvUdf::new()
            .invoke_with_args(args)
            .expect("conv must accept Utf8View");
        let arr = match out {
            ColumnarValue::Array(a) => a,
            _ => panic!("expected array output"),
        };
        let s = arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 output");
        assert_eq!(s.value(0), "1011"); // 11 base10 → base2
        assert_eq!(s.value(1), "255"); // FF base16 → base10
        assert!(s.is_null(2)); // null input → null
    }
}
