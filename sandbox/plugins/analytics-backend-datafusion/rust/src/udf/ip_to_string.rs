/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! ip_to_string(bytes) — render a 16-byte ipv6-mapped buffer as a canonical IP string.
//!
//! Bound from `IpBinaryCastFunctionAdapter` (Java) which rewrites
//! `CAST(<IpType> AS VARCHAR)` to a call into this UDF, so the output is
//! formatted server-side by the analytics backend rather than getting
//! buffer-reinterpreted as Latin-1 by DataFusion's built-in `cast(binary,
//! utf8)` kernel.
//!
//! Output format matches OpenSearch core's `InetAddresses.toAddrString`:
//!   - IPv4-mapped IPv6 (10 zero bytes + `0xff 0xff` + 4 IPv4 bytes) → dotted-quad
//!     `"a.b.c.d"` (matches what the legacy `IpFieldMapper` emits).
//!   - Pure IPv6 → `Ipv6Addr::to_string()`'s RFC 5952 compressed form
//!     (e.g. `"::1"`, `"2001:db8::1"`).
//!
//! Buffers that aren't 16 bytes (defensive — should never happen for parquet-
//! encoded ip columns) yield NULL rather than panic.

use std::hash::{Hash, Hasher};
use std::net::Ipv6Addr;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(IpToStringUdf::new()));
}

/// `ip_to_string(binary)` → `varchar`.
#[derive(Debug)]
pub struct IpToStringUdf {
    signature: Signature,
}

impl IpToStringUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Exact(vec![DataType::Binary])],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for IpToStringUdf {
    fn default() -> Self {
        Self::new()
    }
}

// `ScalarUDFImpl` requires DynEq + DynHash. Stateless — all instances are equivalent.
impl PartialEq for IpToStringUdf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for IpToStringUdf {}
impl Hash for IpToStringUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "ip_to_string".hash(state);
    }
}

impl ScalarUDFImpl for IpToStringUdf {
    fn name(&self) -> &str {
        "ip_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!(
                "ip_to_string expects exactly 1 argument, got {}",
                args.args.len()
            );
        }
        match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Binary(opt)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Utf8(opt.as_ref().and_then(|b| format_ip_bytes(b))),
            )),
            ColumnarValue::Scalar(other) => {
                exec_err!("ip_to_string: expected Binary input, got {other:?}")
            }
            ColumnarValue::Array(arr) => {
                let bin = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                    datafusion::common::DataFusionError::Execution(format!(
                        "ip_to_string: expected BinaryArray, got {:?}",
                        arr.data_type()
                    ))
                })?;
                let out: StringArray = (0..bin.len())
                    .map(|i| {
                        if bin.is_null(i) {
                            None
                        } else {
                            format_ip_bytes(bin.value(i))
                        }
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }
        }
    }
}

/// Format a 16-byte ipv6-mapped buffer the way OpenSearch core's
/// `InetAddresses.toAddrString` does. Returns `None` for buffers that aren't
/// 16 bytes (defensive — should never happen for valid parquet-encoded ip data).
fn format_ip_bytes(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 16 {
        return None;
    }
    // IPv4-mapped IPv6: 80 bits of zero, then 0xffff, then 4 bytes of IPv4.
    if bytes[..10].iter().all(|&b| b == 0) && bytes[10] == 0xff && bytes[11] == 0xff {
        return Some(format!(
            "{}.{}.{}.{}",
            bytes[12], bytes[13], bytes[14], bytes[15]
        ));
    }
    let arr: [u8; 16] = bytes.try_into().expect("checked length above");
    Some(Ipv6Addr::from(arr).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, AsArray, BinaryArray};
    use datafusion::arrow::datatypes::Field;

    fn udf() -> IpToStringUdf {
        IpToStringUdf::new()
    }

    fn invoke_scalar(value: ScalarValue) -> Result<ColumnarValue> {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Binary, true))],
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        u.invoke_with_args(args)
    }

    fn as_utf8(v: ColumnarValue) -> Option<String> {
        match v {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt)) => opt,
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
    }

    fn ipv4_mapped_bytes(a: u8, b: u8, c: u8, d: u8) -> Vec<u8> {
        let mut bytes = vec![0u8; 16];
        bytes[10] = 0xff;
        bytes[11] = 0xff;
        bytes[12] = a;
        bytes[13] = b;
        bytes[14] = c;
        bytes[15] = d;
        bytes
    }

    #[test]
    fn formats_ipv4_mapped_as_dotted_quad() {
        let out = invoke_scalar(ScalarValue::Binary(Some(ipv4_mapped_bytes(1, 2, 3, 4)))).unwrap();
        assert_eq!(as_utf8(out).unwrap(), "1.2.3.4");
    }

    #[test]
    fn formats_pure_ipv6_loopback() {
        let mut bytes = vec![0u8; 16];
        bytes[15] = 1;
        let out = invoke_scalar(ScalarValue::Binary(Some(bytes))).unwrap();
        assert_eq!(as_utf8(out).unwrap(), "::1");
    }

    #[test]
    fn formats_pure_ipv6_arbitrary() {
        // 2001:db8::1
        let bytes: Vec<u8> = vec![0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        let out = invoke_scalar(ScalarValue::Binary(Some(bytes))).unwrap();
        assert_eq!(as_utf8(out).unwrap(), "2001:db8::1");
    }

    #[test]
    fn null_input_yields_null() {
        let out = invoke_scalar(ScalarValue::Binary(None)).unwrap();
        assert!(as_utf8(out).is_none());
    }

    #[test]
    fn wrong_length_yields_null() {
        let out = invoke_scalar(ScalarValue::Binary(Some(vec![1, 2, 3, 4]))).unwrap();
        assert!(as_utf8(out).is_none());
    }

    #[test]
    fn array_input_preserves_null_mask() {
        let u = udf();
        let return_field = Arc::new(Field::new(u.name(), DataType::Utf8, true));
        let v4 = ipv4_mapped_bytes(1, 2, 3, 4);
        let mut v6 = vec![0u8; 16];
        v6[15] = 1;
        let values: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(v4.as_slice()),
            None,
            Some(v6.as_slice()),
        ]));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(values)],
            arg_fields: vec![Arc::new(Field::new("v", DataType::Binary, true))],
            number_rows: 3,
            return_field,
            config_options: Arc::new(Default::default()),
        };
        match u.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(arr) => {
                let s = arr.as_string::<i32>();
                assert_eq!(s.value(0), "1.2.3.4");
                assert!(s.is_null(1));
                assert_eq!(s.value(2), "::1");
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }
}
