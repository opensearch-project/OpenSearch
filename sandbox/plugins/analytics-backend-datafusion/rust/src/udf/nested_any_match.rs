/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Placeholder registration for `nested_any_match`.
//!
//! This UDF is never executed. `NestedAnyMatchRewriteRule` rewrites every
//! `nested_any_match` call to a native `array_any_match` HOF before the
//! physical planner runs. The UDF registration exists only so the Substrait
//! consumer can resolve the function name when building the logical plan.
//!
//! TODO(native-array_any_match): delete this placeholder once a real array_any_match lambda
//! round-trips through Substrait.

use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

use super::udf_identity;

#[derive(Debug)]
pub struct NestedAnyMatch {
    signature: Signature,
}

udf_identity!(NestedAnyMatch, "nested_any_match");

impl NestedAnyMatch {
    pub fn new() -> Self {
        // Accept any 2-argument call — the rewrite rule replaces this before type-checking matters.
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NestedAnyMatch {
    fn name(&self) -> &str {
        "nested_any_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        datafusion::common::internal_err!(
            "nested_any_match placeholder invoked — NestedAnyMatchRewriteRule must have failed to rewrite"
        )
    }
}

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(NestedAnyMatch::new()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::FunctionRegistry;
    use datafusion::logical_expr::ScalarUDFImpl;

    #[test]
    fn reports_name_and_boolean_return() {
        let udf = NestedAnyMatch::new();
        assert_eq!(udf.name(), "nested_any_match");
        assert_eq!(udf.return_type(&[]).unwrap(), DataType::Boolean);
    }

    #[test]
    fn register_all_makes_the_name_resolvable() {
        // The whole point of the placeholder: the Substrait consumer can resolve the name.
        let ctx = SessionContext::new();
        register_all(&ctx);
        assert!(ctx.udf("nested_any_match").is_ok());
    }
}
