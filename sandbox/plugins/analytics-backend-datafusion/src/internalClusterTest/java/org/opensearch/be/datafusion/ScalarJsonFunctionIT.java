/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

/**
 * End-to-end smoke tests for PPL {@code json_*} scalar functions routed through
 * PPL → Calcite → Substrait → DataFusion. Mirrors the {@link ScalarDateTimeFunctionIT}
 * one-method-per-function pattern: each method exercises literal-arg and
 * column-valued paths, edge cases are covered in Rust unit tests.
 *
 * <p>Bank fixture (see {@link BaseScalarFunctionIT}) carries a {@code json_str}
 * keyword column — row 1 is {@code "[1,2,3]"}, row 6 is {@code "{\"k\":1}"}. The
 * {@code where account_number=1} row pin baked into
 * {@link BaseScalarFunctionIT#evalScalar} prevents Calcite const-folding the
 * outer query, and literals in the eval expression still traverse the full
 * Substrait + UDF path end-to-end (the adapter, the Sig, the UDF's coerce_types
 * and invoke_with_args all fire).
 */
public class ScalarJsonFunctionIT extends BaseScalarFunctionIT {

    /**
     * Covers happy path + NULL-on-non-array + NULL-on-malformed via the scalar
     * fast-path, plus a column-valued invocation that exercises the Arrow
     * columnar path in {@code invoke_with_args}.
     */
    public void testJsonArrayLength() {
        // Scalar fast-path — literal arg constant-folded in Substrait, forced
        // through the UDF by the row-pin in evalScalar.
        assertScalarIntStrict("json_array_length('[1,2,3]')", 3);
        assertScalarIntStrict("json_array_length('[]')", 0);
        assertScalarNull("json_array_length('{\"k\":1}')");
        assertScalarNull("json_array_length('not-json')");

        // Columnar path — real keyword-typed field from the bank fixture. Row
        // 1's json_str is '[1,2,3]' → length 3. This is the branch production
        // traffic actually hits; literal-only coverage leaves it unexercised.
        assertScalarIntStrict("json_array_length(json_str)", 3);
    }

    /**
     * Parity replay of the legacy SQL plugin's
     * {@code CalcitePPLJsonBuiltinFunctionIT.testJsonArrayLength} fixture
     * (query returns rows 4, 5, null). Encodes the parity relationship as an
     * automated assertion so future changes can't silently diverge from legacy.
     */
    public void testJsonArrayLengthParityWithLegacy() {
        assertScalarIntStrict("json_array_length('[1,2,3,4]')", 4);
        assertScalarIntStrict("json_array_length('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]')", 5);
        assertScalarNull("json_array_length('{\"key\": 1}')");
    }
}
