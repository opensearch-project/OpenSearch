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

    /**
     * Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonKeys}:
     * object input yields JSON-array-encoded keys (insertion order); array /
     * non-object inputs yield NULL. `preserve_order` on serde_json in the
     * DataFusion crate preserves insertion order to match legacy LinkedHashMap.
     */
    public void testJsonKeysParityWithLegacy() {
        assertScalarString("json_keys('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}')", "[\"f1\",\"f2\"]");
        assertScalarNull("json_keys('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]')");
        assertScalarNull("json_keys('not-json')");
        assertScalarNull("json_keys('42')");
    }

    /**
     * Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonExtract},
     * {@code testJsonExtractWithMultiplyResult},
     * {@code testJsonExtractReturnsNullForMissingPathAndNullValue} and
     * {@code testJsonExtractMultiPathWithMissingPath}. The legacy fixture's
     * byte-for-byte expected output survives our serde_json round-trip because
     * {@code preserve_order} is enabled and no integer↔double coercion happens
     * (see {@code rust/Cargo.toml}'s serde_json feature declaration).
     */
    public void testJsonExtractParityWithLegacy() {
        String candidate = "[{\"name\":\"London\",\"Bridges\":[{\"name\":\"Tower Bridge\",\"length\":801.0},"
            + "{\"name\":\"Millennium Bridge\",\"length\":1066.0}]},"
            + "{\"name\":\"Venice\",\"Bridges\":[{\"name\":\"Rialto Bridge\",\"length\":157.0},"
            + "{\"type\":\"Bridge of Sighs\",\"length\":36.0},"
            + "{\"type\":\"Ponte della Paglia\"}]},"
            + "{\"name\":\"San Francisco\",\"Bridges\":[{\"name\":\"Golden Gate Bridge\",\"length\":8981.0},"
            + "{\"name\":\"Bay Bridge\",\"length\":23556.0}]}]";

        // Single-path, wildcard-at-root over top-level array → 3 matches wrapped
        // in a JSON array. Round-tripped bytes equal the input because
        // preserve_order + no numeric coercion.
        assertScalarString("json_extract('" + candidate + "', '{}')", candidate);

        // Single-path scalar match — legacy `.toString()` on Double(8981.0).
        assertScalarString("json_extract('" + candidate + "', '{2}.Bridges{0}.length')", "8981.0");

        // Wildcard-over-wildcard-missing-key: only Venice entries without a
        // `name` field expose a `type`, so two matches wrap into a JSON array.
        assertScalarString("json_extract('" + candidate + "', '{}.Bridges{}.type')", "[\"Bridge of Sighs\",\"Ponte della Paglia\"]");

        // Single-path object match — jsonized with insertion order preserved.
        assertScalarString("json_extract('" + candidate + "', '{2}.Bridges{0}')", "{\"name\":\"Golden Gate Bridge\",\"length\":8981.0}");

        // Multi-path with wildcard-multi + scalar-match → outer array wraps
        // the two per-path results (array + scalar) as-is.
        assertScalarString(
            "json_extract('" + candidate + "', '{}.Bridges{}.type', '{2}.Bridges{0}.length')",
            "[[\"Bridge of Sighs\",\"Ponte della Paglia\"],8981.0]"
        );

        // Missing path (empty object) and explicit-null both resolve to SQL NULL.
        assertScalarNull("json_extract('{}', 'name')");
        assertScalarNull("json_extract('{\"name\": null}', 'name')");

        // Multi-path with missing path yields literal `null` element in the
        // outer JSON array.
        assertScalarString("json_extract('{\"name\": \"John\"}', 'name', 'age')", "[\"John\",null]");
    }

    /**
     * Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonSet*} —
     * wildcard replace, missing path unchanged, and partial-wildcard set. All
     * values stored as JSON strings because the Rust UDF coerces every arg to
     * Utf8, matching the legacy fixture's {@code "b":"3"} (stringified, not
     * numeric) outputs.
     */
    public void testJsonSetParityWithLegacy() {
        // testJsonSet: wildcard replace across every array element.
        assertScalarString("json_set('{\"a\":[{\"b\":1},{\"b\":2}]}', 'a{}.b', '3')", "{\"a\":[{\"b\":\"3\"},{\"b\":\"3\"}]}");

        // testJsonSetWithWrongPath: 'a{}.b.d' doesn't exist — input unchanged.
        assertScalarString("json_set('{\"a\":[{\"b\":1},{\"b\":2}]}', 'a{}.b.d', '3')", "{\"a\":[{\"b\":1},{\"b\":2}]}");

        // testJsonSetPartialSet: wildcard where only one branch has the full
        // path; only the matching branch is rewritten.
        assertScalarString(
            "json_set('{\"a\":[{\"b\":1},{\"b\":{\"c\":2}}]}', 'a{}.b.c', '3')",
            "{\"a\":[{\"b\":1},{\"b\":{\"c\":\"3\"}}]}"
        );
    }

    /**
     * Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonDelete*}
     * — flat-key delete, nested-key delete, missing-path-unchanged, and
     * wildcard-array delete. Output order is preserved because the plugin's
     * serde_json dependency enables the {@code preserve_order} feature
     * (see {@code rust/Cargo.toml}).
     */
    public void testJsonDeleteParityWithLegacy() {
        // testJsonDelete: flat-key delete of two fields.
        assertScalarString(
            "json_delete('{\"account_number\":1,\"balance\":39225,\"age\":32,\"gender\":\"M\"}', 'age', 'gender')",
            "{\"account_number\":1,\"balance\":39225}"
        );

        // testJsonDeleteWithNested: delete a single nested key.
        assertScalarString(
            "json_delete('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}', 'f2.f3')",
            "{\"f1\":\"abc\",\"f2\":{\"f4\":\"b\"}}"
        );

        // testJsonDeleteWithNestedNothing: missing nested key leaves input unchanged.
        assertScalarString(
            "json_delete('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}', 'f2.f100')",
            "{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}"
        );

        // testJsonDeleteWithNestedAndArray: wildcard path drops one key from every array element.
        assertScalarString(
            "json_delete('{\"teacher\":\"Alice\",\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}', 'teacher', 'student{}.rank')",
            "{\"student\":[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]}"
        );
    }
}
