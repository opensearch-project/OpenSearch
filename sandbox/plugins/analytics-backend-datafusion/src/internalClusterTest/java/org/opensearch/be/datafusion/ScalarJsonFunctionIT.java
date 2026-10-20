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
 * PPL → Calcite → Substrait → DataFusion. One method per function for happy-path
 * + column-valued coverage; {@code *ParityWithLegacy} methods replay the legacy
 * SQL plugin's {@code CalcitePPLJsonBuiltinFunctionIT} fixtures verbatim.
 * Edge cases are covered in Rust unit tests.
 */
public class ScalarJsonFunctionIT extends BaseScalarFunctionIT {

    /** Happy path + NULL-on-non-array/malformed (scalar fast-path) + column-valued (Arrow columnar path). */
    public void testJsonArrayLength() {
        assertScalarIntStrict("json_array_length('[1,2,3]')", 3);
        assertScalarIntStrict("json_array_length('[]')", 0);
        assertScalarNull("json_array_length('{\"k\":1}')");
        assertScalarNull("json_array_length('not-json')");
        // Columnar path: bank fixture's json_str row 1 is '[1,2,3]'.
        assertScalarIntStrict("json_array_length(json_str)", 3);
    }

    /** Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonArrayLength}. */
    public void testJsonArrayLengthParityWithLegacy() {
        assertScalarIntStrict("json_array_length('[1,2,3,4]')", 4);
        assertScalarIntStrict("json_array_length('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]')", 5);
        assertScalarNull("json_array_length('{\"key\": 1}')");
    }

    /** Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonKeys} — insertion order preserved via {@code serde_json} {@code preserve_order}. */
    public void testJsonKeysParityWithLegacy() {
        assertScalarString("json_keys('{\"f1\":\"abc\",\"f2\":{\"f3\":\"a\",\"f4\":\"b\"}}')", "[\"f1\",\"f2\"]");
        assertScalarNull("json_keys('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]')");
        assertScalarNull("json_keys('not-json')");
        assertScalarNull("json_keys('42')");
    }

    /** Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonExtract*} — byte-for-byte match via {@code serde_json} {@code preserve_order} + no integer↔double coercion. */
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

    /** Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonSet*} — values stored as JSON strings matches legacy {@code "b":"3"} outputs (Utf8 arg coercion). */
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

    /** Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonAppend} — nested {@code json_object}/{@code json_array} constructors replaced with their stringified equivalents (same observable contract). */
    public void testJsonAppendParityWithLegacy() {
        // Case a: pre-stringified json_object(...) appended as a single array element.
        assertScalarString(
            "json_append('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                + " 'student', '{\"name\":\"Tomy\",\"rank\":5}')",
            "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},"
                + "\"{\\\"name\\\":\\\"Tomy\\\",\\\"rank\\\":5}\"]}"
        );

        // Case b: multi-pair append on the same target.
        assertScalarString(
            "json_append('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                + " 'teacher', 'Tom', 'teacher', 'Walt')",
            "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}"
        );

        // Case c: nested-path + pre-stringified json_array(...) appended as a single string element.
        assertScalarString(
            "json_append('{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}',"
                + " 'school.teacher', '[\"Tom\",\"Walt\"]')",
            "{\"school\":{\"teacher\":[\"Alice\",\"[\\\"Tom\\\",\\\"Walt\\\"]\"],"
                + "\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}"
        );
    }

    /** Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonExtend} — case c diverges from append: a JSON-array value is spread (not pushed as a single element). */
    public void testJsonExtendParityWithLegacy() {
        // Case a: stringified json_object value — not a JSON array → single push.
        assertScalarString(
            "json_extend('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                + " 'student', '{\"name\":\"Tommy\",\"rank\":5}')",
            "{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2},"
                + "\"{\\\"name\\\":\\\"Tommy\\\",\\\"rank\\\":5}\"]}"
        );

        // Case b: plain strings — each fails List-parse → each pushed individually.
        assertScalarString(
            "json_extend('{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}',"
                + " 'teacher', 'Tom', 'teacher', 'Walt')",
            "{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}"
        );

        // Case c: stringified json_array — parses as JSON array → elements spread.
        assertScalarString(
            "json_extend('{\"school\":{\"teacher\":[\"Alice\"],\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}',"
                + " 'school.teacher', '[\"Tom\",\"Walt\"]')",
            "{\"school\":{\"teacher\":[\"Alice\",\"Tom\",\"Walt\"],"
                + "\"student\":[{\"name\":\"Bob\",\"rank\":1},{\"name\":\"Charlie\",\"rank\":2}]}}"
        );
    }

    /** Parity replay of {@code CalcitePPLJsonBuiltinFunctionIT.testJsonDelete*} — output order preserved via {@code serde_json} {@code preserve_order}. */
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
