/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * End-to-end coverage for variadic {@code concat(...)} over the shared `calcs` dataset.
 * Substrait declares {@code concat} with CONSISTENT parameter consistency, so a mixed
 * VARCHAR / FixedChar operand list (the natural shape when fields and short string
 * literals appear together) trips isthmus' consistency check. The adapter pre-casts
 * every non-VARCHAR operand to VARCHAR so substrait emits a uniform-typed call.
 *
 * <p>Operand-shape coverage matters here: a concat operand can be a field reference
 * ({@code RexInputRef}), a string literal ({@code RexLiteral}), or a nested scalar-function
 * call ({@code RexCall}, e.g. {@code substring(...)}). These travel different conversion
 * paths, so the tests below exercise concat over <em>scalar-function operands</em> in
 * addition to the field/literal shapes — the natural form of queries like
 * {@code concat(substring(url, 1, 10), '|', substring(referer, 1, 10))}.
 *
 * <p>Fixture row 0 ({@code key00}) of calcs: str0="FURNITURE", str1="CLAMP ON LAMPS",
 * str2="one", str3="e".
 */
public class ConcatVariadicIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testConcatThreeStringLiterals() throws IOException {
        // 3-arg concat of short literals — every operand is FixedChar, so the cast
        // path is exercised across all three slots.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval r = concat('He', 'll', 'o') | fields r | head 1",
            row("Hello")
        );
    }

    public void testConcatFieldAndLiteralAndField() throws IOException {
        // Mixed shape: VARCHAR field + FixedChar literal + VARCHAR field. First row of
        // calcs has str2=`one` and str3=`e`, so the result is `one-e`.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval r = concat(str2, '-', str3) | fields r | head 1",
            row("one-e")
        );
    }

    public void testConcatSubstringLiteralSubstringWithFilter() throws IOException {
        // 3-arg concat where the outer operands are SUBSTRING scalar-function calls (RexCall, not
        // field refs) and the middle operand is a short string literal, above a WHERE filter on the
        // same fields. Row 0 of calcs has str0=`FURNITURE` and str1=`CLAMP ON LAMPS`, so
        // substring(_,1,3) yields `FUR` and `CLA` → `FUR|CLA`.
        assertRowsEqual(
            "source="
                + DATASET.indexName
                + " | where str0 != '' and str1 != ''"
                + " | eval r = concat(substring(str0, 1, 3), '|', substring(str1, 1, 3))"
                + " | fields r | head 1",
            row("FUR|CLA")
        );
    }

    public void testConcatTwoSubstringsNoLiteral() throws IOException {
        // 2-arg concat of two SUBSTRING calls, no middle literal — every operand is an already
        // VARCHAR-typed scalar call, so the variadic adapter makes no change. `FUR` + `CLA`.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval r = concat(substring(str0, 1, 3), substring(str1, 1, 3)) | fields r | head 1",
            row("FURCLA")
        );
    }

    public void testConcatUpperAndLowerScalarFns() throws IOException {
        // concat of two different scalar-function operands, no literal: upper(str3)=`E`,
        // lower(str0)=`furniture` → `Efurniture`. Confirms the adapter's no-op path holds when
        // operands are RexCalls (not field refs) typed VARCHAR.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval r = concat(upper(str3), lower(str0)) | fields r | head 1",
            row("Efurniture")
        );
    }

    public void testConcatScalarFnLiteralScalarFnDistinctFns() throws IOException {
        // 3-arg concat(scalarFn, literal, scalarFn) with two distinct functions around a FixedChar
        // literal: left(str0,3)=`FUR`, right(str0,3)=`URE` → `FUR#URE`. Exercises the cast path on
        // the middle literal while both outer operands are scalar-function calls.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval r = concat(left(str0, 3), '#', right(str0, 3)) | fields r | head 1",
            row("FUR#URE")
        );
    }

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i + " for query: " + ppl, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, want.get(j), got.get(j));
            }
        }
    }
}
