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
