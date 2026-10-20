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

/** End-to-end coverage for PPL {@code <field> regexp '<pattern>'} routed to DataFusion's regexp_like. */
public class RegexpIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testRegexpInWhereLiteral() throws IOException {
        // calcs has 2 rows with str0 = "FURNITURE" matching the substring `FUR`.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where str0 regexp 'FUR' | stats count() as cnt",
            row(2L)
        );
    }

    public void testRegexpInEvalReturnsBoolean() throws IOException {
        // First three str1 values: "CLAMP ON LAMPS", "CLOCKS", "AIR PURIFIERS" — only the
        // first two match `CL.*`.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | eval f = str1 regexp 'CL.*' | fields f | head 3",
            row(true),
            row(true),
            row(false)
        );
    }

    public void testRegexpWildcardMatchesEverything() throws IOException {
        // Pattern `.*` matches every non-null str0 row — calcs has 17 such rows.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where str0 regexp '.*' | stats count() as cnt",
            row(17L)
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
                Object w = want.get(j);
                Object g = got.get(j);
                // Numeric cells deserialize as Integer when they fit in 32 bits; compare by longValue.
                if (w instanceof Number && g instanceof Number) {
                    assertEquals(
                        "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                        ((Number) w).longValue(),
                        ((Number) g).longValue()
                    );
                } else {
                    assertEquals("Cell mismatch at row " + i + ", col " + j + " for query: " + ppl, w, g);
                }
            }
        }
    }
}
