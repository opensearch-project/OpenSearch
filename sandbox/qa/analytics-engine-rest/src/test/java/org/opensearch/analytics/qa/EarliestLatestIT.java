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
 * End-to-end coverage for PPL {@code where earliest('absolute-literal', ts)} and
 * {@code where latest(...)} over the shared `calcs` dataset. The adapter pins the
 * rewritten comparison's result type back to the call's declared type; when only
 * nullability differs Calcite's `Filter.isValid` rejects the pure-nullability cast —
 * covered by the short-circuit on the equality check.
 */
public class EarliestLatestIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testEarliestAbsoluteLiteral() throws IOException {
        // calcs has 17 datetime0 rows spanning 2004-07-04 → 2004-08-02. 4 rows are ≥ the pivot.
        assertRowsEqual(
            "source=" + DATASET.indexName
                + " | where earliest('07/28/2004:12:34:27', datetime0) | stats count() as cnt",
            row(4L)
        );
    }

    public void testLatestAbsoluteLiteral() throws IOException {
        // 13 rows are ≤ the same pivot.
        assertRowsEqual(
            "source=" + DATASET.indexName
                + " | where latest('07/28/2004:12:34:27', datetime0) | stats count() as cnt",
            row(13L)
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
