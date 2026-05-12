/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code head} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteHeadCommandIT}. {@code head N} lowers to {@code LogicalSort}
 * with {@code fetch=N} (no sort key); {@code head N from M} adds {@code offset=M}.
 * Pure relational op, no scalar surface — exercises the row-cap path through
 * {@code OpenSearchSort} and the DataFusion fragment driver's limit propagation.
 */
public class HeadCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    public void testHeadDefault() throws IOException {
        // `head` without a count defaults to 10.
        assertRowCount("source=" + DATASET.indexName + " | fields str2 | head", 10);
    }

    public void testHeadWithCount() throws IOException {
        assertRowCount("source=" + DATASET.indexName + " | fields str2 | head 3", 3);
    }

    public void testHeadWithCountLargerThanData() throws IOException {
        // Calcs has 17 rows. Asking for more should cap at 17, not error.
        assertRowCount("source=" + DATASET.indexName + " | fields str2 | head 100", 17);
    }

    public void testHeadFromOffset() throws IOException {
        // `head N from M` skips M rows and returns the next N. With 17 rows total,
        // `head 5 from 14` returns rows 14, 15, 16 (only 3 left).
        assertRowCount("source=" + DATASET.indexName + " | fields str2 | head 5 from 14", 3);
    }

    public void testHeadValuesMatchInsertionOrder() throws IOException {
        // Parquet returns rows in storage / insertion order. The first 5 calcs rows
        // (key00..key04) have str2 = one, two, three, null, five.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | fields str2 | head 5",
            row("one"),
            row("two"),
            row("three"),
            row((Object) null),
            row("five")
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRowsEqual(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, actualRows);
        assertEquals("Row count for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(
                "Cell mismatch at row " + i + " for query: " + ppl,
                expected[i],
                actualRows.get(i)
            );
        }
    }

    private void assertRowCount(String ppl, int expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertEquals("Row count for query: " + ppl, expected, rows.size());
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
