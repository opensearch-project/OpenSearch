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
 * Self-contained integration test for PPL {@code rare} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteRareCommandIT} / {@code RareCommandIT} from the
 * {@code opensearch-project/sql} repository. {@code rare} lowers the same way as
 * {@code top} via {@code CalciteRelNodeVisitor#visitRareTopN} — a
 * {@code LogicalProject} containing {@code ROW_NUMBER() OVER (PARTITION BY ... ORDER BY count ASC)} —
 * so it exercises the same {@code EngineCapability.WINDOW} flag in
 * {@code OpenSearchProjectRule} that this PR introduces.
 */
public class RareCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── rare without group ─────────────────────────────────────────────────────

    public void testRareWithoutGroup() throws IOException {
        // calcs.str0 has 3 distinct values with distinct counts → rare order is
        // deterministic ASC by count: {FURNITURE: 2, OFFICE SUPPLIES: 6, TECHNOLOGY: 9}.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | rare str0",
            row("FURNITURE", 2),
            row("OFFICE SUPPLIES", 6),
            row("TECHNOLOGY", 9)
        );
    }

    public void testRareWithGroup() throws IOException {
        // rare str3 per str0 group. calcs.str3 distribution by str0:
        //   FURNITURE       → {e: 2}                  → 1 row
        //   OFFICE SUPPLIES → {e: 4, null: 2}         → 2 rows
        //   TECHNOLOGY      → {null: 5, e: 4}         → 2 rows
        // Total: 5 distinct (str0, str3) pairs under the default rare limit.
        assertNumOfRows("source=" + DATASET.indexName + " | rare str3 by str0", 5);
    }

    // ── usenull behaviour ──────────────────────────────────────────────────────

    public void testRareCommandUseNull() throws IOException {
        // calcs.int0 distribution: 6 nulls; 7 distinct non-null values {1, 3, 4 (×3),
        // 7, 8 (×3), 10, 11}. Default usenull=true → 8 bucket categories (7 + null).
        assertNumOfRows("source=" + DATASET.indexName + " | rare int0", 8);
    }

    public void testRareCommandUseNullFalse() throws IOException {
        // usenull=false drops the null bucket → 7 result rows.
        assertNumOfRows("source=" + DATASET.indexName + " | rare usenull=false int0", 7);
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
        assertEquals("Row count mismatch for query: " + ppl, expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals(
                "Column count mismatch at row " + i + " for query: " + ppl,
                want.size(),
                got.size()
            );
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals(
                    "Cell mismatch at row " + i + ", col " + j + " for query: " + ppl,
                    want.get(j),
                    got.get(j)
                );
            }
        }
    }

    private void assertNumOfRows(String ppl, int expectedRows) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, actualRows);
        assertEquals("Row count mismatch for query: " + ppl, expectedRows, actualRows.size());
    }

    /** Numeric-tolerant cell comparator (Jackson returns Integer/Long/Double interchangeably). */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            if (Double.compare(e, a) != 0) {
                fail(message + ": expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        assertEquals(message, expected, actual);
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
