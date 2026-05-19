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
 * Self-contained integration test for PPL {@code top} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteTopCommandIT} / {@code TopCommandIT} from the
 * {@code opensearch-project/sql} repository. {@code top} lowers (via
 * {@code CalciteRelNodeVisitor#visitRareTopN}) to a {@code LogicalProject} containing
 * {@code ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)}. This PR's
 * {@code EngineCapability.WINDOW} flag in {@code OpenSearchProjectRule} is what lets
 * the planner accept the {@code RexOver} and emit an inline substrait
 * {@code WindowFunctionInvocation} that DataFusion's substrait consumer decodes natively.
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class TopCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── top without group ──────────────────────────────────────────────────────

    public void testTopWithoutGroup() throws IOException {
        // calcs.str0 has 3 distinct values with distinct counts → order is deterministic.
        // {TECHNOLOGY: 9, OFFICE SUPPLIES: 6, FURNITURE: 2}.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | top str0",
            row("TECHNOLOGY", 9),
            row("OFFICE SUPPLIES", 6),
            row("FURNITURE", 2)
        );
    }

    public void testTopNWithoutGroup() throws IOException {
        // top 1: the single highest-count str0.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | top 1 str0",
            row("TECHNOLOGY", 9)
        );
    }

    public void testTopNWithGroup() throws IOException {
        // top 1 str3 per str0 group. Each str0 bucket gets its own ROW_NUMBER() OVER
        // (PARTITION BY str0 ORDER BY count DESC) window; the row_number=1 row wins.
        // calcs has 3 str0 groups, so 3 result rows.
        assertNumOfRows("source=" + DATASET.indexName + " | top 1 str3 by str0", 3);
    }

    // ── usenull behaviour ──────────────────────────────────────────────────────

    public void testTopCommandUseNull() throws IOException {
        // calcs.int0 distribution: 6 nulls; 7 distinct non-null values {1, 3, 4 (×3),
        // 7, 8 (×3), 10, 11}. Default usenull=true (legacy) → top surfaces 8 bucket
        // categories (7 distinct non-null + 1 null bucket).
        assertNumOfRows("source=" + DATASET.indexName + " | top int0", 8);
    }

    public void testTopCommandUseNullFalse() throws IOException {
        // usenull=false drops the null bucket → 7 distinct non-null values.
        assertNumOfRows("source=" + DATASET.indexName + " | top usenull=false int0", 7);
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
