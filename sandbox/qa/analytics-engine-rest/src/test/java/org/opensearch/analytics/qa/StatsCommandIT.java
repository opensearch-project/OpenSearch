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
 * Self-contained integration test for PPL {@code stats} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteStatsCommandIT} from the {@code opensearch-project/sql} repository
 * for the operators reachable from this PR — SPAN over numerics (the wiring added here) plus
 * the broader stats family (AVG, SUM, COUNT, MIN/MAX, DISTINCT_COUNT, STDDEV_POP / SAMP,
 * VAR_POP / SAMP) which already flow through the analytics-engine route via Calcite's
 * {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateReduceRule} decomposition.
 * Each test sends a PPL query through {@code POST /_analytics/ppl} (exposed by the
 * {@code test-ppl-frontend} plugin), exercising the same {@code UnifiedQueryPlanner} →
 * {@code CalciteRelNodeVisitor} → Substrait → DataFusion pipeline as the SQL plugin's
 * analytics-route ITs, but inside core without depending on the SQL plugin.
 *
 * <p>Group-by tests use an explicit {@code | sort} suffix because the analytics-engine backend's
 * hash-aggregation output order is non-deterministic.
 *
 * <p>Provisions the {@code calcs} dataset (parquet-backed) once per class via
 * {@link DatasetProvisioner}; {@link AnalyticsRestTestCase#preserveIndicesUponCompletion()}
 * keeps it across test methods.
 */
public class StatsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── single-value aggregates ────────────────────────────────────────────────

    public void testStatsCount() throws IOException {
        // 17 rows in calcs.
        assertRowsEqual("source=" + DATASET.indexName + " | stats count()", row(17));
    }

    public void testStatsCountField() throws IOException {
        // num0 has 9 nulls and 8 non-nulls; count(num0) counts non-nulls only.
        assertRowsEqual("source=" + DATASET.indexName + " | stats count(num0)", row(8));
    }

    public void testStatsSum() throws IOException {
        // num0 non-nulls sum to 10.0 (12.3 + -12.3 + 15.7 + -15.7 + 3.5 + -3.5 + 0 + 10).
        assertRowsEqual("source=" + DATASET.indexName + " | stats sum(num0)", row(10.0));
    }

    public void testStatsAvg() throws IOException {
        // 10.0 / 8 = 1.25.
        assertRowsEqual("source=" + DATASET.indexName + " | stats avg(num0)", row(1.25));
    }

    public void testStatsMin() throws IOException {
        assertRowsEqual("source=" + DATASET.indexName + " | stats min(num0)", row(-15.7));
    }

    public void testStatsMax() throws IOException {
        assertRowsEqual("source=" + DATASET.indexName + " | stats max(num0)", row(15.7));
    }

    public void testStatsDistinctCount() throws IOException {
        // str0 takes 3 distinct values: FURNITURE, OFFICE SUPPLIES, TECHNOLOGY (plus nulls).
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats distinct_count(str0)",
            row(3)
        );
    }

    // ── filter + aggregate ─────────────────────────────────────────────────────

    public void testStatsWithFilter() throws IOException {
        // Positive-num0 values: 12.3 + 15.7 + 3.5 + 10 = 41.5.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | where num0 > 0 | stats sum(num0)",
            row(41.5)
        );
    }

    // ── group-by aggregates ────────────────────────────────────────────────────

    public void testStatsCountByGroup() throws IOException {
        // calcs str0: 2 FURNITURE, 6 OFFICE SUPPLIES, 9 TECHNOLOGY rows (no nulls).
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats count() by str0 | sort str0",
            row(2, "FURNITURE"),
            row(6, "OFFICE SUPPLIES"),
            row(9, "TECHNOLOGY")
        );
    }

    public void testStatsSumByGroup() throws IOException {
        // num0 sums per str0 bucket. FURNITURE and OFFICE SUPPLIES happen to net to 0.0;
        // TECHNOLOGY nets to 10.0.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats sum(num0) by str0 | sort str0",
            row(0.0, "FURNITURE"),
            row(0.0, "OFFICE SUPPLIES"),
            row(10.0, "TECHNOLOGY")
        );
    }

    public void testStatsAvgByGroup() throws IOException {
        // num0 averages per str0 bucket. FURNITURE: {12.3, -12.3} → 0. OFFICE SUPPLIES:
        // {15.7, -15.7} → 0. TECHNOLOGY non-null num0 is just {10} → 10. The other
        // TECHNOLOGY rows have null num0 and don't contribute to the average.
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats avg(num0) by str0 | sort str0",
            row(0.0, "FURNITURE"),
            row(0.0, "OFFICE SUPPLIES"),
            row(10.0, "TECHNOLOGY")
        );
    }

    // ── statistical aggregates (STDDEV / VAR) ──────────────────────────────────

    public void testStatsStddevPop() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats stddev_pop(num0)",
            row(10.651056285646039)
        );
    }

    public void testStatsStddevSamp() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats stddev_samp(num0)",
            row(11.386458122323578)
        );
    }

    public void testStatsVarPop() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats var_pop(num0)",
            row(113.445)
        );
    }

    public void testStatsVarSamp() throws IOException {
        assertRowsEqual(
            "source=" + DATASET.indexName + " | stats var_samp(num0)",
            row(129.65142857142857)
        );
    }

    // ── span (numeric bucketing) ───────────────────────────────────────────────

    public void testStatsCountBySpanNumeric() throws IOException {
        // int0 buckets at width 5: {0:[1,3,4,4,4], 5:[7,8,8], 10:[10,11], null:6}.
        // Sort by the bucket field so the test is deterministic across engines; ASC sort
        // puts the null bucket first.
        assertRowsEqual(
            "source=" + DATASET.indexName
                + " | stats count() by span(int0, 5) as bucket | sort bucket",
            row(6, (Object) null),
            row(5, 0),
            row(4, 5),
            row(2, 10)
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
