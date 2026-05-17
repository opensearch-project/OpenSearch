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
 * Self-contained integration test for PPL's numeric-{@code span} on the
 * analytics-engine route. Exercises the {@code stats ... by span(field,
 * interval) as alias} surface, which is how PPL users invoke the numeric-
 * span kernel (see {@code CalciteExplainIT} /
 * {@code CalcitePPLJoinIT}'s extensive use of {@code by span(age, 10)
 * as age_span} for grouping).
 *
 * <p>Pipeline:
 * PPL {@code stats count() by span(f, n) as a} &rarr;
 * {@code CalciteRexNodeVisitor.visitSpan} emits {@code SPAN(field, n, NULL)} &rarr;
 * {@link org.opensearch.be.datafusion.SpanAdapter} rewrites to local
 * {@code span} SqlFunction &rarr;
 * DataFusion resolves the {@code span} Rust UDF by name &rarr;
 * {@code rust/src/udf/span.rs}'s numeric branch returns
 * {@code floor(value/n) * n}.
 *
 * <p>SPAN returns a numeric bin-start (same type as arg0 — the field), not
 * a VARCHAR label. Aggregation rows are keyed by the numeric bin-start.
 *
 * <p><b>Scope:</b> numeric span only. Date/time span like
 * {@code span(@ts, 1d)} is bridged on the coordinator in this PR and is
 * NOT exercised here. A follow-up will cover the time-unit path once the
 * chrono-backed Rust kernel lands or the coord bridge is pinned down.
 */
public class SpanCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── Basic grouping by numeric span ─────────────────────────────────────
    //
    // For num1 (no nulls, range [2.47, 16.81]) and span=5, the bin-start
    // values are 0.0, 5.0, 10.0, 15.0. Row counts:
    //   0.0: 1   (2.47)
    //   5.0: 9   (8.42, 6.71, 9.78, 7.43, 9.05, 9.38, 9.47, 7.1, 7.12)
    //  10.0: 5   (11.38, 12.4, 10.32, 12.05, 10.37)
    //  15.0: 2   (16.42, 16.81)
    // sort s puts them in ascending numeric order.
    public void testStatsBySpanBucketsRowsByNumericBinStart() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(num1, 5) as s | sort s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("rows missing", rows);
        assertEquals("4 distinct numeric bin-starts expected", 4, rows.size());
        assertBucket(rows.get(0), 0.0, 1);
        assertBucket(rows.get(1), 5.0, 9);
        assertBucket(rows.get(2), 10.0, 5);
        assertBucket(rows.get(3), 15.0, 2);
    }

    // ── Negative values — floor semantics (documented divergence from Java
    //    integer-trunc for integer fields). SpanAdapter's Javadoc notes that
    //    SPAN(-12.3, 5) on the DataFusion backend yields floor(-12.3/5)*5
    //    = -3*5 = -15 (floor), not -10 (truncation-toward-zero). ─────────
    //
    // num0 ∈ {12.3, -12.3, 15.7, -15.7, 3.5, -3.5, 0, null, 10, null, ...}.
    // Distinct span(num0, 5) values (excluding null-propagations):
    //   12.3 → 10
    //  -12.3 → -15
    //   15.7 → 15
    //  -15.7 → -20
    //    3.5 → 0
    //   -3.5 → -5
    //    0   → 0
    //    10  → 10
    // Plus nulls for 9 rows where num0 is null (they become one null-key
    // group by default, unless `bucket_nullable=false` is set).
    //
    // Row count distribution over the 17 calcs rows:
    //   -20: 1 (-15.7)
    //   -15: 1 (-12.3)
    //    -5: 1 (-3.5)
    //     0: 2 (3.5, 0)
    //    10: 2 (12.3, 10)
    //    15: 1 (15.7)
    //  null: 9 (nulls in num0)
    // sort s (nulls last by default in PPL).
    public void testStatsBySpanFloorsNegativeValuesTowardNegativeInfinity() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(num0, 5) as s | sort s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("rows missing", rows);
        // Find the -15 bucket to verify floor semantics (the documented divergence
        // from Java integer-trunc). Don't assume null ordering; just scan.
        boolean found_neg_15 = false;
        boolean found_neg_20 = false;
        boolean found_neg_5 = false;
        for (List<Object> r : rows) {
            Object s = r.get(1);
            if (s instanceof Number) {
                double v = ((Number) s).doubleValue();
                if (v == -15.0) {
                    found_neg_15 = true;
                    assertEquals("count for -15 bucket", 1L, ((Number) r.get(0)).longValue());
                } else if (v == -20.0) {
                    found_neg_20 = true;
                    assertEquals("count for -20 bucket", 1L, ((Number) r.get(0)).longValue());
                } else if (v == -5.0) {
                    found_neg_5 = true;
                    assertEquals("count for -5 bucket", 1L, ((Number) r.get(0)).longValue());
                }
            }
        }
        assertTrue("-15 bucket must exist (floor(-12.3/5)*5 under floor semantics)", found_neg_15);
        assertTrue("-20 bucket must exist (floor(-15.7/5)*5)", found_neg_20);
        assertTrue("-5 bucket must exist (floor(-3.5/5)*5)", found_neg_5);
    }

    // ── Fractional interval produces fractional bin-starts ──────────────────
    //
    // span(num1, 2.5): floor(num1/2.5)*2.5.
    //   Distinct values for num1 ∈ [2.47, 16.81]:
    //   2.47  → floor(0.988)*2.5 = 0.0
    //   ~5..7 → 5.0
    //   7.x..9.99 → 7.5
    //   10..12.4 → 10.0
    //   11.38..12.05 → 10.0 or 12.5 depending on exact value
    //   ...
    // Rather than enumerate every bucket (risky with fp), assert just the
    // count of distinct buckets and that the smallest bin-start is 0.0 (for
    // num1=2.47) and the largest is 15.0 (for 16.42/16.81).
    public void testStatsByFractionalSpanProducesFractionalBinStarts() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(num1, 2.5) as s | sort s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("rows missing", rows);
        // num1 ∈ {2.47, 6.71, 7.1, 7.12, 7.43, 8.42, 9.05, 9.38, 9.47, 9.78,
        //         10.32, 10.37, 11.38, 12.05, 12.4, 16.42, 16.81}
        // bin_starts at span=2.5: 0 (2.47), 5 (6.71,7.1,7.12,7.43), 7.5 (8.42,9.05,
        //   9.38,9.47,9.78), 10 (10.32,10.37,11.38,12.05,12.4), 15 (16.42,16.81)
        // → 5 distinct buckets
        assertEquals("5 distinct fractional bin-starts expected", 5, rows.size());
        double first = ((Number) rows.get(0).get(1)).doubleValue();
        double last = ((Number) rows.get(rows.size() - 1).get(1)).doubleValue();
        assertEquals("smallest bin-start must be 0.0 (for num1=2.47)", 0.0, first, 1e-9);
        assertEquals("largest bin-start must be 15.0 (for 16.42/16.81)", 15.0, last, 1e-9);
    }

    // ── Total row count sanity check ────────────────────────────────────────
    //
    // The span aggregation must account for every row in calcs (17). This
    // guards against planner bugs that silently drop rows during the
    // span → group-by lowering.
    public void testStatsBySpanAccountsForEveryRow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(num1, 5) as s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        long total = 0;
        for (List<Object> r : rows) {
            total += ((Number) r.get(0)).longValue();
        }
        assertEquals("total count across all buckets must equal row count of calcs", 17L, total);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static void assertBucket(List<Object> row, double expectedBinStart, long expectedCount) {
        assertEquals("unexpected count for bin_start " + expectedBinStart, expectedCount, ((Number) row.get(0)).longValue());
        assertEquals("unexpected bin_start", expectedBinStart, ((Number) row.get(1)).doubleValue(), 1e-9);
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
