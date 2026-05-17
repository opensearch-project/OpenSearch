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
 * Self-contained integration test for the PPL {@code width_bucket} scalar
 * on the analytics-engine route. Exercises PPL's {@code bin <field>
 * bins=N} syntax, which is the only user-facing surface that emits the
 * {@code width_bucket} UDF (via
 * {@code org.opensearch.sql.calcite.utils.binning.handlers.CountBinHandler}).
 *
 * <p>{@code CountBinHandler} wraps {@code data_range} and {@code max_value} in
 * {@code MIN(field) OVER ()} / {@code MAX(field) OVER ()} empty-partition
 * window aggregates. End-to-end pushdown works once {@code OpenSearchProject}'s
 * pre-substrait lift hoists the nested RexOver into a child Project — see the
 * {@code liftNestedRexOver} helper for details. DataFusion's substrait consumer
 * then auto-lifts the top-level WindowFunction into a {@code LogicalWindow}
 * which the physical planner can handle.
 *
 * <p>Not the ISO-SQL {@code width_bucket}. This is PPL's VARCHAR-label
 * variant (returns e.g. {@code "0-100"}) via the OpenSearch nice-number
 * algorithm, documented extensively in
 * {@link org.opensearch.be.datafusion.WidthBucketAdapter}'s Javadoc.
 */
public class WidthBucketCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── Happy path: bin <field> bins=N ─────────────────────────────────────
    //
    // `bin num1 bins=10` invokes CountBinHandler with num_bins=10 over
    // num1's data range [~2.47, 16.81]. Running through Java's width_bucket
    // algorithm with those (approximate) bounds:
    //   range ≈ 14.34, target_width = 1.434, exponent=ceil(log10(1.434))=1
    //   → width = 10. actualBins = ceil(14.34/10) = 2.
    //   max=16.81 not on boundary → no bump. 2 > 10 false → width stays 10.
    //   first_bin_start = floor(0/10)*10 = 0.
    //   Every num1 < 10 → "0-10"; num1 ≥ 10 → "10-20".
    //
    // Expected labels for head 5 of natural calcs order (all < 10):
    //   8.42, 6.71, 9.78, 7.43, 9.05 → all "0-10".
    public void testBinBinsParameterBucketsValues() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num1 bins=10 | fields num1 | head 5",
            row("0-10"),
            row("0-10"),
            row("0-10"),
            row("0-10"),
            row("0-10")
        );
    }

    // ── `bin bins=N` is an aggregation-like operation — label distribution
    //    across the full dataset reflects the bucketing. ───────────────────
    //
    // `bin num1 bins=10` → width=10 (computed as above). The 17 rows of num1
    // distribute as:
    //   "0-10":  12 rows (all num1 < 10)
    //   "10-20": 5 rows (10.32, 10.37, 11.38, 12.05, 12.4, 16.42, 16.81 →
    //            actually 7 rows; let me recount)
    // num1 values: 8.42, 6.71, 9.78, 7.43, 9.05, 9.38, 16.42, 11.38, 9.47,
    //   12.4, 10.32, 2.47, 12.05, 10.37, 7.1, 16.81, 7.12.
    // num1 < 10: 8.42, 6.71, 9.78, 7.43, 9.05, 9.38, 9.47, 2.47, 7.1, 7.12 → 10
    // num1 ≥ 10: 16.42, 11.38, 12.4, 10.32, 12.05, 10.37, 16.81 → 7
    // Total: 17 ✓
    public void testBinBinsParameterDistributesRowsCorrectly() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | bin num1 bins=10 | stats count() as c by num1 | sort num1"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("rows missing", rows);
        assertEquals("2 distinct buckets expected", 2, rows.size());
        // Lex-sort on VARCHAR: "0-10", "10-20".
        assertBucket(rows.get(0), "0-10", 10);
        assertBucket(rows.get(1), "10-20", 7);
    }

    // ── Null-value propagation ──────────────────────────────────────────────
    //
    // num0 has nulls from key07. `bin num0 bins=10` computes width from the
    // non-null range [-15.7, 15.7] → range=31.4. target_width = 3.14,
    // exponent=ceil(log10(3.14))=1 → width=10. max=15.7 not on boundary → no
    // bump; actualBins = ceil(31.4/10) = 4 ≤ 10 so width stays 10.
    //
    // Per-row mapping uses the data-node UDF's per-value formula
    // bin_start = floor(value / width) * width (rust/src/udf/width_bucket.rs).
    // The min/max OVER () operands are inputs to optimal_width only — they
    // determine `width`, not the bin offset. PPL's java-side WidthBucket
    // function uses a different `adj = value - first_bin_start` form; both
    // produce the same labels EXCEPT for values whose
    // `floor(value/width)*width` differs from `first_bin_start + idx*width`
    // when `first_bin_start` is not a multiple of `width`. For width=10 the
    // formulae coincide (every multiple-of-10 boundary is also a
    // floor(_/10)*10 boundary), so the labels listed below match both.
    //
    //   12.3 → floor(1.23)*10 = 10 → "10-20"
    //  -12.3 → floor(-1.23)*10 = -20 → "-20--10"
    //   15.7 → floor(1.57)*10 = 10 → "10-20"
    //  -15.7 → floor(-1.57)*10 = -20 → "-20--10"
    //    3.5 → floor(0.35)*10 = 0 → "0-10"
    //   -3.5 → floor(-0.35)*10 = -10 → "-10-0"
    //    0   → floor(0)*10    = 0 → "0-10"
    //   null → null (preserved by UDF's null-input short-circuit)
    //    10  → floor(1)*10 = 10 → "10-20"
    public void testBinBinsPreservesNullsInNullableField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num0 bins=10 | fields num0 | head 10",
            row("10-20"),
            row("-20--10"),
            row("10-20"),
            row("-20--10"),
            row("0-10"),
            row("-10-0"),
            row("0-10"),
            row((Object) null),
            row("10-20"),
            row((Object) null)
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private static void assertBucket(List<Object> row, String expectedLabel, long expectedCount) {
        assertEquals("unexpected count for bucket " + expectedLabel, expectedCount, ((Number) row.get(0)).longValue());
        assertEquals("unexpected bucket label", expectedLabel, row.get(1));
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final void assertRows(String ppl, List<Object>... expected) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' field for query: " + ppl, actualRows);
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

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

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
}
