/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
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
 * <p><b>Blocked on empty-partition window aggregate pushdown.</b>
 * {@code CountBinHandler} wraps the {@code data_range} and {@code
 * max_value} arguments in {@code MIN(field) OVER ()} and {@code
 * MAX(field) OVER ()} empty-partition window aggregates. The DataFusion
 * analytics-engine backend does not yet declare or implement empty-
 * partition window aggregate pushdown, so the PPL {@code bin bins=N}
 * command cannot currently route through the {@code width_bucket} Rust
 * UDF end-to-end — the planner either rejects pushdown or falls back to
 * coordinator-side eval where the UDF is never exercised.
 *
 * <p>The {@code width_bucket} UDF, YAML signature, enum entry, adapter,
 * and {@code scalarFunctionAdapters} registration all ship in this PR so
 * that when empty-partition window aggregate pushdown lands in a future
 * PR this IT will start exercising the full path by simply removing the
 * class-level {@link AwaitsFix} annotation. The 16 Rust unit tests in
 * {@code rust/src/udf/width_bucket.rs} provide algorithm-correctness
 * coverage in the meantime; the 2 {@code WidthBucketAdapterTests} cases
 * provide adapter-rewrite-shape coverage.
 *
 * <p>Not the ISO-SQL {@code width_bucket}. This is PPL's VARCHAR-label
 * variant (returns e.g. {@code "0-100"}) via the OpenSearch nice-number
 * algorithm, documented extensively in
 * {@link org.opensearch.be.datafusion.WidthBucketAdapter}'s Javadoc.
 */
@AwaitsFix(
    bugUrl = "Pending window aggregate pushdown in the analytics-engine planner. "
        + "PPL `bin <f> bins=N` (CountBinHandler) lowers to `width_bucket(f, N, MAX(f) OVER () - "
        + "MIN(f) OVER (), MAX(f) OVER ())`. The empty-partition RexOver wrapping MIN/MAX "
        + "(SqlKind.MIN / SqlKind.MAX inside a RexOver) is not recognized anywhere in "
        + "sandbox/plugins/analytics-engine: OpenSearchProjectRule.annotateExpr descends into "
        + "RexCall operands via ScalarFunction.fromSqlKind, which returns null for aggregate "
        + "SqlKinds inside a RexOver; there is no OpenSearchWindow RelNode, no WindowRule in "
        + "PlannerImpl, and no windowAggregate capability entry in CapabilityRegistry. "
        + "Follow-up PR must: (1) add OpenSearchWindow RelNode + OpenSearchWindowRule registered "
        + "in PlannerImpl, (2) add a WindowAggregate capability track in CapabilityRegistry "
        + "separate from scalar/aggregate, (3) extend DataFusionFragmentConvertor and "
        + "opensearch_scalar.yaml (or a new extension) with substrait window-function wiring, "
        + "(4) declare MIN/MAX OVER () as a pushdownable capability in the DataFusion backend. "
        + "Delete this annotation once those land."
)
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
    // exponent=1 → width=10. max=15.7 not on boundary → no bump. 2 > 10 false
    // → width stays 10. first_bin_start = floor(-15.7/10)*10 = -20.
    // Labels:
    //   12.3 → adj=32.3, idx=3, bin=-20+30=10 → "10-20"
    //  -12.3 → adj=7.7, idx=0, bin=-20 → "-20--10"
    //   15.7 → adj=35.7, idx=3, bin=10 → "10-20"
    //  -15.7 → adj=4.3, idx=0, bin=-20 → "-20--10"
    //    3.5 → adj=19.2, idx=1, bin=-10 → "-10-0"
    //   -3.5 → adj=12.2, idx=1, bin=-10 → "-10-0"
    //    0   → adj=20, idx=2, bin=0 → "0-10"
    //   null → null
    public void testBinBinsPreservesNullsInNullableField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num0 bins=10 | fields num0 | head 10",
            row("10-20"),
            row("-20--10"),
            row("10-20"),
            row("-20--10"),
            row("-10-0"),
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
