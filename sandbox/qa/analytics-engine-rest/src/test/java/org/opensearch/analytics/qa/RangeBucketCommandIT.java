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
 * Self-contained integration test for the PPL {@code range_bucket} scalar
 * on the analytics-engine route. Exercises PPL's {@code bin <field>
 * start=X end=Y} syntax, the only user-facing surface that emits the
 * {@code range_bucket} UDF (via
 * {@code org.opensearch.sql.calcite.utils.binning.handlers.RangeBinHandler}).
 *
 * <p><b>Blocked on empty-partition window aggregate pushdown.</b>
 * {@code RangeBinHandler} wraps the {@code data_min} and {@code data_max}
 * arguments in {@code MIN(field) OVER ()} and {@code MAX(field) OVER ()}
 * empty-partition window aggregates. The DataFusion analytics-engine
 * backend does not yet support that capability; same blocker as
 * {@link WidthBucketCommandIT} and {@link MinspanBucketCommandIT}.
 *
 * <p>The 17 Rust unit tests in {@code rust/src/udf/range_bucket.rs} and
 * the 2 {@code RangeBucketAdapterTests} cases provide unit-level
 * correctness coverage until window pushdown lands.
 */
@AwaitsFix(
    bugUrl = "PPL `bin <f> start=X end=Y` (RangeBinHandler) emits `range_bucket(f, MIN(f) OVER (), "
        + "MAX(f) OVER (), X, Y)` with windows nested in the Project. Same root cause as "
        + "WidthBucketCommandIT — see its bugUrl for the ProjectToWindowRule follow-up."
)
public class RangeBucketCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── Happy path: bin <field> start=X end=Y ───────────────────────────────
    //
    // `bin num1 start=0 end=100` invokes RangeBinHandler with start=0,
    // end=100 over num1's observed range [~2.47, 16.81].
    // Effective bounds (expansion-only):
    //   effective_min = min(0, 2.47) = 0 (start=0 expands down)
    //   effective_max = max(100, 16.81) = 100 (end=100 expands up)
    // range = 100 → exact power of 10 → width = 10^(2-1) = 10.
    // first_bin_start = floor(0/10)*10 = 0.
    // Labels for num1:
    //   8.42 → bin_index=0, bin_start=0, bin_end=10 → "0-10"
    //   6.71, 9.78, 7.43, 9.05 → all "0-10".
    public void testBinStartEndBucketsValuesIntoExpandedRange() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num1 start=0 end=100 | fields num1 | head 5",
            row("0-10"),
            row("0-10"),
            row("0-10"),
            row("0-10"),
            row("0-10")
        );
    }

    // ── Distribution across full dataset ────────────────────────────────────
    //
    // `bin num1 start=0 end=100` — width=10, every num1 value ∈ [2.47, 16.81]
    // falls in either "0-10" (num1 < 10, 10 rows) or "10-20" (num1 ≥ 10,
    // 7 rows).
    public void testBinStartEndDistributesRowsCorrectly() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | bin num1 start=0 end=100 | stats count() as c by num1 | sort num1"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("rows missing", rows);
        assertEquals("2 distinct buckets expected", 2, rows.size());
        assertBucket(rows.get(0), "0-10", 10);
        assertBucket(rows.get(1), "10-20", 7);
    }

    // ── Null-value propagation ──────────────────────────────────────────────
    //
    // `bin num0 start=-100 end=100` over num0's range [-15.7, 15.7].
    //   effective_min = min(-100, -15.7) = -100
    //   effective_max = max(100, 15.7) = 100
    //   range = 200 (not power of 10) → width = 10^floor(log10(200)) = 100.
    //   first_bin_start = floor(-100/100)*100 = -100.
    // Labels:
    //   12.3 → adj=112.3, idx=1, bin=-100+100=0 → "0-100"
    //  -12.3 → adj=87.7, idx=0, bin=-100 → "-100-0"
    //   15.7 → adj=115.7, idx=1, bin=0 → "0-100"
    //  -15.7 → adj=84.3, idx=0, bin=-100 → "-100-0"
    //    3.5 → adj=103.5, idx=1, bin=0 → "0-100"
    //   -3.5 → adj=96.5, idx=0, bin=-100 → "-100-0"
    //    0   → adj=100, idx=1, bin=0 → "0-100"
    //    null → null
    //    10  → adj=110, idx=1, bin=0 → "0-100"
    //    null → null
    public void testBinStartEndPreservesNullsInNullableField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num0 start=-100 end=100 | fields num0 | head 10",
            row("0-100"),
            row("-100-0"),
            row("0-100"),
            row("-100-0"),
            row("0-100"),
            row("-100-0"),
            row("0-100"),
            row((Object) null),
            row("0-100"),
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
