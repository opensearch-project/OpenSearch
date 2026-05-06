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
 * Self-contained integration test for the PPL {@code minspan_bucket} scalar
 * on the analytics-engine route. Exercises PPL's {@code bin <field>
 * minspan=N} syntax, the only user-facing surface that emits the
 * {@code minspan_bucket} UDF (via
 * {@code org.opensearch.sql.calcite.utils.binning.handlers.MinSpanBinHandler}).
 *
 * <p><b>Blocked on empty-partition window aggregate pushdown.</b>
 * {@code MinSpanBinHandler} wraps the {@code data_range} and
 * {@code max_value} arguments in {@code MIN(field) OVER ()} and
 * {@code MAX(field) OVER ()} empty-partition window aggregates. The
 * DataFusion analytics-engine backend does not yet support that
 * capability, so {@code bin minspan=N} cannot route through the
 * {@code minspan_bucket} Rust UDF end-to-end. See
 * {@link WidthBucketCommandIT}'s @AwaitsFix rationale — same story.
 *
 * <p>The 18 Rust unit tests in {@code rust/src/udf/minspan_bucket.rs} and
 * the 2 {@code MinspanBucketAdapterTests} cases provide unit-level
 * correctness coverage until window pushdown lands.
 */
@AwaitsFix(
    bugUrl = "Pending window aggregate pushdown in the analytics-engine planner. "
        + "PPL `bin <f> minspan=N` (MinSpanBinHandler) lowers to `minspan_bucket(f, N, "
        + "MAX(f) OVER () - MIN(f) OVER (), MAX(f) OVER ())`. Same root cause as "
        + "WidthBucketCommandIT: OpenSearchProjectRule.annotateExpr does not handle RexOver "
        + "operators carrying SqlKind.MIN / SqlKind.MAX aggregates, and there is no "
        + "OpenSearchWindow RelNode / WindowRule / windowAggregate capability. See "
        + "WidthBucketCommandIT for the full follow-up-PR checklist; delete this annotation "
        + "once that window-pushdown track lands."
)
public class MinspanBucketCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── Happy path: bin <field> minspan=N ──────────────────────────────────
    //
    // `bin num1 minspan=3` invokes MinSpanBinHandler with min_span=3 over
    // num1's observed data range [~2.47, 16.81] ≈ 14.34.
    // Java algorithm:
    //   minspan_width = 10^ceil(log10(3)) = 10
    //   default_width = 10^floor(log10(14.34)) = 10
    //   10 >= 3 → use default_width = 10.
    //   first_bin_start = floor(2.47/10)*10 = 0.
    // Labels for num1:
    //   8.42 → "0-10", 6.71 → "0-10", 9.78 → "0-10" (all < 10)
    public void testBinMinspanParameterBucketsValues() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num1 minspan=3 | fields num1 | head 3",
            row("0-10"),
            row("0-10"),
            row("0-10")
        );
    }

    // ── Distribution across full dataset ────────────────────────────────────
    //
    // Same 10/7 split as `bin bins=10` given the chosen minspan produces
    // the same width=10.
    public void testBinMinspanDistributesRowsCorrectly() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | bin num1 minspan=3 | stats count() as c by num1 | sort num1"
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
    // `bin num0 minspan=3` over num0's range [-15.7, 15.7] ≈ 31.4.
    //   minspan_width=10, default_width=10, 10>=3 → width=10.
    //   first_bin_start = floor(-15.7/10)*10 = -20.
    // Same shape as WidthBucketCommandIT.testBinBinsPreservesNullsInNullableField.
    public void testBinMinspanPreservesNullsInNullableField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num0 minspan=3 | fields num0 | head 10",
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
