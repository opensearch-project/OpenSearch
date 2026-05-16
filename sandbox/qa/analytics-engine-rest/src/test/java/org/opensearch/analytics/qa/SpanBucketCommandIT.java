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
 * Self-contained integration test for the PPL {@code span_bucket} scalar on
 * the analytics-engine route. Exercises PPL's {@code bin <field> span=N}
 * syntax, which is the only user-facing surface that emits the
 * {@code span_bucket} UDF (via
 * {@code org.opensearch.sql.calcite.utils.binning.handlers.NumericSpanHelper}).
 *
 * <p>Unlike the other {@code *_bucket} functions (width/minspan/range), the
 * {@code span=N} path emits {@code span_bucket(field, span)} directly
 * without a {@code MIN/MAX OVER ()} window wrapper, so it's exercisable
 * end-to-end without empty-partition window pushdown support.
 *
 * <p>Pipeline:
 * PPL {@code bin f span=N} &rarr;
 * {@code BinCommand} AST &rarr;
 * {@code NumericSpanHelper.createNumericSpanExpression} &rarr;
 * {@code SPAN_BUCKET(field, span)} RexNode &rarr;
 * {@link org.opensearch.be.datafusion.SpanBucketAdapter} rewrites to local
 * {@code span_bucket} SqlFunction &rarr;
 * isthmus resolves via {@code ADDITIONAL_SCALAR_SIGS} &rarr;
 * DataFusion resolves the {@code span_bucket} Rust UDF by name &rarr;
 * {@code rust/src/udf/span_bucket.rs} returns the VARCHAR bucket label.
 *
 * <p>The {@code bin} command REPLACES the original field column with the
 * bucket label (same column name, VARCHAR type) — matching
 * {@code CalciteBinCommandIT}'s observed behaviour in the {@code sql}
 * repository.
 */
public class SpanBucketCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    // ── Integer span on a non-null double field ─────────────────────────────
    //
    // `num1` has no nulls; values ∈ [2.47, 16.81]. `bin num1 span=5` labels:
    //   2.47 → "0-5"
    //   6.71, 7.43, 9.78, ... → "5-10"
    //   11.38, 12.4, ... → "10-15"
    //   16.42, 16.81 → "15-20"
    // Head 5 of the natural calcs order (8.42, 6.71, 9.78, 7.43, 9.05) all
    // fall in "5-10".
    public void testBinSpanOnDoubleFieldProducesBucketLabel() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num1 span=5 | fields num1 | head 5",
            row("5-10"),
            row("5-10"),
            row("5-10"),
            row("5-10"),
            row("5-10")
        );
    }

    // ── Null-value propagation via `bin` on a nullable field ────────────────
    //
    // num0 goes null at key07. `bin num0 span=5` must preserve nulls.
    //   12.3  → "10-15"
    //   -12.3 → floor(-12.3/5)*5 = -15 → "-15--10"
    //   15.7  → "15-20"
    //   -15.7 → "-20--15"
    //   3.5   → "0-5"
    //   -3.5  → "-5-0"
    //   0     → "0-5"
    //   null  → null (preserved)
    //   10    → "10-15"
    //   null  → null
    public void testBinSpanPreservesNullsInNullableField() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num0 span=5 | fields num0 | head 10",
            row("10-15"),
            row("-15--10"),
            row("15-20"),
            row("-20--15"),
            row("0-5"),
            row("-5-0"),
            row("0-5"),
            row((Object) null),
            row("10-15"),
            row((Object) null)
        );
    }

    // ── Aggregating over bucket labels ──────────────────────────────────────
    //
    // Feeds `bin` into `stats count()` to verify the VARCHAR column is
    // usable as a grouping key. Distribution for num1 at span=5:
    //   "0-5": 1    (2.47)
    //   "5-10": 9   (8.42, 6.71, 9.78, 7.43, 9.05, 9.38, 9.47, 7.1, 7.12)
    //   "10-15": 5  (11.38, 12.4, 10.32, 12.05, 10.37)
    //   "15-20": 2  (16.42, 16.81)
    // Lexicographic sort on the VARCHAR label: "0-5", "10-15", "15-20", "5-10".
    public void testBinSpanAsGroupingKey() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | bin num1 span=5 | stats count() as c by num1 | sort num1"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("rows missing", rows);
        assertEquals("4 distinct bucket labels expected", 4, rows.size());
        assertBucket(rows.get(0), "0-5", 1);
        assertBucket(rows.get(1), "10-15", 5);
        assertBucket(rows.get(2), "15-20", 2);
        assertBucket(rows.get(3), "5-10", 9);
    }

    // ── Fractional span uses decimal formatting ─────────────────────────────
    //
    // `bin num1 span=2.5` — span is a literal float, spans the non-integer
    // path of the formatter (1dp since span >= 1.0).
    //   8.42 → floor(8.42/2.5)*2.5 = 7.5, binEnd=10.0 → "7.5-10.0"
    //   6.71 → floor(2.684)*2.5 = 5.0, binEnd=7.5 → "5.0-7.5"
    //   9.78 → floor(3.912)*2.5 = 7.5 → "7.5-10.0"
    public void testBinSpanFractionalSpanUsesDecimalFormat() throws IOException {
        assertRows(
            "source=" + DATASET.indexName + " | bin num1 span=2.5 | fields num1 | head 3",
            row("7.5-10.0"),
            row("5.0-7.5"),
            row("7.5-10.0")
        );
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    private static void assertBucket(List<Object> row, String expectedLabel, long expectedCount) {
        // PPL stats emits (aggregate, groupingKey) column order.
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
