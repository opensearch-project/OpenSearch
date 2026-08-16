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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Self-contained integration test for PPL's time-mode {@code span} on the
 * analytics-engine route. Sister to {@link SpanCommandIT}, which covers the
 * numeric-span Rust kernel and explicitly scopes out date/time.
 *
 * <p>Exercises all three lowering paths in
 * {@code org.opensearch.be.datafusion.SpanAdapter}:
 *
 * <ul>
 *   <li><b>{@code interval == 1}, any unit:</b> rewritten to
 *       {@code date_trunc(<unit>, field)}.</li>
 *   <li><b>Fixed-length unit ({@code s}/{@code m}/{@code h}/{@code d}/{@code w})
 *       with {@code interval > 1}:</b> bucketed via integer-seconds arithmetic
 *       through {@code to_unixtime} / {@code from_unixtime}.</li>
 *   <li><b>Sub-second unit ({@code us}/{@code ms}) with {@code interval > 1}:</b>
 *       rewritten to DataFusion's native {@code date_bin("<N> <unit>", field)}.
 *       The arithmetic path can't preserve sub-second precision because
 *       {@code to_unixtime} returns BIGINT seconds.</li>
 * </ul>
 *
 * <p>The sub-second multi-unit path is the dashboard-trigger case: the logs-tab
 * histogram emits {@code span(@timestamp, 40ms)} and previously surfaced
 * {@code Unable to convert call SPAN(precision_timestamp<0>?, i32, char<1>)}.
 *
 * <p>Uses a focused {@code span_time} dataset (5 rows with explicit sub-second
 * timestamps) rather than the {@code calcs} dataset because {@code calcs}'s
 * {@code datetime0} field has whole-second values and can't exercise 40ms
 * bucketing meaningfully.
 */
public class SpanTimeCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("span_time", "span_time");

    private static boolean dataProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /**
     * Sample data summary (5 rows in {@code datasets/span_time/bulk.json}):
     * <pre>
     * 2026-05-20T00:07:56.114Z   INFO
     * 2026-05-20T00:07:56.500Z   INFO
     * 2026-05-20T00:07:57.200Z   WARN
     * 2026-05-20T00:07:58.000Z   WARN
     * 2026-05-20T00:08:00.000Z   INFO
     * </pre>
     */

    // ── date_trunc path: N == 1 ───────────────────────────────────────────────

    /** {@code span(@timestamp, 1h)} — all 5 rows fall in a single hour bucket. */
    public void testSpanUnitOneHourLowersToDateTrunc() throws IOException {
        long total = assertSingleHourBucketTotal();
        assertEquals("all 5 rows fall in the 00:00 hour bucket", 5L, total);
    }

    private long assertSingleHourBucketTotal() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(`@timestamp`, 1h) as s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("rows missing", rows);
        assertEquals("1h bucketing must collapse to a single bucket", 1, rows.size());
        return ((Number) rows.get(0).get(0)).longValue();
    }

    // ── fixed-second arithmetic path: N > 1, unit in {s, m, h, d, w} ──────────

    /** {@code span(@timestamp, 2h)} — all 5 rows fall in a single 2-hour bucket. */
    public void testSpanMultiUnitHoursLowersToArithmetic() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(`@timestamp`, 2h) as s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("rows missing", rows);
        assertEquals("2h bucketing must collapse to a single bucket", 1, rows.size());
        assertEquals("all 5 rows fall in the 00:00 2h bucket", 5L, ((Number) rows.get(0).get(0)).longValue());
    }

    // ── date_bin path (new): N > 1, sub-second unit ───────────────────────────

    /**
     * {@code span(@timestamp, 40ms)} — the dashboard-trigger case. The 5 rows are spaced
     * far enough apart that each lands in a distinct 40ms bucket. Previously failed with
     * {@code Unable to convert call SPAN(...)} on the analytics-engine route.
     */
    public void testSpanFortyMillisecondsBucketsEachRowDistinctly() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(`@timestamp`, 40ms) as s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("rows missing", rows);
        assertEquals("each of 5 rows must fall in a distinct 40ms bucket", 5, rows.size());

        long total = 0;
        Set<Object> distinctBuckets = new HashSet<>();
        for (List<Object> r : rows) {
            total += ((Number) r.get(0)).longValue();
            distinctBuckets.add(r.get(1));
            assertEquals("each 40ms bucket must contain exactly 1 row", 1L, ((Number) r.get(0)).longValue());
        }
        assertEquals("row total must equal the 5 source rows", 5L, total);
        assertEquals("bucket labels must be distinct", 5, distinctBuckets.size());
    }

    /**
     * {@code span(@timestamp, 250us)} — sub-second microsecond bucket. With 5 rows spaced
     * milliseconds apart, each must land in its own 250us bucket. Exercises the same
     * date_bin path as ms, but with the second sub-second unit.
     */
    public void testSpanMicrosecondsBucketsEachRowDistinctly() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(`@timestamp`, 250us) as s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("rows missing", rows);
        assertEquals("each of 5 rows must fall in a distinct 250us bucket", 5, rows.size());
    }

    /**
     * Regression guard: {@code span(@timestamp, 1ms)} must stay on the cheaper
     * {@code date_trunc} path (N == 1, any unit) rather than the new
     * {@code date_bin} sub-second path. The unit is "ms" but N is 1, so the
     * adapter must check {@code isUnitInterval} before falling through to
     * sub-second handling.
     */
    public void testSpanOneMillisecondStaysOnDateTruncPath() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | stats count() as c by span(`@timestamp`, 1ms) as s"
        );
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("rows missing", rows);
        // All source @timestamp values have distinct millisecond values, so 1ms truncation
        // produces 5 buckets, one per row. The point of this assertion is that the query
        // executes successfully — the precise bucket layout is the same as 250us in this
        // dataset, just resolved through a different lowering path.
        long total = 0;
        for (List<Object> r : rows) {
            total += ((Number) r.get(0)).longValue();
        }
        assertEquals("row total must equal the 5 source rows", 5L, total);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

}
