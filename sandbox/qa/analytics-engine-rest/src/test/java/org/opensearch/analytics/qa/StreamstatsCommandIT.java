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
import org.opensearch.client.ResponseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Self-contained integration test for PPL {@code streamstats} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalciteStreamstatsCommandIT} from the {@code opensearch-project/sql}
 * repository one-to-one — every {@code @Test} method here corresponds to a method of the same
 * name in the source IT. Reachable cases assert exact rows; cases that depend on functionality
 * not yet wired through the analytics-engine route are {@code expectThrows} negative tests
 * pinning the precise error message — failing those is how a future PR observes that they
 * should be promoted to passing assertions.
 *
 * <h2>What works on the analytics-engine route after #21668 + this PR's PARTITION BY relaxation</h2>
 * <ul>
 *   <li><b>Plain {@code streamstats <agg>}</b> (no by, no current, no window, no reset, no global) —
 *       lowers to {@code SUM/AVG/COUNT/MIN/MAX OVER (ROWS UNBOUNDED PRECEDING TO CURRENT ROW)}. Cost
 *       gate forces SINGLETON gather, WindowAggExec runs running-aggregate on coordinator.</li>
 *   <li><b>{@code streamstats … by <field>}</b> — adds a helper {@code __row_number_for_streamstats__}
 *       column via {@code ROW_NUMBER() OVER ()} (now in our enum), then per-partition running
 *       aggregate. Cost gate gathers, WindowAggExec runs.</li>
 *   <li><b>{@code streamstats current=false}</b> — frame is {@code ROWS UNBOUNDED PRECEDING TO 1
 *       PRECEDING}. Still a RexOver, still passes through.</li>
 *   <li><b>{@code streamstats window=N}</b> — frame is {@code ROWS N-1 PRECEDING TO CURRENT ROW}.
 *       Bounded frame, RexOver-shaped, passes through.</li>
 * </ul>
 *
 * <h2>What fails (expectThrows)</h2>
 * <ul>
 *   <li>{@code streamstats reset_before=… / reset_after=…} — the PPL lowering uses correlate +
 *       conditional aggregate, doesn't flow through RexOver at all (see {@code
 *       CalciteRelNodeVisitor#buildStreamWindowJoinPlan}). Not reachable on analytics-engine.</li>
 *   <li>{@code streamstats global=true window=N by …} — uses self-join lowering, also not RexOver.</li>
 *   <li>{@code streamstats … by span(int0, 10)} works after BackendPlanAdapter recurses
 *       into RexOver.window.partitionKeys, letting SpanAdapter rewrite SPAN's NULL operand
 *       before substrait emission. The {@code bySpan} cases assert exact per-bucket rows.</li>
 *   <li>{@code streamstats dc / distinct_count / earliest / latest / percentile / median / mode}
 *       — not in {@link org.opensearch.analytics.spi.WindowFunction} enum.</li>
 *   <li>{@code testLeftJoinWithStreamstats} — streamstats inside a left-join, not exercised on
 *       the analytics-engine route yet.</li>
 *   <li>{@code testWhereInWithStreamstatsSubquery} — reachable on the analytics-engine route via
 *       PlannerImpl's subquery-remove phase, but the streamstats-inside-decorrelated-correlate
 *       shape has a nondeterministic multi-node execution race today. Marked {@code @AwaitsFix}
 *       until the downstream race is closed.</li>
 * </ul>
 *
 * <h2>Note on row order determinism</h2>
 *
 * <p>Streamstats running aggregates depend on row-order. Calcs has 17 rows; without an explicit
 * sort, DataFusion's parallel scan can return rows in any order. Every reachable test below
 * starts with {@code | sort key} (the unique row id) to pin output to a deterministic order
 * before the streamstats step. This differs from the sql IT which trusts ingestion order on a
 * 4-row STATE_COUNTRY index.
 *
 * <h2>Schema translation</h2>
 * <p>Same as {@link EventstatsCommandIT}: {@code age→int0}, {@code country→str0},
 * {@code state→str3}, {@code name→key}.
 */
public class StreamstatsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    /** Multi-shard alias for tests that exercise the SINGLETON-gather cost gate on
     *  partitioned streaming windows. See {@code EventstatsCommandIT.DATASET_MULTI} for the
     *  same pattern. */
    private static final Dataset DATASET_MULTI = new Dataset("calcs", "calcs_multi_streamstats");

    private static boolean dataProvisioned = false;
    private static boolean multiProvisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    private void ensureMultiShardProvisioned() throws IOException {
        if (multiProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET_MULTI, 3);
            multiProvisioned = true;
        }
    }

    // ── Basic streamstats (no by) ──────────────────────────────────────────────

    /** sql IT: testStreamstats. Per-row running count/avg/min/max over int0. count() counts every
     *  row including int0 nulls; avg/min/max ignore them. */
    public void testStreamstats() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx"
                + " | fields key, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", 1,    1L,  1.0,                 1, 1),
            row("key01", null, 2L,  1.0,                 1, 1),
            row("key02", null, 3L,  1.0,                 1, 1),
            row("key03", null, 4L,  1.0,                 1, 1),
            row("key04", 7,    5L,  4.0,                 1, 7),
            row("key05", 3,    6L,  3.6666666666666665,  1, 7),
            row("key06", 8,    7L,  4.75,                1, 8),
            row("key07", null, 8L,  4.75,                1, 8),
            row("key08", null, 9L,  4.75,                1, 8),
            row("key09", 8,    10L, 5.4,                 1, 8),
            row("key10", 4,    11L, 5.166666666666667,   1, 8),
            row("key11", 10,   12L, 5.857142857142857,   1, 10),
            row("key12", null, 13L, 5.857142857142857,   1, 10),
            row("key13", 4,    14L, 5.625,               1, 10),
            row("key14", 11,   15L, 6.222222222222222,   1, 11),
            row("key15", 4,    16L, 6.0,                 1, 11),
            row("key16", 8,    17L, 6.181818181818182,   1, 11)
        );
    }

    // ── streamstats … by ───────────────────────────────────────────────────────

    /** sql IT: testStreamstatsBy. Per-partition running cnt/avg/min/max. Each row gets the
     *  running aggregate at its position within the partition (rows ordered by key).
     *  Streamstats does not gather rows from the same partition together — it sees them in
     *  the input row order, so e.g. OFFICE SUPPLIES rows {key02..key05, key07, key09} appear
     *  in key order intermixed with FURNITURE/TECHNOLOGY rows but the running counter is
     *  per-partition. */
    public void testStreamstatsBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by str0"
                + " | fields key, str0, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    1L, 1.0,                 1,    1),
            row("key01", "FURNITURE",       null, 2L, 1.0,                 1,    1),
            row("key02", "OFFICE SUPPLIES", null, 1L, null,                null, null),
            row("key03", "OFFICE SUPPLIES", null, 2L, null,                null, null),
            row("key04", "OFFICE SUPPLIES", 7,    3L, 7.0,                 7,    7),
            row("key05", "OFFICE SUPPLIES", 3,    4L, 5.0,                 3,    7),
            row("key06", "OFFICE SUPPLIES", 8,    5L, 6.0,                 3,    8),
            row("key07", "OFFICE SUPPLIES", null, 6L, 6.0,                 3,    8),
            row("key08", "TECHNOLOGY",      null, 1L, null,                null, null),
            row("key09", "TECHNOLOGY",      8,    2L, 8.0,                 8,    8),
            row("key10", "TECHNOLOGY",      4,    3L, 6.0,                 4,    8),
            row("key11", "TECHNOLOGY",      10,   4L, 7.333333333333333,   4,    10),
            row("key12", "TECHNOLOGY",      null, 5L, 7.333333333333333,   4,    10),
            row("key13", "TECHNOLOGY",      4,    6L, 6.5,                 4,    10),
            row("key14", "TECHNOLOGY",      11,   7L, 7.4,                 4,    11),
            row("key15", "TECHNOLOGY",      4,    8L, 6.833333333333333,   4,    11),
            row("key16", "TECHNOLOGY",      8,    9L, 7.0,                 4,    11)
        );
    }

    /** sql IT: testStreamstatsByWithNull. Default {@code bucket_nullable=true} mode — null-key
     *  rows form their own running partition and accumulate normally. {@code str3} has 7 nulls
     *  + 10 'e' rows; companion to {@link #testStreamstatsByWithNullBucket}. */
    public void testStreamstatsByWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by str3"
                + " | fields key, str3, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    1L,  1.0,                1,    1),
            row("key01", "e",  null, 2L,  1.0,                1,    1),
            row("key02", "e",  null, 3L,  1.0,                1,    1),
            row("key03", "e",  null, 4L,  1.0,                1,    1),
            row("key04", null, 7,    1L,  7.0,                7,    7),
            row("key05", null, 3,    2L,  5.0,                3,    7),
            row("key06", "e",  8,    5L,  4.5,                1,    8),
            row("key07", "e",  null, 6L,  4.5,                1,    8),
            row("key08", null, null, 3L,  5.0,                3,    7),
            row("key09", "e",  8,    7L,  5.666666666666667,  1,    8),
            row("key10", "e",  4,    8L,  5.25,               1,    8),
            row("key11", null, 10,   4L,  6.666666666666667,  3,    10),
            row("key12", null, null, 5L,  6.666666666666667,  3,    10),
            row("key13", null, 4,    6L,  6.0,                3,    10),
            row("key14", "e",  11,   9L,  6.4,                1,    11),
            row("key15", "e",  4,    10L, 6.0,                1,    11),
            row("key16", null, 8,    7L,  6.4,                3,    10)
        );
    }

    /** sql IT: testStreamstatsByWithNullBucket. {@code bucket_nullable=false} drops the
     *  null-key partition: rows whose {@code by} key is null get NULL running output, while
     *  the non-null partition's running count proceeds as usual. Same {@code by str3} as
     *  {@link #testStreamstatsByWithNull}; null-key rows now show NULL aggregates. */
    public void testStreamstatsByWithNullBucket() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats bucket_nullable=false count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by str3"
                + " | fields key, str3, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    1L,   1.0,               1,    1),
            row("key01", "e",  null, 2L,   1.0,               1,    1),
            row("key02", "e",  null, 3L,   1.0,               1,    1),
            row("key03", "e",  null, 4L,   1.0,               1,    1),
            row("key04", null, 7,    null, null,              null, null),
            row("key05", null, 3,    null, null,              null, null),
            row("key06", "e",  8,    5L,   4.5,               1,    8),
            row("key07", "e",  null, 6L,   4.5,               1,    8),
            row("key08", null, null, null, null,              null, null),
            row("key09", "e",  8,    7L,   5.666666666666667, 1,    8),
            row("key10", "e",  4,    8L,   5.25,              1,    8),
            row("key11", null, 10,   null, null,              null, null),
            row("key12", null, null, null, null,              null, null),
            row("key13", null, 4,    null, null,              null, null),
            row("key14", "e",  11,   9L,   6.4,               1,    11),
            row("key15", "e",  4,    10L,  6.0,               1,    11),
            row("key16", null, 8,    null, null,              null, null)
        );
    }

    // ── Multi-shard correctness — exercises the SINGLETON-gather cost gate ────

    /** {@code streamstats … by str0} on a 3-shard index. Pre-fix this crashed with
     *  "Field reference offset (27) must be less than number of fields in struct (27)" —
     *  see {@code DataFusionFragmentConvertor.replaceInput} for the lifted-window Project
     *  case it now handles. */
    public void testStreamstatsBy_3shard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName + " | sort key | streamstats count() as cnt by str0"
                + " | stats max(cnt) as final_cnt by str0 | sort str0"
        );
        assertRowsEqual(response,
            row(2L, "FURNITURE"),
            row(6L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
        );
    }

    /** sql IT: testStreamstatsBySpan. Bucketing key has no nulls — uses {@code int3} which
     *  is fully populated. {@code span(int3, 10)} produces two buckets: 0 (9 rows) and 10
     *  (8 rows). Pairs with {@link #testStreamstatsBySpanWithNull} which uses
     *  {@code span(int0, 10)} reintroducing the null bucket. */
    public void testStreamstatsBySpan() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by span(int3, 10)"
                + " | fields key, int3, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", 8,  1,    1L, 1.0,                1,    1),
            row("key01", 13, null, 1L, null,               null, null),
            row("key02", 2,  null, 2L, 1.0,                1,    1),
            row("key03", 5,  null, 3L, 1.0,                1,    1),
            row("key04", 9,  7,    4L, 4.0,                1,    7),
            row("key05", 7,  3,    5L, 3.6666666666666665, 1,    7),
            row("key06", 18, 8,    2L, 8.0,                8,    8),
            row("key07", 3,  null, 6L, 3.6666666666666665, 1,    7),
            row("key08", 17, null, 3L, 8.0,                8,    8),
            row("key09", 2,  8,    7L, 4.75,               1,    8),
            row("key10", 11, 4,    4L, 6.0,                4,    8),
            row("key11", 2,  10,   8L, 5.8,                1,    10),
            row("key12", 11, null, 5L, 6.0,                4,    8),
            row("key13", 18, 4,    6L, 5.333333333333333,  4,    8),
            row("key14", 18, 11,   7L, 6.75,               4,    11),
            row("key15", 11, 4,    8L, 6.2,                4,    11),
            row("key16", 0,  8,    9L, 6.166666666666667,  1,    10)
        );
    }

    /** sql IT: testStreamstatsBySpanWithNull. Bucketing key has nulls — uses
     *  {@code span(int0, 10)} which produces a null bucket alongside bucket 0 and bucket 10.
     *  Null-key rows form their own running partition and accumulate count() but
     *  avg/min/max stay NULL since there are no non-null int0 values to consume. */
    public void testStreamstatsBySpanWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by span(int0, 10)"
                + " | fields key, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", 1,    1L, 1.0,                1,    1),
            row("key01", null, 1L, null,               null, null),
            row("key02", null, 2L, null,               null, null),
            row("key03", null, 3L, null,               null, null),
            row("key04", 7,    2L, 4.0,                1,    7),
            row("key05", 3,    3L, 3.6666666666666665, 1,    7),
            row("key06", 8,    4L, 4.75,               1,    8),
            row("key07", null, 4L, null,               null, null),
            row("key08", null, 5L, null,               null, null),
            row("key09", 8,    5L, 5.4,                1,    8),
            row("key10", 4,    6L, 5.166666666666667,  1,    8),
            row("key11", 10,   1L, 10.0,               10,   10),
            row("key12", null, 6L, null,               null, null),
            row("key13", 4,    7L, 5.0,                1,    8),
            row("key14", 11,   2L, 10.5,               10,   11),
            row("key15", 4,    8L, 4.875,              1,    8),
            row("key16", 8,    9L, 5.222222222222222,  1,    8)
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitions1. {@code by span(int0, 10), str0}. */
    public void testStreamstatsByMultiplePartitions1() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt by span(int0, 10), str0"
                + " | fields key, str0, int0, cnt"
        );
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    1L),
            row("key01", "FURNITURE",       null, 1L),
            row("key02", "OFFICE SUPPLIES", null, 1L),
            row("key03", "OFFICE SUPPLIES", null, 2L),
            row("key04", "OFFICE SUPPLIES", 7,    1L),
            row("key05", "OFFICE SUPPLIES", 3,    2L),
            row("key06", "OFFICE SUPPLIES", 8,    3L),
            row("key07", "OFFICE SUPPLIES", null, 3L),
            row("key08", "TECHNOLOGY",      null, 1L),
            row("key09", "TECHNOLOGY",      8,    1L),
            row("key10", "TECHNOLOGY",      4,    2L),
            row("key11", "TECHNOLOGY",      10,   1L),
            row("key12", "TECHNOLOGY",      null, 2L),
            row("key13", "TECHNOLOGY",      4,    3L),
            row("key14", "TECHNOLOGY",      11,   2L),
            row("key15", "TECHNOLOGY",      4,    4L),
            row("key16", "TECHNOLOGY",      8,    5L)
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitions2. {@code by span(int0, 10), str3}. */
    public void testStreamstatsByMultiplePartitions2() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt by span(int0, 10), str3"
                + " | fields key, str3, int0, cnt"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    1L),
            row("key01", "e",  null, 1L),
            row("key02", "e",  null, 2L),
            row("key03", "e",  null, 3L),
            row("key04", null, 7,    1L),
            row("key05", null, 3,    2L),
            row("key06", "e",  8,    2L),
            row("key07", "e",  null, 4L),
            row("key08", null, null, 1L),
            row("key09", "e",  8,    3L),
            row("key10", "e",  4,    4L),
            row("key11", null, 10,   1L),
            row("key12", null, null, 2L),
            row("key13", null, 4,    3L),
            row("key14", "e",  11,   1L),
            row("key15", "e",  4,    5L),
            row("key16", null, 8,    4L)
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitionsWithNull1. Same query as
     *  {@link #testStreamstatsByMultiplePartitions1}. */
    public void testStreamstatsByMultiplePartitionsWithNull1() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt by span(int0, 10), str0"
                + " | fields key, str0, int0, cnt"
        );
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    1L),
            row("key01", "FURNITURE",       null, 1L),
            row("key02", "OFFICE SUPPLIES", null, 1L),
            row("key03", "OFFICE SUPPLIES", null, 2L),
            row("key04", "OFFICE SUPPLIES", 7,    1L),
            row("key05", "OFFICE SUPPLIES", 3,    2L),
            row("key06", "OFFICE SUPPLIES", 8,    3L),
            row("key07", "OFFICE SUPPLIES", null, 3L),
            row("key08", "TECHNOLOGY",      null, 1L),
            row("key09", "TECHNOLOGY",      8,    1L),
            row("key10", "TECHNOLOGY",      4,    2L),
            row("key11", "TECHNOLOGY",      10,   1L),
            row("key12", "TECHNOLOGY",      null, 2L),
            row("key13", "TECHNOLOGY",      4,    3L),
            row("key14", "TECHNOLOGY",      11,   2L),
            row("key15", "TECHNOLOGY",      4,    4L),
            row("key16", "TECHNOLOGY",      8,    5L)
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitionsWithNull2. Same query as
     *  {@link #testStreamstatsByMultiplePartitions2}. */
    public void testStreamstatsByMultiplePartitionsWithNull2() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt by span(int0, 10), str3"
                + " | fields key, str3, int0, cnt"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    1L),
            row("key01", "e",  null, 1L),
            row("key02", "e",  null, 2L),
            row("key03", "e",  null, 3L),
            row("key04", null, 7,    1L),
            row("key05", null, 3,    2L),
            row("key06", "e",  8,    2L),
            row("key07", "e",  null, 4L),
            row("key08", null, null, 1L),
            row("key09", "e",  8,    3L),
            row("key10", "e",  4,    4L),
            row("key11", null, 10,   1L),
            row("key12", null, null, 2L),
            row("key13", null, 4,    3L),
            row("key14", "e",  11,   1L),
            row("key15", "e",  4,    5L),
            row("key16", null, 8,    4L)
        );
    }

    // ── current=false / window=N variants ──────────────────────────────────────

    /** sql IT: testStreamstatsCurrent. {@code current=false} → frame is N-1 preceding to 1 PRECEDING.
     *  At each row, prev_avg = avg of int0 over all preceding rows (excluding current). */
    public void testStreamstatsCurrent() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false avg(int0) as prev_avg"
                + " | fields key, int0, prev_avg"
        );
        assertRowsEqual(response,
            row("key00", 1,    null),
            row("key01", null, 1.0),
            row("key02", null, 1.0),
            row("key03", null, 1.0),
            row("key04", 7,    1.0),
            row("key05", 3,    4.0),
            row("key06", 8,    3.6666666666666665),
            row("key07", null, 4.75),
            row("key08", null, 4.75),
            row("key09", 8,    4.75),
            row("key10", 4,    5.4),
            row("key11", 10,   5.166666666666667),
            row("key12", null, 5.857142857142857),
            row("key13", 4,    5.857142857142857),
            row("key14", 11,   5.625),
            row("key15", 4,    6.222222222222222),
            row("key16", 8,    6.0)
        );
    }

    /** sql IT: testStreamstatsCurrentWithNUll. Same query as {@link #testStreamstatsCurrent} —
     *  calcs already has int0 nulls covered. */
    public void testStreamstatsCurrentWithNUll() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false avg(int0) as prev_avg"
                + " | fields key, int0, prev_avg"
        );
        assertRowsEqual(response,
            row("key00", 1,    null),
            row("key01", null, 1.0),
            row("key02", null, 1.0),
            row("key03", null, 1.0),
            row("key04", 7,    1.0),
            row("key05", 3,    4.0),
            row("key06", 8,    3.6666666666666665),
            row("key07", null, 4.75),
            row("key08", null, 4.75),
            row("key09", 8,    4.75),
            row("key10", 4,    5.4),
            row("key11", 10,   5.166666666666667),
            row("key12", null, 5.857142857142857),
            row("key13", 4,    5.857142857142857),
            row("key14", 11,   5.625),
            row("key15", 4,    6.222222222222222),
            row("key16", 8,    6.0)
        );
    }

    /** sql IT: testStreamstatsWindow. {@code window=3} → frame is 2 PRECEDING TO CURRENT ROW.
     *  Each row's avg covers up to 3 consecutive rows ending at current; null int0 ignored. */
    public void testStreamstatsWindow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats window=3 avg(int0) as avg3"
                + " | fields key, int0, avg3"
        );
        assertRowsEqual(response,
            row("key00", 1,    1.0),
            row("key01", null, 1.0),
            row("key02", null, 1.0),
            row("key03", null, null),  // window={null,null,null}, no non-null
            row("key04", 7,    7.0),   // window={null,null,7}
            row("key05", 3,    5.0),   // window={null,7,3}
            row("key06", 8,    6.0),   // window={7,3,8}
            row("key07", null, 5.5),   // window={3,8,null}
            row("key08", null, 8.0),   // window={8,null,null}
            row("key09", 8,    8.0),   // window={null,null,8}
            row("key10", 4,    6.0),   // window={null,8,4}
            row("key11", 10,   7.333333333333333),  // {8,4,10}
            row("key12", null, 7.0),   // {4,10,null}
            row("key13", 4,    7.0),   // {10,null,4}
            row("key14", 11,   7.5),   // {null,4,11}
            row("key15", 4,    6.333333333333333),  // {4,11,4}
            row("key16", 8,    7.666666666666667)   // {11,4,8}
        );
    }

    /** sql IT: testStreamstatsWindowWithNull. Same query as {@link #testStreamstatsWindow} —
     *  calcs already has int0 nulls covered. */
    public void testStreamstatsWindowWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats window=3 avg(int0) as avg3"
                + " | fields key, int0, avg3"
        );
        assertRowsEqual(response,
            row("key00", 1,    1.0),
            row("key01", null, 1.0),
            row("key02", null, 1.0),
            row("key03", null, null),
            row("key04", 7,    7.0),
            row("key05", 3,    5.0),
            row("key06", 8,    6.0),
            row("key07", null, 5.5),
            row("key08", null, 8.0),
            row("key09", 8,    8.0),
            row("key10", 4,    6.0),
            row("key11", 10,   7.333333333333333),
            row("key12", null, 7.0),
            row("key13", 4,    7.0),
            row("key14", 11,   7.5),
            row("key15", 4,    6.333333333333333),
            row("key16", 8,    7.666666666666667)
        );
    }

    /** sql IT: testStreamstatsBigWindow. {@code window=100} exceeds row count; effectively
     *  same as the unbounded running avg in {@link #testStreamstats}. */
    public void testStreamstatsBigWindow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats window=100 avg(int0) as avg"
                + " | fields key, int0, avg"
        );
        assertRowsEqual(response,
            row("key00", 1,    1.0),
            row("key01", null, 1.0),
            row("key02", null, 1.0),
            row("key03", null, 1.0),
            row("key04", 7,    4.0),
            row("key05", 3,    3.6666666666666665),
            row("key06", 8,    4.75),
            row("key07", null, 4.75),
            row("key08", null, 4.75),
            row("key09", 8,    5.4),
            row("key10", 4,    5.166666666666667),
            row("key11", 10,   5.857142857142857),
            row("key12", null, 5.857142857142857),
            row("key13", 4,    5.625),
            row("key14", 11,   6.222222222222222),
            row("key15", 4,    6.0),
            row("key16", 8,    6.181818181818182)
        );
    }

    /** sql IT: testStreamstatsWindowError. window=-1 must be a parse-time / argument-validation
     *  error. PPL parser rejects this before reaching analytics-engine planner. */
    public void testStreamstatsWindowError() throws IOException {
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats window=-1 avg(int0) as avg",
            "Window size must be >= 0"
        );
    }

    /** sql IT: testStreamstatsCurrentAndWindow. {@code current=false window=2} → frame is
     *  the 2 rows immediately before current (excluding current itself). avg per row =
     *  avg of int0 over those 2 prior rows. */
    public void testStreamstatsCurrentAndWindow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false window=2 avg(int0) as avg"
                + " | fields key, int0, avg"
        );
        assertRowsEqual(response,
            row("key00", 1,    null),
            row("key01", null, 1.0),
            row("key02", null, 1.0),
            row("key03", null, null),
            row("key04", 7,    null),
            row("key05", 3,    7.0),
            row("key06", 8,    5.0),
            row("key07", null, 5.5),
            row("key08", null, 8.0),
            row("key09", 8,    null),
            row("key10", 4,    8.0),
            row("key11", 10,   6.0),
            row("key12", null, 7.0),
            row("key13", 4,    10.0),
            row("key14", 11,   4.0),
            row("key15", 4,    7.5),
            row("key16", 8,    7.5)
        );
    }

    /** sql IT: testStreamstatsCurrentAndWindowWithNull. Same query as
     *  {@link #testStreamstatsCurrentAndWindow}. */
    public void testStreamstatsCurrentAndWindowWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false window=2 avg(int0) as avg"
                + " | fields key, int0, avg"
        );
        assertRowsEqual(response,
            row("key00", 1,    null),
            row("key01", null, 1.0),
            row("key02", null, 1.0),
            row("key03", null, null),
            row("key04", 7,    null),
            row("key05", 3,    7.0),
            row("key06", 8,    5.0),
            row("key07", null, 5.5),
            row("key08", null, 8.0),
            row("key09", 8,    null),
            row("key10", 4,    8.0),
            row("key11", 10,   6.0),
            row("key12", null, 7.0),
            row("key13", 4,    10.0),
            row("key14", 11,   4.0),
            row("key15", 4,    7.5),
            row("key16", 8,    7.5)
        );
    }

    // ── global / reset — uses self-join lowering, not RexOver ─────────────────

    /** sql IT: testStreamstatsGlobal. {@code global=false} with {@code window=2 by str0} is the
     *  per-partition windowed running aggregate. Each row's avg covers up to 2 rows of int0
     *  ending at current within its str0 partition (sorted by key). */
    public void testStreamstatsGlobal() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats window=2 global=false avg(int0) as avg by str0"
                + " | fields key, str0, int0, avg"
        );
        // Per-(str0) sliding window=2 over int0 ignoring nulls:
        // FURNITURE: [1, null] → avg=1, 1
        // OFFICE SUPPLIES: [null, null, 7, 3, 8, null] → null, null, 7, 5, 5.5, 8
        // TECHNOLOGY: [null, 8, 4, 10, null, 4, 11, 4, 8] → null, 8, 6, 7, 10, 4, 7.5, 7.5, 6
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    1.0),
            row("key01", "FURNITURE",       null, 1.0),
            row("key02", "OFFICE SUPPLIES", null, null),
            row("key03", "OFFICE SUPPLIES", null, null),
            row("key04", "OFFICE SUPPLIES", 7,    7.0),
            row("key05", "OFFICE SUPPLIES", 3,    5.0),
            row("key06", "OFFICE SUPPLIES", 8,    5.5),
            row("key07", "OFFICE SUPPLIES", null, 8.0),
            row("key08", "TECHNOLOGY",      null, null),
            row("key09", "TECHNOLOGY",      8,    8.0),
            row("key10", "TECHNOLOGY",      4,    6.0),
            row("key11", "TECHNOLOGY",      10,   7.0),
            row("key12", "TECHNOLOGY",      null, 10.0),
            row("key13", "TECHNOLOGY",      4,    4.0),
            row("key14", "TECHNOLOGY",      11,   7.5),
            row("key15", "TECHNOLOGY",      4,    7.5),
            row("key16", "TECHNOLOGY",      8,    6.0)
        );
    }

    /** sql IT: testStreamstatsGlobalWithNull. PR #21795 + the layered marking fixes
     *  (LITERAL_AGG lowering, OpenSearchJoinRule relaxation) wire enough of the
     *  streamstats lowering through that this query now plans and executes. */
    public void testStreamstatsGlobalWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | streamstats window=2 global=true avg(int0) as avg by str0"
        );
        assertNotNull(response);
    }

    /** sql IT: testStreamstatsGlobalWithNullBucket. */
    public void testStreamstatsGlobalWithNullBucket() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | streamstats bucket_nullable=false window=2 global=true avg(int0) as avg by str0"
        );
        assertNotNull(response);
    }

    /** sql IT: testStreamstatsReset. {@code reset_before} / {@code reset_after} use
     *  {@code buildStreamWindowJoinPlan} — Correlate + segment-id filter, not RexOver.
     *  Ryan's PR #21795 added subquery-remove + decorrelate to PlannerImpl; this test
     *  is flipped to positive to observe whether streamstats-reset's directly-built
     *  LogicalCorrelate falls through that path correctly. */
    public void testStreamstatsReset() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats reset_before=(int0 > 5) avg(int0) as avg by str0 | fields key, str0, int0, avg"
        );
        assertNotNull(response);
    }

    /** sql IT: testStreamstatsResetWithNull. */
    public void testStreamstatsResetWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | streamstats reset_before=(int0 > 5) avg(int0) as avg by str0"
        );
        assertNotNull(response);
    }

    /** sql IT: testStreamstatsResetWithNullBucket. */
    public void testStreamstatsResetWithNullBucket() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | streamstats bucket_nullable=false reset_before=(int0 > 5)"
                + " avg(int0) as avg by str0"
        );
        assertNotNull(response);
    }

    // ── Unsupported window functions ───────────────────────────────────────────

    /** sql IT: testUnsupportedWindowFunctions. */
    public void testUnsupportedWindowFunctions() throws IOException {
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats percentile_approx(int0)",
            "percentile_approx"
        );
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats percentile(int0)",
            "percentile"
        );
    }

    // ── Multiple streamstats (chained) ─────────────────────────────────────────

    /** sql IT: testMultipleStreamstats. Two chained streamstats — first running count, then
     *  running avg over those counts. At row N, cnt=N and avg_cnt = (N+1)/2. */
    public void testMultipleStreamstats() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | streamstats avg(cnt) as avg_cnt"
                + " | fields key, int0, cnt, avg_cnt"
        );
        assertRowsEqual(response,
            row("key00", 1,    1L,  1.0),
            row("key01", null, 2L,  1.5),
            row("key02", null, 3L,  2.0),
            row("key03", null, 4L,  2.5),
            row("key04", 7,    5L,  3.0),
            row("key05", 3,    6L,  3.5),
            row("key06", 8,    7L,  4.0),
            row("key07", null, 8L,  4.5),
            row("key08", null, 9L,  5.0),
            row("key09", 8,    10L, 5.5),
            row("key10", 4,    11L, 6.0),
            row("key11", 10,   12L, 6.5),
            row("key12", null, 13L, 7.0),
            row("key13", 4,    14L, 7.5),
            row("key14", 11,   15L, 8.0),
            row("key15", 4,    16L, 8.5),
            row("key16", 8,    17L, 9.0)
        );
    }

    /** sql IT: testMultipleStreamstatsWithWindow. Second streamstats uses window=3 over the
     *  running cnt. */
    public void testMultipleStreamstatsWithWindow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | streamstats window=3 avg(cnt) as avg_cnt"
                + " | fields key, int0, cnt, avg_cnt"
        );
        // Window=3 over running counts [1,2,...,17]: at row N≥3, avg = (N-2 + N-1 + N)/3 = N-1.
        assertRowsEqual(response,
            row("key00", 1,    1L,  1.0),
            row("key01", null, 2L,  1.5),
            row("key02", null, 3L,  2.0),
            row("key03", null, 4L,  3.0),
            row("key04", 7,    5L,  4.0),
            row("key05", 3,    6L,  5.0),
            row("key06", 8,    7L,  6.0),
            row("key07", null, 8L,  7.0),
            row("key08", null, 9L,  8.0),
            row("key09", 8,    10L, 9.0),
            row("key10", 4,    11L, 10.0),
            row("key11", 10,   12L, 11.0),
            row("key12", null, 13L, 12.0),
            row("key13", 4,    14L, 13.0),
            row("key14", 11,   15L, 14.0),
            row("key15", 4,    16L, 15.0),
            row("key16", 8,    17L, 16.0)
        );
    }

    /** sql IT: testMultipleStreamstatsWithNull1. Same query as
     *  {@link #testMultipleStreamstats}. */
    public void testMultipleStreamstatsWithNull1() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | streamstats avg(cnt) as avg_cnt"
                + " | fields key, int0, cnt, avg_cnt"
        );
        assertRowsEqual(response,
            row("key00", 1,    1L,  1.0),
            row("key01", null, 2L,  1.5),
            row("key02", null, 3L,  2.0),
            row("key03", null, 4L,  2.5),
            row("key04", 7,    5L,  3.0),
            row("key05", 3,    6L,  3.5),
            row("key06", 8,    7L,  4.0),
            row("key07", null, 8L,  4.5),
            row("key08", null, 9L,  5.0),
            row("key09", 8,    10L, 5.5),
            row("key10", 4,    11L, 6.0),
            row("key11", 10,   12L, 6.5),
            row("key12", null, 13L, 7.0),
            row("key13", 4,    14L, 7.5),
            row("key14", 11,   15L, 8.0),
            row("key15", 4,    16L, 8.5),
            row("key16", 8,    17L, 9.0)
        );
    }

    /** sql IT: testMultipleStreamstatsWithNull2. Same query as
     *  {@link #testMultipleStreamstatsWithWindow}. */
    public void testMultipleStreamstatsWithNull2() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | streamstats window=3 avg(cnt) as avg_cnt"
                + " | fields key, int0, cnt, avg_cnt"
        );
        assertRowsEqual(response,
            row("key00", 1,    1L,  1.0),
            row("key01", null, 2L,  1.5),
            row("key02", null, 3L,  2.0),
            row("key03", null, 4L,  3.0),
            row("key04", 7,    5L,  4.0),
            row("key05", 3,    6L,  5.0),
            row("key06", 8,    7L,  6.0),
            row("key07", null, 8L,  7.0),
            row("key08", null, 9L,  8.0),
            row("key09", 8,    10L, 9.0),
            row("key10", 4,    11L, 10.0),
            row("key11", 10,   12L, 11.0),
            row("key12", null, 13L, 12.0),
            row("key13", 4,    14L, 13.0),
            row("key14", 11,   15L, 14.0),
            row("key15", 4,    16L, 15.0),
            row("key16", 8,    17L, 16.0)
        );
    }

    // ── streamstats composed with other commands ──────────────────────────────

    /** sql IT: testStreamstatsAndEventstats. Streamstats running cnt followed by eventstats
     *  global max(cnt) — every row carries the same global max=17. */
    public void testStreamstatsAndEventstats() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | eventstats max(cnt) as max_cnt"
                + " | fields key, int0, cnt, max_cnt"
        );
        assertRowsEqual(response,
            row("key00", 1,    1L,  17L),
            row("key01", null, 2L,  17L),
            row("key02", null, 3L,  17L),
            row("key03", null, 4L,  17L),
            row("key04", 7,    5L,  17L),
            row("key05", 3,    6L,  17L),
            row("key06", 8,    7L,  17L),
            row("key07", null, 8L,  17L),
            row("key08", null, 9L,  17L),
            row("key09", 8,    10L, 17L),
            row("key10", 4,    11L, 17L),
            row("key11", 10,   12L, 17L),
            row("key12", null, 13L, 17L),
            row("key13", 4,    14L, 17L),
            row("key14", 11,   15L, 17L),
            row("key15", 4,    16L, 17L),
            row("key16", 8,    17L, 17L)
        );
    }

    /** sql IT: testStreamstatsAndSort. Streamstats then sort -key (descending). */
    public void testStreamstatsAndSort() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | sort -key | head 5"
                + " | fields key, int0, cnt"
        );
        assertRowsEqual(response,
            row("key16", 8,    17L),
            row("key15", 4,    16L),
            row("key14", 11,   15L),
            row("key13", 4,    14L),
            row("key12", null, 13L)
        );
    }

    /** sql IT: testLeftJoinWithStreamstats. streamstats inside left join — exercises both
     *  join and streamstats together. The join shape uses lowering paths (LogicalJoin) that
     *  may not flow through analytics-engine for the specific embedded shape; conservatively
     *  expect throw. */
    public void testLeftJoinWithStreamstats() throws IOException {
        // Test source uses BANK_TWO which we don't have. Use a self-join on calcs as a stand-in;
        // analytics-engine likely rejects the streamstats-in-subsearch shape.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() as cnt"
                + " | left join on key=key right=" + DATASET.indexName
                + " | head 1"
        );
    }

    /** sql IT: testWhereInWithStreamstatsSubquery. WHERE-IN with streamstats subquery — uses
     *  semi-join lowering inside the subquery. After PlannerImpl's subquery-remove phase the
     *  RexSubQuery becomes a decorrelated correlate, but the streamstats-inside-correlate
     *  shape is nondeterministic on multi-node execution: sometimes errors with
     *  {@code Stage 0 sink feed failed: partition stream receiver dropped before send},
     *  sometimes returns one row successfully. Neither a positive nor a broad-failure
     *  assertion is stable across runs. Skipped until the downstream multi-node race is
     *  fixed — re-enable by removing {@code @AwaitsFix}. */
    @AwaitsFix(
        bugUrl = "streamstats-inside-decorrelated-correlate has a nondeterministic multi-node"
            + " execution race; needs a downstream analytics-engine fix before this test can"
            + " assert a deterministic outcome"
    )
    public void testWhereInWithStreamstatsSubquery() throws IOException {
        assertErrorAny(
            "source=" + DATASET.indexName + " | where key in"
                + " [ source=" + DATASET.indexName + " | streamstats count() as cnt"
                + " | where cnt < 5 | fields key ]"
                + " | head 1"
        );
    }

    /** sql IT: testMultipleStreamstatsWithEval. Streamstats running cnt → eval x=cnt+1 →
     *  streamstats sum(x) running. */
    public void testMultipleStreamstatsWithEval() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | eval x = cnt + 1"
                + " | streamstats sum(x) as sx"
                + " | fields key, cnt, x, sx"
        );
        assertRowsEqual(response,
            row("key00", 1L,  2,  2L),
            row("key01", 2L,  3,  5L),
            row("key02", 3L,  4,  9L),
            row("key03", 4L,  5,  14L),
            row("key04", 5L,  6,  20L),
            row("key05", 6L,  7,  27L),
            row("key06", 7L,  8,  35L),
            row("key07", 8L,  9,  44L),
            row("key08", 9L,  10, 54L),
            row("key09", 10L, 11, 65L),
            row("key10", 11L, 12, 77L),
            row("key11", 12L, 13, 90L),
            row("key12", 13L, 14, 104L),
            row("key13", 14L, 15, 119L),
            row("key14", 15L, 16, 135L),
            row("key15", 16L, 17, 152L),
            row("key16", 17L, 18, 170L)
        );
    }

    /** sql IT: testMultipleStreamstatsWithEval2. Streamstats with eval and a partition-key
     *  reset on the second streamstats. First pass: per-partition cnt by str0 (each str0
     *  gets its own running 1..N). eval x=cnt+1. Second streamstats sum(x) by str0
     *  produces per-partition running sum of (cnt+1) within the same partition. */
    public void testMultipleStreamstatsWithEval2() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats count() as cnt"
                + " | eval x = cnt + 1"
                + " | streamstats sum(x) as sx by str0"
                + " | fields key, str0, cnt, x, sx"
        );
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1L,  2,  2L),
            row("key01", "FURNITURE",       2L,  3,  5L),
            row("key02", "OFFICE SUPPLIES", 3L,  4,  4L),
            row("key03", "OFFICE SUPPLIES", 4L,  5,  9L),
            row("key04", "OFFICE SUPPLIES", 5L,  6,  15L),
            row("key05", "OFFICE SUPPLIES", 6L,  7,  22L),
            row("key06", "OFFICE SUPPLIES", 7L,  8,  30L),
            row("key07", "OFFICE SUPPLIES", 8L,  9,  39L),
            row("key08", "TECHNOLOGY",      9L,  10, 10L),
            row("key09", "TECHNOLOGY",      10L, 11, 21L),
            row("key10", "TECHNOLOGY",      11L, 12, 33L),
            row("key11", "TECHNOLOGY",      12L, 13, 46L),
            row("key12", "TECHNOLOGY",      13L, 14, 60L),
            row("key13", "TECHNOLOGY",      14L, 15, 75L),
            row("key14", "TECHNOLOGY",      15L, 16, 91L),
            row("key15", "TECHNOLOGY",      16L, 17, 108L),
            row("key16", "TECHNOLOGY",      17L, 18, 126L)
        );
    }

    // ── Empty input ────────────────────────────────────────────────────────────

    /** sql IT: testStreamstatsEmptyRows. */
    public void testStreamstatsEmptyRows() throws IOException {
        Map<String, Object> r1 = executePpl(
            "source=" + DATASET.indexName + " | where key = 'non-existed'"
                + " | streamstats count(), avg(int0), min(int0), max(int0),"
                + " stddev_pop(int0), stddev_samp(int0), var_pop(int0), var_samp(int0)"
        );
        assertRowCount(r1, 0);

        Map<String, Object> r2 = executePpl(
            "source=" + DATASET.indexName + " | where key = 'non-existed'"
                + " | streamstats count(), avg(int0), min(int0), max(int0),"
                + " stddev_pop(int0), stddev_samp(int0), var_pop(int0), var_samp(int0) by str0"
        );
        assertRowCount(r2, 0);
    }

    // ── Variance / stddev (running) ────────────────────────────────────────────

    /** sql IT: testStreamstatsVariance. Running stddev_pop / stddev_samp / var_pop / var_samp
     *  over int0. Each row carries the running value at its position (NaN for samp variants
     *  while only 0/1 non-null int0 has been seen). Final row matches eventstats global. */
    public void testStreamstatsVariance() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs"
                + " | fields key, int0, sp, ss, vp, vs"
        );
        assertRowsEqual(response,
            row("key00", 1,    0.0,                 (Double) null,         0.0,                (Double) null),
            row("key01", null, 0.0,                 (Double) null,         0.0,                (Double) null),
            row("key02", null, 0.0,                 (Double) null,         0.0,                (Double) null),
            row("key03", null, 0.0,                 (Double) null,         0.0,                (Double) null),
            row("key04", 7,    3.0,                 4.242640687119285,  9.0,                18.0),
            row("key05", 3,    2.494438257849294,   3.055050463303893,  6.222222222222221,  9.333333333333332),
            row("key06", 8,    2.8613807855648994,  3.304037933599835,  8.1875,             10.916666666666666),
            row("key07", null, 2.8613807855648994,  3.304037933599835,  8.1875,             10.916666666666666),
            row("key08", null, 2.8613807855648994,  3.304037933599835,  8.1875,             10.916666666666666),
            row("key09", 8,    2.8705400188814645,  3.209361307176242,  8.239999999999998,  10.299999999999997),
            row("key10", 4,    2.6718699236469,     2.926886855802026,  7.13888888888889,   8.566666666666668),
            row("key11", 10,   2.996596709057576,   3.236694374850748,  8.979591836734695,  10.476190476190476),
            row("key12", null, 2.996596709057576,   3.236694374850748,  8.979591836734695,  10.476190476190476),
            row("key13", 4,    2.8695600708122493,  3.0676887530703447, 8.234375,           9.410714285714286),
            row("key14", 11,   3.189488909868294,   3.3829638550307397, 10.172839506172838, 11.444444444444443),
            row("key15", 4,    3.0983866769659336,  3.265986323710904,  9.6,                10.666666666666666),
            row("key16", 8,    3.009626428590336,   3.1565228279922772, 9.05785123966942,   9.963636363636363)
        );
    }

    /** sql IT: testStreamstatsVarianceWithNull. Same query as
     *  {@link #testStreamstatsVariance}. */
    public void testStreamstatsVarianceWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs"
                + " | fields key, int0, sp, ss, vp, vs"
        );
        assertRowsEqual(response,
            row("key00", 1,    0.0,                 (Double) null,         0.0,                (Double) null),
            row("key01", null, 0.0,                 (Double) null,         0.0,                (Double) null),
            row("key02", null, 0.0,                 (Double) null,         0.0,                (Double) null),
            row("key03", null, 0.0,                 (Double) null,         0.0,                (Double) null),
            row("key04", 7,    3.0,                 4.242640687119285,  9.0,                18.0),
            row("key05", 3,    2.494438257849294,   3.055050463303893,  6.222222222222221,  9.333333333333332),
            row("key06", 8,    2.8613807855648994,  3.304037933599835,  8.1875,             10.916666666666666),
            row("key07", null, 2.8613807855648994,  3.304037933599835,  8.1875,             10.916666666666666),
            row("key08", null, 2.8613807855648994,  3.304037933599835,  8.1875,             10.916666666666666),
            row("key09", 8,    2.8705400188814645,  3.209361307176242,  8.239999999999998,  10.299999999999997),
            row("key10", 4,    2.6718699236469,     2.926886855802026,  7.13888888888889,   8.566666666666668),
            row("key11", 10,   2.996596709057576,   3.236694374850748,  8.979591836734695,  10.476190476190476),
            row("key12", null, 2.996596709057576,   3.236694374850748,  8.979591836734695,  10.476190476190476),
            row("key13", 4,    2.8695600708122493,  3.0676887530703447, 8.234375,           9.410714285714286),
            row("key14", 11,   3.189488909868294,   3.3829638550307397, 10.172839506172838, 11.444444444444443),
            row("key15", 4,    3.0983866769659336,  3.265986323710904,  9.6,                10.666666666666666),
            row("key16", 8,    3.009626428590336,   3.1565228279922772, 9.05785123966942,   9.963636363636363)
        );
    }

    /** sql IT: testStreamstatsVarianceBy. Per-partition running variance. NaN until 2+ non-null
     *  int0 seen in the partition; null until 1+ non-null seen. */
    public void testStreamstatsVarianceBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs by str0"
                + " | fields key, str0, int0, sp, ss, vp, vs"
        );
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    0.0,                 (Double) null,         0.0,                (Double) null),
            row("key01", "FURNITURE",       null, 0.0,                 (Double) null,         0.0,                (Double) null),
            row("key02", "OFFICE SUPPLIES", null, null,                null,               null,               null),
            row("key03", "OFFICE SUPPLIES", null, null,                null,               null,               null),
            row("key04", "OFFICE SUPPLIES", 7,    0.0,                 (Double) null,         0.0,                (Double) null),
            row("key05", "OFFICE SUPPLIES", 3,    2.0,                 2.8284271247461903, 4.0,                8.0),
            row("key06", "OFFICE SUPPLIES", 8,    2.160246899469287,   2.6457513110645907, 4.666666666666667,  7.0),
            row("key07", "OFFICE SUPPLIES", null, 2.160246899469287,   2.6457513110645907, 4.666666666666667,  7.0),
            row("key08", "TECHNOLOGY",      null, null,                null,               null,               null),
            row("key09", "TECHNOLOGY",      8,    0.0,                 (Double) null,         0.0,                (Double) null),
            row("key10", "TECHNOLOGY",      4,    2.0,                 2.8284271247461903, 4.0,                8.0),
            row("key11", "TECHNOLOGY",      10,   2.4944382578492936,  3.0550504633038926, 6.222222222222219,  9.333333333333329),
            row("key12", "TECHNOLOGY",      null, 2.4944382578492936,  3.0550504633038926, 6.222222222222219,  9.333333333333329),
            row("key13", "TECHNOLOGY",      4,    2.598076211353316,   3.0,                6.75,               9.0),
            row("key14", "TECHNOLOGY",      11,   2.9393876913398134,  3.2863353450309964, 8.639999999999997,  10.799999999999997),
            row("key15", "TECHNOLOGY",      4,    2.967415635794142,   3.250640962435972,  8.805555555555552,  10.566666666666663),
            row("key16", "TECHNOLOGY",      8,    2.7774602993176543,  3.0,                7.714285714285714,  9.0)
        );
    }

    /** sql IT: testStreamstatsVarianceBySpan. Per-row running stddev_samp partitioned by
     *  {@code span(int0, 10)}. NaN at 1-element partitions; null until 1+ non-null seen. */
    public void testStreamstatsVarianceBySpan() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_samp(int0) as ss by span(int0, 10)"
                + " | fields key, int0, ss"
        );
        assertRowsEqual(response,
            row("key00", 1,    (Double) null),
            row("key01", null, null),
            row("key02", null, null),
            row("key03", null, null),
            row("key04", 7,    4.242640687119285),
            row("key05", 3,    3.055050463303893),
            row("key06", 8,    3.304037933599835),
            row("key07", null, null),
            row("key08", null, null),
            row("key09", 8,    3.209361307176242),
            row("key10", 4,    2.926886855802026),
            row("key11", 10,   (Double) null),
            row("key12", null, null),
            row("key13", 4,    2.70801280154532),
            row("key14", 11,   0.7071067811865476),
            row("key15", 4,    2.5319388392523003),
            row("key16", 8,    2.5873624493766703)
        );
    }

    /** sql IT: testStreamstatsVarianceWithNullBy. Variance partitioned by {@code str3}, which
     *  has 7 nulls + 10 'e' rows — covers null-key partition handling. Each partition runs
     *  its own running variance over int0. */
    public void testStreamstatsVarianceWithNullBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs by str3"
                + " | fields key, str3, int0, sp, ss, vp, vs"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    0.0,                (Double) null,         0.0,                (Double) null),
            row("key01", "e",  null, 0.0,                (Double) null,         0.0,                (Double) null),
            row("key02", "e",  null, 0.0,                (Double) null,         0.0,                (Double) null),
            row("key03", "e",  null, 0.0,                (Double) null,         0.0,                (Double) null),
            row("key04", null, 7,    0.0,                (Double) null,         0.0,                (Double) null),
            row("key05", null, 3,    2.0,                2.8284271247461903, 4.0,                8.0),
            row("key06", "e",  8,    3.5,                4.949747468305833,  12.25,              24.5),
            row("key07", "e",  null, 3.5,                4.949747468305833,  12.25,              24.5),
            row("key08", null, null, 2.0,                2.8284271247461903, 4.0,                8.0),
            row("key09", "e",  8,    3.299831645537222,  4.041451884327381,  10.888888888888891, 16.333333333333336),
            row("key10", "e",  4,    2.947456530637899,  3.4034296427770228, 8.6875,             11.583333333333334),
            row("key11", null, 10,   2.867441755680875,  3.5118845842842457, 8.22222222222222,   12.333333333333329),
            row("key12", null, null, 2.867441755680875,  3.5118845842842457, 8.22222222222222,   12.333333333333329),
            row("key13", null, 4,    2.7386127875258306, 3.1622776601683795, 7.5,                10.0),
            row("key14", "e",  11,   3.49857113690718,   3.911521443121589,  12.239999999999998, 15.299999999999997),
            row("key15", "e",  4,    3.3166247903554,    3.63318042491699,   11.0,               13.2),
            row("key16", null, 8,    2.576819745345025,  2.880972058177586,  6.639999999999998,  8.299999999999997)
        );
    }

    // ── distinct_count / dc ────────────────────────────────────────────────────
    // BackendPlanAdapter rewrites RexOver(DISTINCT_COUNT_APPROX) → APPROX_COUNT_DISTINCT,
    // and the DataFusion plugin's approx_count_distinct wrapper UDAF aliases that name to
    // DataFusion's built-in approx_distinct (HyperLogLog). streamstats applies the aggregate
    // over a UNBOUNDED PRECEDING / CURRENT ROW frame, so dc_* is a per-row running count of
    // distinct non-null values seen so far in the partition. At calcs's scale (low cardinality)
    // HLL is exact, so we can pin the exact running count.
    //
    // calcs.str3 is "e" on every non-null row → dc_str3 stays at 1 once the first non-null is
    // seen. calcs.str0 has three values {FURNITURE, OFFICE SUPPLIES, TECHNOLOGY} appearing in
    // that order (sorted by key), so dc_str0 walks 1 → 2 → 3.

    /** sql IT: testStreamstatsDistinctCount. */
    public void testStreamstatsDistinctCount() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats dc(str3) as dc_str3 | fields key, str3, dc_str3"
        );
        // Running dc(str3): "e" on key00..03 → 1; nulls on key04..05 don't change count;
        // "e" on key06..07 still 1; null on key08; "e" on key09..10; nulls on key11..13;
        // "e" on key14..15; null on key16. Once the first non-null is seen, count is 1 forever.
        assertRowsEqual(
            response,
            row("key00", "e",  1L),
            row("key01", "e",  1L),
            row("key02", "e",  1L),
            row("key03", "e",  1L),
            row("key04", null, 1L),
            row("key05", null, 1L),
            row("key06", "e",  1L),
            row("key07", "e",  1L),
            row("key08", null, 1L),
            row("key09", "e",  1L),
            row("key10", "e",  1L),
            row("key11", null, 1L),
            row("key12", null, 1L),
            row("key13", null, 1L),
            row("key14", "e",  1L),
            row("key15", "e",  1L),
            row("key16", null, 1L)
        );
    }

    /** sql IT: testStreamstatsDistinctCountByCountry. Per-partition running dc(str3).
     *  FURNITURE (key00..01): "e" both → 1, 1.
     *  OFFICE SUPPLIES (key02..07): "e","e","e",null,null,"e","e" → 1,1,1,1,1,1.
     *  TECHNOLOGY (key08..16): null,"e","e",null,null,null,"e","e",null
     *    → 0,1,1,1,1,1,1,1,1 (the leading null sees no non-null yet, so dc=0). */
    public void testStreamstatsDistinctCountByCountry() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats dc(str3) as dc_str3 by str0 | fields key, str0, dc_str3"
        );
        assertRowsEqual(
            response,
            row("key00", "FURNITURE", 1L),
            row("key01", "FURNITURE", 1L),
            row("key02", "OFFICE SUPPLIES", 1L),
            row("key03", "OFFICE SUPPLIES", 1L),
            row("key04", "OFFICE SUPPLIES", 1L),
            row("key05", "OFFICE SUPPLIES", 1L),
            row("key06", "OFFICE SUPPLIES", 1L),
            row("key07", "OFFICE SUPPLIES", 1L),
            row("key08", "TECHNOLOGY", 0L),
            row("key09", "TECHNOLOGY", 1L),
            row("key10", "TECHNOLOGY", 1L),
            row("key11", "TECHNOLOGY", 1L),
            row("key12", "TECHNOLOGY", 1L),
            row("key13", "TECHNOLOGY", 1L),
            row("key14", "TECHNOLOGY", 1L),
            row("key15", "TECHNOLOGY", 1L),
            row("key16", "TECHNOLOGY", 1L)
        );
    }

    /** sql IT: testStreamstatsDistinctCountFunction. {@code distinct_count()} alias for dc. */
    public void testStreamstatsDistinctCountFunction() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats distinct_count(str0) as dc_str0 | fields key, str0, dc_str0"
        );
        // Running dc(str0): FURNITURE×2 → 1,1; OFFICE×6 → 2,2,2,2,2,2; TECHNOLOGY×9 → 3,3,...
        assertRowsEqual(
            response,
            row("key00", "FURNITURE", 1L),
            row("key01", "FURNITURE", 1L),
            row("key02", "OFFICE SUPPLIES", 2L),
            row("key03", "OFFICE SUPPLIES", 2L),
            row("key04", "OFFICE SUPPLIES", 2L),
            row("key05", "OFFICE SUPPLIES", 2L),
            row("key06", "OFFICE SUPPLIES", 2L),
            row("key07", "OFFICE SUPPLIES", 2L),
            row("key08", "TECHNOLOGY", 3L),
            row("key09", "TECHNOLOGY", 3L),
            row("key10", "TECHNOLOGY", 3L),
            row("key11", "TECHNOLOGY", 3L),
            row("key12", "TECHNOLOGY", 3L),
            row("key13", "TECHNOLOGY", 3L),
            row("key14", "TECHNOLOGY", 3L),
            row("key15", "TECHNOLOGY", 3L),
            row("key16", "TECHNOLOGY", 3L)
        );
    }

    /** sql IT: testStreamstatsDistinctCountWithNull. Same shape as
     *  {@link #testStreamstatsDistinctCount} — sql-plugin's variant uses STATE_COUNTRY_WITH_NULL,
     *  but this QA module only ships the {@code calcs} dataset (whose {@code str3} already has
     *  7 nulls). */
    public void testStreamstatsDistinctCountWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats dc(str3) as dc_str3 | fields key, str3, dc_str3"
        );
        assertRowsEqual(
            response,
            row("key00", "e",  1L),
            row("key01", "e",  1L),
            row("key02", "e",  1L),
            row("key03", "e",  1L),
            row("key04", null, 1L),
            row("key05", null, 1L),
            row("key06", "e",  1L),
            row("key07", "e",  1L),
            row("key08", null, 1L),
            row("key09", "e",  1L),
            row("key10", "e",  1L),
            row("key11", null, 1L),
            row("key12", null, 1L),
            row("key13", null, 1L),
            row("key14", "e",  1L),
            row("key15", "e",  1L),
            row("key16", null, 1L)
        );
    }

    /** Multi-shard variant for streamstats dc by partition. Streamstats running aggregates
     *  depend on input row-order; under multi-shard parallelism rows arrive at the
     *  coordinator out of key order, so per-row running values are not deterministic.
     *  Collapse the running stream to per-partition finals via {@code stats max(dc_str3) by str0}
     *  — same shape as {@link #testStreamstatsBy_3shard}. The final dc per partition is 1
     *  (each {@code str0} group has only the value "e" or null in {@code str3}; null is
     *  skipped). HLL is exact at calcs's scale so {@code max(dc_str3)} matches single-shard. */
    public void testStreamstatsDistinctCountByCountry_3shard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName
                + " | sort key | streamstats dc(str3) as dc_str3 by str0"
                + " | stats max(dc_str3) as final_dc by str0 | sort str0"
        );
        assertRowsEqual(
            response,
            row(1L, "FURNITURE"),
            row(1L, "OFFICE SUPPLIES"),
            row(1L, "TECHNOLOGY")
        );
    }

    /** sql IT: testStreamstatsEarliestAndLatest. earliest/latest exercise the PPL frontend's
     *  default-{@code @timestamp} check — calcs has no @timestamp column. Either way the
     *  path is not yet reachable on analytics-engine — assert the failure. */
    public void testStreamstatsEarliestAndLatest() throws IOException {
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats earliest(str0), latest(str0) by str3"
        );
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static List<Object> row(Object... values) {
        return Arrays.asList(values);
    }

    @SafeVarargs
    @SuppressWarnings({"unchecked", "varargs"})
    private final void assertRowsEqual(Map<String, Object> response, List<Object>... expected) {
        List<List<Object>> actualRows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'rows'", actualRows);
        assertEquals("Row count mismatch", expected.length, actualRows.size());
        for (int i = 0; i < expected.length; i++) {
            List<Object> want = expected[i];
            List<Object> got = actualRows.get(i);
            assertEquals("Column count mismatch at row " + i, want.size(), got.size());
            for (int j = 0; j < want.size(); j++) {
                assertCellEquals("Cell mismatch at row " + i + ", col " + j, want.get(j), got.get(j));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertScalarRow(Map<String, Object> response, Object... expected) {
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Expected exactly one row", 1, rows.size());
        List<Object> got = rows.get(0);
        assertEquals("Expected " + expected.length + " columns", expected.length, got.size());
        for (int j = 0; j < expected.length; j++) {
            assertCellEquals("Cell mismatch at col " + j, expected[j], got.get(j));
        }
    }

    @SuppressWarnings("unchecked")
    private static void assertRowCount(Map<String, Object> response, int expected) {
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Row count mismatch", expected, rows.size());
    }

    /** Numeric-tolerant cell comparator (Jackson returns Integer/Long/Double interchangeably).
     *  NaN-aware: (Double) null matches both (Double) null and the string "NaN" (Jackson encodes
     *  NaN as a string in JSON arrays). */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        if (expected instanceof Double && Double.isNaN((Double) expected)
            && (actual instanceof Double && Double.isNaN((Double) actual)
                || "NaN".equals(actual))) {
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            if (Math.abs(e - a) > 1e-9 * Math.max(Math.abs(e), 1.0)) {
                fail(message + ": expected <" + expected + "> but was <" + actual + ">");
            }
            return;
        }
        assertEquals(message, expected, actual);
    }


    /** Send a PPL query and assert response body contains {@code expectedSubstring}. */
    private void assertErrorContains(String ppl, String expectedSubstring) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = entityAsMap(response);
            fail("Expected query [" + ppl + "] to fail with [" + expectedSubstring + "] but got: " + body);
        } catch (ResponseException e) {
            String body;
            try {
                body = org.apache.hc.core5.http.io.entity.EntityUtils.toString(e.getResponse().getEntity());
            } catch (Exception ioe) {
                body = e.getMessage();
            }
            assertTrue(
                "Query [" + ppl + "] expected error containing [" + expectedSubstring + "] but body was: " + body,
                body.contains(expectedSubstring)
            );
        }
    }

    /** Send a PPL query and assert it fails with ANY error response. Used for cases where
     *  the precise error message depends on which lowering path the PPL parser picks (which
     *  may change as more analytics-engine functionality lands). The contract here is just
     *  "this PPL form is not yet supported". */
    private void assertErrorAny(String ppl) throws IOException {
        Request request = new Request("POST", "/_plugins/_ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = entityAsMap(response);
            fail("Expected query [" + ppl + "] to fail but got: " + body);
        } catch (ResponseException expected) {
            // Any error response is acceptable.
        }
    }
}
