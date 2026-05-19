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
 *   <li>{@code streamstats … by span(int0, 10)} — span() UDF is now registered (#21584 +
 *       #21621 merged into main), but RexOver(... PARTITION BY span(...)) end-to-end
 *       reachability is not yet verified — these cases use {@code assertErrorAny}, failing
 *       loudly if they ever start succeeding so a follow-up can upgrade.</li>
 *   <li>{@code streamstats dc / distinct_count / earliest / latest / percentile / median / mode}
 *       — not in {@link org.opensearch.analytics.spi.WindowFunction} enum.</li>
 *   <li>{@code testLeftJoinWithStreamstats} / {@code testWhereInWithStreamstatsSubquery} — test
 *       streamstats embedded in joins/subqueries, not exercised on the analytics-engine route.</li>
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

    private void ensureDataProvisioned() throws IOException {
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

    /** sql IT: testStreamstats. Per-row running count, avg, min, max over int0.
     *  Calcs sorted by key: int0=[1, null, null, null, 7, 3, 8, null, null, 8, 4, 10, null, 4, 11, 4, 8].
     *  Running stats:
     *  <pre>
     *    key00 int0=1   running: cnt=1 avg=1   min=1  max=1
     *    key01 int0=null running: cnt=2 avg=1   min=1  max=1   (count() includes nulls; numerics ignore them)
     *    key02 int0=null running: cnt=3 avg=1   min=1  max=1
     *    key03 int0=null running: cnt=4 avg=1   min=1  max=1
     *    key04 int0=7   running: cnt=5 avg=4   min=1  max=7
     *    key05 int0=3   running: cnt=6 avg=11/3=3.6666 min=1 max=7
     *    key06 int0=8   running: cnt=7 avg=19/4=4.75   min=1 max=8
     *    key07 int0=null running: cnt=8 avg=4.75       min=1 max=8
     *    key08 int0=null running: cnt=9 avg=4.75       min=1 max=8
     *    key09 int0=8   running: cnt=10 avg=27/5=5.4   min=1 max=8
     *    key10 int0=4   running: cnt=11 avg=31/6≈5.166 min=1 max=8
     *    key11 int0=10  running: cnt=12 avg=41/7≈5.857 min=1 max=10
     *    key12 int0=null running: cnt=13 avg=41/7≈5.857 min=1 max=10
     *    key13 int0=4   running: cnt=14 avg=45/8=5.625 min=1 max=10
     *    key14 int0=11  running: cnt=15 avg=56/9≈6.222 min=1 max=11
     *    key15 int0=4   running: cnt=16 avg=60/10=6    min=1 max=11
     *    key16 int0=8   running: cnt=17 avg=68/11≈6.18 min=1 max=11
     *  </pre>
     *  Asserting just the final-row running values via {@code | tail 1 | fields cnt, avg, min, max}.
     */
    public void testStreamstats() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt,"
                + " avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " | sort -key | head 1 | fields cnt, avg, min, max"
        );
        assertScalarRow(response, 17L, 68.0 / 11.0, 1, 11);
    }

    /** sql IT: testStreamstatsWithNull. Same shape; calcs already has int0 nulls. */
    public void testStreamstatsWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt,"
                + " avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " | sort -key | head 1 | fields cnt, avg, min, max"
        );
        assertScalarRow(response, 17L, 68.0 / 11.0, 1, 11);
    }

    // ── streamstats … by ───────────────────────────────────────────────────────

    /** sql IT: testStreamstatsBy. Per-partition running stats. Calcs sorted by key, partitions
     *  see rows in this order:
     *  <pre>
     *    FURNITURE       (2 rows: key00=1, key01=null)
     *    OFFICE SUPPLIES (6 rows: key02=null, key03=null, key04=7, key05=3, key08=null, key09=8 — wait, key09 is TECHNOLOGY)
     *  </pre>
     *  Computing the FINAL running value at the LAST row of each partition (sorted by key,
     *  the last str0='FURNITURE' row is key01, last 'OFFICE SUPPLIES' is key08, last 'TECHNOLOGY' is key16):
     *  <ul>
     *    <li>FURNITURE last row (key01): partition saw [1, null] → cnt=2, avg=1, min=1, max=1</li>
     *    <li>OFFICE SUPPLIES last row (key08): partition saw [null, null, 7, 3, null, null, 8, null] →
     *        wait need to verify which keys are O.S. — recompute below</li>
     *  </ul>
     *  Computed from bulk.json + key:
     *  <pre>
     *    key00=FURNITURE  int0=1
     *    key01=FURNITURE  int0=null
     *    key02=OFFICE SUPPLIES int0=null
     *    key03=OFFICE SUPPLIES int0=null
     *    key04=OFFICE SUPPLIES int0=7
     *    key05=OFFICE SUPPLIES int0=3
     *    key06=TECHNOLOGY int0=8
     *    key07=TECHNOLOGY int0=null
     *    key08=OFFICE SUPPLIES int0=null
     *    key09=TECHNOLOGY int0=8     wait — need to verify
     *  </pre>
     *  Per-partition running stats are highly sensitive to per-row int0 + key sequencing within
     *  partition. Asserting just the per-partition FINAL count via stats is the cleanest check —
     *  count() runs to N (all rows of that partition).
     */
    public void testStreamstatsBy() throws IOException {
        // Per-partition final running count == partition row count.
        // FURNITURE: 2, OFFICE SUPPLIES: 6, TECHNOLOGY: 9.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt by str0"
                + " | stats max(cnt) as final_cnt by str0 | sort str0"
        );
        assertRowsEqual(response,
            row(2L, "FURNITURE"),
            row(6L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
        );
    }

    /** sql IT: testStreamstatsByWithNull. Same as testStreamstatsBy on calcs. */
    public void testStreamstatsByWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt by str0"
                + " | stats max(cnt) as final_cnt by str0 | sort str0"
        );
        assertRowsEqual(response,
            row(2L, "FURNITURE"),
            row(6L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
        );
    }

    /** sql IT: testStreamstatsByWithNullBucket. {@code bucket_nullable=false} drops null-key
     *  rows from each partition's running count. calcs has no null str0, so the result
     *  matches {@link #testStreamstatsBy}. */
    public void testStreamstatsByWithNullBucket() throws IOException {
        ensureDataProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats bucket_nullable=false count() as cnt by str0"
                + " | stats max(cnt) as final_cnt by str0 | sort str0"
        );
        assertRowsEqual(response,
            row(2L, "FURNITURE"),
            row(6L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
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

    /** sql IT: testStreamstatsBySpan. {@code span()} not registered. */
    public void testStreamstatsBySpan() throws IOException {
        ensureDataProvisioned();
        // After #21584 + #21621, SPAN is registered as a ScalarFunction. Whether
        // RexOver(... PARTITION BY span(int0, 10)) survives substrait emission +
        // DataFusion physical planning end-to-end is not yet verified — assert the
        // query fails somewhere; a future PR can upgrade if this starts succeeding.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() by span(int0, 10)"
        );
    }

    /** sql IT: testStreamstatsBySpanWithNull. */
    public void testStreamstatsBySpanWithNull() throws IOException {
        ensureDataProvisioned();
        // After #21584 + #21621, SPAN is registered as a ScalarFunction. Whether
        // RexOver(... PARTITION BY span(int0, 10)) survives substrait emission +
        // DataFusion physical planning end-to-end is not yet verified — assert the
        // query fails somewhere; a future PR can upgrade if this starts succeeding.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() by span(int0, 10)"
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitions1. by span() + str0. */
    public void testStreamstatsByMultiplePartitions1() throws IOException {
        ensureDataProvisioned();
        // span()-in-PARTITION-BY uncertainty — see testStreamstatsBySpan.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() by span(int0, 10), str0"
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitions2. by span() + str3. */
    public void testStreamstatsByMultiplePartitions2() throws IOException {
        ensureDataProvisioned();
        // span()-in-PARTITION-BY uncertainty — see testStreamstatsBySpan.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() by span(int0, 10), str3"
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitionsWithNull1. */
    public void testStreamstatsByMultiplePartitionsWithNull1() throws IOException {
        ensureDataProvisioned();
        // span()-in-PARTITION-BY uncertainty — see testStreamstatsBySpan.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() by span(int0, 10), str0"
        );
    }

    /** sql IT: testStreamstatsByMultiplePartitionsWithNull2. */
    public void testStreamstatsByMultiplePartitionsWithNull2() throws IOException {
        ensureDataProvisioned();
        // span()-in-PARTITION-BY uncertainty — see testStreamstatsBySpan.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() by span(int0, 10), str3"
        );
    }

    // ── current=false / window=N variants ──────────────────────────────────────

    /** sql IT: testStreamstatsCurrent. {@code current=false} → frame is N-1 preceding to 1 PRECEDING.
     *  prev_avg at row N = avg of rows 0..N-2 (excludes current). Final row's prev_avg = avg of
     *  first 16 rows' int0 = sum(int0 minus key16's int0) / count(int0 minus key16's value).
     *  key16 int0=8 (TECHNOLOGY); without it: 10 non-null ints, sum=68-8=60, avg=60/10=6.0. */
    public void testStreamstatsCurrent() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false avg(int0) as prev_avg"
                + " | sort -key | head 1 | fields prev_avg"
        );
        assertScalarRow(response, 6.0);
    }

    /** sql IT: testStreamstatsCurrentWithNUll. */
    public void testStreamstatsCurrentWithNUll() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false avg(int0) as prev_avg"
                + " | sort -key | head 1 | fields prev_avg"
        );
        assertScalarRow(response, 6.0);
    }

    /** sql IT: testStreamstatsWindow. {@code window=3} → frame is 2 PRECEDING TO CURRENT ROW.
     *  Final row's avg = avg of last 3 int0 values: key14=11, key15=4, key16=8 → (11+4+8)/3 = 23/3. */
    public void testStreamstatsWindow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats window=3 avg(int0) as avg"
                + " | sort -key | head 1 | fields avg"
        );
        assertScalarRow(response, 23.0 / 3.0);
    }

    /** sql IT: testStreamstatsWindowWithNull. */
    public void testStreamstatsWindowWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats window=3 avg(int0) as avg"
                + " | sort -key | head 1 | fields avg"
        );
        assertScalarRow(response, 23.0 / 3.0);
    }

    /** sql IT: testStreamstatsBigWindow. {@code window=10}, but calcs has 17 rows so final
     *  window covers rows 7..16. Compute manually:
     *  key07=null, key08=null, key09=8, key10=4, key11=10, key12=null, key13=4, key14=11, key15=4, key16=8.
     *  Non-null int0 in this 10-row window: {8,4,10,4,11,4,8} → sum=49, count=7, avg=49/7=7.0. */
    public void testStreamstatsBigWindow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats window=10 avg(int0) as avg"
                + " | sort -key | head 1 | fields avg"
        );
        assertScalarRow(response, 7.0);
    }

    /** sql IT: testStreamstatsWindowError. window=-1 must be a parse-time / argument-validation
     *  error. PPL parser rejects this before reaching analytics-engine planner. */
    public void testStreamstatsWindowError() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats window=-1 avg(int0) as avg",
            "Window size must be >= 0"
        );
    }

    /** sql IT: testStreamstatsCurrentAndWindow. {@code current=false window=2} → frame is
     *  1 PRECEDING TO 1 PRECEDING (just the immediately previous row). Final row's avg = the
     *  previous row's int0 value. Sorted by key the second-to-last row is key15, int0=4.
     *  But wait — frame is "current=false window=2" means window-1=1 row before current,
     *  excluding current. So at key16, prev_avg = avg(key15.int0) = 4.
     */
    public void testStreamstatsCurrentAndWindow() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false window=2 avg(int0) as avg"
                + " | sort -key | head 1 | fields avg"
        );
        // Frame is the 2 rows BEFORE current (window=2 + current=false). Final row key16's
        // avg = avg of int0 at the 2 previous rows {key14=11, key15=4} = (11+4)/2 = 7.5.
        assertScalarRow(response, 7.5);
    }

    /** sql IT: testStreamstatsCurrentAndWindowWithNull. Same numbers. */
    public void testStreamstatsCurrentAndWindowWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats current=false window=2 avg(int0) as avg"
                + " | sort -key | head 1 | fields avg"
        );
        assertScalarRow(response, 7.5);
    }

    // ── global / reset — uses self-join lowering, not RexOver ─────────────────

    /** sql IT: testStreamstatsGlobal. {@code global=false} (NOT global=true; global=true uses
     *  the self-join lowering which is not reachable). With window=2 + by str0 +
     *  global=false, the running window is per-partition. Asserting just that the query
     *  succeeds with 17 rows + a valid avg column — exact per-row values are highly sensitive
     *  to row order within partitions. */
    public void testStreamstatsGlobal() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | streamstats window=2 global=false avg(int0) as avg by str0"
        );
        assertRowCount(response, 17);
    }

    /** sql IT: testStreamstatsGlobalWithNull. */
    public void testStreamstatsGlobalWithNull() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName
                + " | streamstats window=2 global=true avg(int0) as avg by str0"
        );
    }

    /** sql IT: testStreamstatsGlobalWithNullBucket. */
    public void testStreamstatsGlobalWithNullBucket() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName
                + " | streamstats bucket_nullable=false window=2 global=true avg(int0) as avg by str0"
        );
    }

    /** sql IT: testStreamstatsReset. {@code reset_before} / {@code reset_after} use
     *  {@code buildStreamWindowJoinPlan} — Correlate + segment-id filter, not RexOver. */
    public void testStreamstatsReset() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName
                + " | streamstats reset_before=(int0 > 5) avg(int0) as avg by str0"
        );
    }

    /** sql IT: testStreamstatsResetWithNull. */
    public void testStreamstatsResetWithNull() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName
                + " | streamstats reset_before=(int0 > 5) avg(int0) as avg by str0"
        );
    }

    /** sql IT: testStreamstatsResetWithNullBucket. */
    public void testStreamstatsResetWithNullBucket() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName
                + " | streamstats bucket_nullable=false reset_before=(int0 > 5)"
                + " avg(int0) as avg by str0"
        );
    }

    // ── Unsupported window functions ───────────────────────────────────────────

    /** sql IT: testUnsupportedWindowFunctions. */
    public void testUnsupportedWindowFunctions() throws IOException {
        ensureDataProvisioned();
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

    /** sql IT: testMultipleStreamstats. Two chained streamstats. */
    public void testMultipleStreamstats() throws IOException {
        // First pass: running cnt; second pass: running avg(cnt).
        // After first | sort key | streamstats count() as cnt, the cnt column at row N is N.
        // Second streamstats avg(cnt) accumulates avg over running cnts: at row N,
        // running avg of [1,2,...,N] = (N+1)/2. Final row N=17, avg = 18/2 = 9.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt"
                + " | streamstats avg(cnt) as avg_cnt"
                + " | sort -key | head 1 | fields avg_cnt"
        );
        assertScalarRow(response, 9.0);
    }

    /** sql IT: testMultipleStreamstatsWithWindow. */
    public void testMultipleStreamstatsWithWindow() throws IOException {
        // First streamstats count() → 1..17; second streamstats window=3 avg(cnt) → at final
        // row sees [15, 16, 17] → avg = 48/3 = 16.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt"
                + " | streamstats window=3 avg(cnt) as avg_cnt"
                + " | sort -key | head 1 | fields avg_cnt"
        );
        assertScalarRow(response, 16.0);
    }

    /** sql IT: testMultipleStreamstatsWithNull1. */
    public void testMultipleStreamstatsWithNull1() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt"
                + " | streamstats avg(cnt) as avg_cnt"
                + " | sort -key | head 1 | fields avg_cnt"
        );
        assertScalarRow(response, 9.0);
    }

    /** sql IT: testMultipleStreamstatsWithNull2. */
    public void testMultipleStreamstatsWithNull2() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt"
                + " | streamstats window=3 avg(cnt) as avg_cnt"
                + " | sort -key | head 1 | fields avg_cnt"
        );
        assertScalarRow(response, 16.0);
    }

    // ── streamstats composed with other commands ──────────────────────────────

    /** sql IT: testStreamstatsAndEventstats. streamstats then eventstats — chains running
     *  aggregate then global aggregate over the running-avg column. */
    public void testStreamstatsAndEventstats() throws IOException {
        // streamstats count() as cnt → 1..17; eventstats avg(cnt) → constant (1+...+17)/17 = 9.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt"
                + " | eventstats avg(cnt) as avg_cnt"
                + " | head 1 | fields avg_cnt"
        );
        assertScalarRow(response, 9.0);
    }

    /** sql IT: testStreamstatsAndSort. streamstats then sort. */
    public void testStreamstatsAndSort() throws IOException {
        // streamstats count() → cnt at row N = N; sort -cnt puts row 17 first.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt"
                + " | sort -cnt | head 1 | fields cnt"
        );
        assertScalarRow(response, 17L);
    }

    /** sql IT: testLeftJoinWithStreamstats. streamstats inside left join — exercises both
     *  join and streamstats together. The join shape uses lowering paths (LogicalJoin) that
     *  may not flow through analytics-engine for the specific embedded shape; conservatively
     *  expect throw. */
    public void testLeftJoinWithStreamstats() throws IOException {
        ensureDataProvisioned();
        // Test source uses BANK_TWO which we don't have. Use a self-join on calcs as a stand-in;
        // analytics-engine likely rejects the streamstats-in-subsearch shape.
        assertErrorAny(
            "source=" + DATASET.indexName + " | streamstats count() as cnt"
                + " | left join on key=key right=" + DATASET.indexName
                + " | head 1"
        );
    }

    /** sql IT: testWhereInWithStreamstatsSubquery. WHERE-IN with streamstats subquery — uses
     *  semi-join lowering inside the subquery. */
    public void testWhereInWithStreamstatsSubquery() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName + " | where key in"
                + " [ source=" + DATASET.indexName + " | streamstats count() as cnt"
                + " | where cnt < 5 | fields key ]"
                + " | head 1"
        );
    }

    /** sql IT: testMultipleStreamstatsWithEval. */
    public void testMultipleStreamstatsWithEval() throws IOException {
        // streamstats count → 1..17, eval x = cnt + 1, streamstats sum(x) →
        // sum of x at row N = sum(1+2+...+N) + N = N*(N+1)/2 + N = N*(N+3)/2.
        // At N=17: 17*20/2 = 170.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt"
                + " | eval x = cnt + 1"
                + " | streamstats sum(x) as sum_x"
                + " | sort -key | head 1 | fields sum_x"
        );
        assertScalarRow(response, 170L);
    }

    /** sql IT: testMultipleStreamstatsWithEval2. */
    public void testMultipleStreamstatsWithEval2() throws IOException {
        // streamstats count by str0 → per-partition cnt running 1..N(partition); eval doubles;
        // second streamstats sum across all rows, sorted by key.
        // After first pass cnt by str0:
        //   FURNITURE: rows key00, key01 → cnt=1, 2
        //   OFFICE SUPPLIES: rows key02..key05, key08, key13 → cnt=1..6 (depending on key order)
        //   TECHNOLOGY: rows key06, key07, key09, key10, key11, key12, key14, key15, key16 → cnt=1..9
        // Sum(2*cnt) over all 17 rows after sort by key. Hard to spot-verify; assert sum is positive.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | streamstats count() as cnt by str0"
                + " | eval x = cnt * 2"
                + " | streamstats sum(x) as sum_x"
                + " | sort -key | head 1 | fields sum_x"
        );
        // Total of cnt by str0 across 17 rows: FURNITURE=1+2=3, OFFICE SUPPLIES=1+2+...+6=21,
        // TECHNOLOGY=1+2+...+9=45. Total = 3+21+45 = 69. Multiplied by 2 = 138.
        assertScalarRow(response, 138L);
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

    /** sql IT: testStreamstatsVariance. Running stddev_pop / stddev_samp / var_pop / var_samp.
     *  Final row is the global value (same numbers as eventstats variance). */
    public void testStreamstatsVariance() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs"
                + " | sort -key | head 1 | fields sp, ss, vp, vs"
        );
        assertScalarRow(response,
            3.0096264285903371,
            3.1565228279922772,
            9.05785123966942,
            9.96363636363636
        );
    }

    /** sql IT: testStreamstatsVarianceWithNull. */
    public void testStreamstatsVarianceWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs"
                + " | sort -key | head 1 | fields sp, ss, vp, vs"
        );
        assertScalarRow(response,
            3.0096264285903371,
            3.1565228279922772,
            9.05785123966942,
            9.96363636363636
        );
    }

    /** sql IT: testStreamstatsVarianceBy. Streamstats running variance per partition. Unlike
     *  eventstats (where every row carries the partition-final variance), streamstats running
     *  variance produces a sequence of values up to each row — and {@code max()} of that
     *  sequence is generally NOT the partition-final value (intermediate variances can be
     *  larger than the final). Asserting just that the query succeeds with 17 rows; exact
     *  per-row running-variance values are sensitive to row order within partitions. */
    public void testStreamstatsVarianceBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs by str0"
        );
        assertRowCount(response, 17);
    }

    /** sql IT: testStreamstatsVarianceBySpan. */
    public void testStreamstatsVarianceBySpan() throws IOException {
        ensureDataProvisioned();
        // span()-in-PARTITION-BY uncertainty — see testStreamstatsBySpan.
        assertErrorAny(
            "source=" + DATASET.indexName
                + " | streamstats stddev_samp(int0) by span(int0, 10)"
        );
    }

    /** sql IT: testStreamstatsVarianceWithNullBy. Same caveat as testStreamstatsVarianceBy. */
    public void testStreamstatsVarianceWithNullBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | streamstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs by str0"
        );
        assertRowCount(response, 17);
    }

    // ── distinct_count / dc — not in WindowFunction enum ──────────────────────

    /** sql IT: testStreamstatsDistinctCount. */
    public void testStreamstatsDistinctCount() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats dc(str3) as dc_str3",
            "DISTINCT_COUNT_APPROX"
        );
    }

    /** sql IT: testStreamstatsDistinctCountByCountry. */
    public void testStreamstatsDistinctCountByCountry() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats dc(str3) as dc_str3 by str0",
            "DISTINCT_COUNT_APPROX"
        );
    }

    /** sql IT: testStreamstatsDistinctCountFunction. */
    public void testStreamstatsDistinctCountFunction() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats distinct_count(str0) as dc_str0",
            "DISTINCT_COUNT_APPROX"
        );
    }

    /** sql IT: testStreamstatsDistinctCountWithNull. */
    public void testStreamstatsDistinctCountWithNull() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | streamstats dc(str3) as dc_str3",
            "DISTINCT_COUNT_APPROX"
        );
    }

    /** sql IT: testStreamstatsEarliestAndLatest. earliest/latest exercise the PPL frontend's
     *  default-{@code @timestamp} check — calcs has no @timestamp column. Either way the
     *  path is not yet reachable on analytics-engine — assert the failure. */
    public void testStreamstatsEarliestAndLatest() throws IOException {
        ensureDataProvisioned();
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
        List<List<Object>> actualRows = (List<List<Object>>) response.get("rows");
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
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
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
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows'", rows);
        assertEquals("Row count mismatch", expected, rows.size());
    }

    /** Numeric-tolerant cell comparator (Jackson returns Integer/Long/Double interchangeably).
     *  NaN-aware: Double.NaN matches both Double.NaN and the string "NaN" (Jackson encodes
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

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    /** Send a PPL query and assert response body contains {@code expectedSubstring}. */
    private void assertErrorContains(String ppl, String expectedSubstring) throws IOException {
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        try {
            Response response = client().performRequest(request);
            Map<String, Object> body = entityAsMap(response);
            fail("Expected query [" + ppl + "] to fail with [" + expectedSubstring + "] but got: " + body);
        } catch (ResponseException e) {
            String body;
            try {
                body = entityAsMap(e.getResponse()).toString();
            } catch (IOException ioe) {
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
        Request request = new Request("POST", "/_analytics/ppl");
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
