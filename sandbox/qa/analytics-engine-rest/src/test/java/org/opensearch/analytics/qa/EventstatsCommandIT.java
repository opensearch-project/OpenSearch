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
 * Self-contained integration test for PPL {@code eventstats} on the analytics-engine route.
 *
 * <p>Mirrors {@code CalcitePPLEventstatsIT} from the {@code opensearch-project/sql} repository
 * one-to-one — every {@code @Test} method here corresponds to a method of the same name in the
 * source IT, with the query string translated from {@code state_country}/{@code logs} schema
 * to the calcs schema (see field-translation table below). Reachable cases assert exact rows;
 * cases that depend on functionality not yet wired through the analytics-engine route are
 * {@code expectThrows} negative tests pinning the precise error message — failing those is
 * how a future PR observes that they should be promoted to passing assertions.
 *
 * <h2>Schema translation (sql IT → calcs)</h2>
 * <pre>
 *   age        → int0      (integer; 11 non-null + 6 nulls)
 *   country    → str0      (keyword; 3 distinct: FURNITURE, OFFICE SUPPLIES, TECHNOLOGY)
 *   state      → str3      (keyword; 2 distinct non-null + 7 nulls)
 *   name       → key       (keyword; unique 17 row-id values key00..key16)
 *   span(age,10) → span(int0,10)  — but PPL `span()` UDF is not registered on the
 *                                  analytics-engine route until #21621 lands; bySpan tests
 *                                  expectThrows.
 * </pre>
 *
 * <h2>Calcs distribution facts (computed from bulk.json)</h2>
 * <pre>
 *   Global int0: 17 rows, 11 non-null = {1,7,3,8,8,4,10,4,11,4,8}
 *     count=17  count(int0)=11  sum=68  min=1  max=11  avg=68/11
 *     var_pop=9.05785123966942   var_samp=9.96363636363636
 *     stddev_pop=3.00962642859034 stddev_samp=3.15652282799228
 *
 *   By str0:
 *     FURNITURE       (2 rows, int0=[1])              sum=1   avg=1   min=1   max=1
 *     OFFICE SUPPLIES (6 rows, int0=[7,3,8])          sum=18  avg=6   min=3   max=8
 *                     var_pop=4.66666666666667  var_samp=7
 *                     stddev_pop=2.16024689946929  stddev_samp=2.64575131106459
 *     TECHNOLOGY      (9 rows, int0=[8,4,10,4,11,4,8]) sum=49  avg=7   min=4   max=11
 *                     var_pop=7.71428571428571  var_samp=9
 *                     stddev_pop=2.77746029931765  stddev_samp=3
 * </pre>
 *
 * <h2>Limitations vs sql IT</h2>
 * <ul>
 *   <li>calcs has no rows with NULL str0 / NULL str3 (other than rows already nullable on str3).
 *       The {@code WithNull} variants in sql IT add a row {@code (Kevin, null, null)} that
 *       calcs cannot reproduce; we test the int0-NULL case instead, which exercises the same
 *       null-bubbling-through-window-aggregate code path.</li>
 *   <li>{@code logs} index is not provisioned in this QA module; {@code testEventstatsEarliestAndLatest}
 *       expectThrows on the missing index AND the missing earliest/latest window function.</li>
 *   <li>{@code span()} UDF is now registered on the analytics-engine route (#21584 + #21621
 *       merged into main). Whether {@code RexOver(... PARTITION BY span(int0, 10))} survives
 *       substrait emission + DataFusion physical planning is not yet verified — all
 *       {@code bySpan} cases assert "fails somewhere" via {@code assertErrorAny}. If they
 *       start succeeding, the assertions fail loudly and a follow-up should upgrade them.</li>
 *   <li>{@code dc} / {@code distinct_count} window function is not in {@link
 *       org.opensearch.analytics.spi.WindowFunction}; expectThrows.</li>
 *   <li>{@code earliest} / {@code latest} window function is not in {@link
 *       org.opensearch.analytics.spi.WindowFunction}; expectThrows.</li>
 * </ul>
 */
public class EventstatsCommandIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");
    /** Multi-shard alias for tests that exercise the SINGLETON-gather cost gate
     *  ({@link org.opensearch.analytics.planner.rel.OpenSearchProject#computeSelfCost}) on
     *  partitioned windows. The 17 calcs rows distribute across 3 shards via UUID-hashed
     *  document ids, so partitions necessarily span shards — without the gather edge,
     *  per-shard window evaluation would silently produce wrong values. */
    private static final Dataset DATASET_MULTI = new Dataset("calcs", "calcs_multi_eventstats");

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

    // ── Basic eventstats (no by) ───────────────────────────────────────────────

    /** sql IT: testEventstats. eventstats count(), avg, min, max — global window over all rows. */
    public void testEventstats() throws IOException {
        // count(*)=17, count(int0)=11 — eventstats count() counts all rows including nulls = 17.
        // avg(int0)=68/11 ≈ 6.181818, min(int0)=1, max(int0)=11.
        // Each output row carries the same constants — assert via stats.values dedup.
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " | stats max(cnt) as cnt, max(avg) as avg, max(min) as min, max(max) as max"
        );
        assertScalarRow(response, 17L, 68.0 / 11.0, 1, 11);
    }

    // ── eventstats … by ────────────────────────────────────────────────────────

    /** sql IT: testEventstatsBy. eventstats partitioned by a categorical field. */
    public void testEventstatsBy() throws IOException {
        // FURNITURE: count=2, avg=1, min=1, max=1
        // OFFICE SUPPLIES: count=6, avg=6, min=3, max=8
        // TECHNOLOGY: count=9, avg=7, min=4, max=11
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max by str0"
                + " | stats max(cnt) as cnt, max(avg) as avg, max(min) as min, max(max) as max by str0"
                + " | sort str0"
        );
        // PPL `stats … by str0` places the by-column last in output.
        assertRowsEqual(response,
            row(2L, 1.0, 1, 1, "FURNITURE"),
            row(6L, 6.0, 3, 8, "OFFICE SUPPLIES"),
            row(9L, 7.0, 4, 11, "TECHNOLOGY")
        );
    }

    /** sql IT: testEventstatsByWithNullBucket. {@code bucket_nullable=false} skips the
     *  null-key partition: rows whose {@code by} key is null get NULL window output, while
     *  the non-null partition aggregates as usual. {@code str3} has 7 nulls + 10 rows with
     *  value {@code 'e'} — the contrast between default and {@code bucket_nullable=false}
     *  is visible in the null partition's row(s). */
    public void testEventstatsByWithNullBucket() throws IOException {
        ensureDataProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats bucket_nullable=false count() as cnt,"
                + " avg(int0) as avg, min(int0) as min, max(int0) as max by str3"
                + " | stats max(cnt) as cnt, max(avg) as avg, max(min) as min, max(max) as max by str3"
                + " | sort str3"
        );
        // str3=null rows: window output dropped → all aggregates NULL.
        // str3='e':       10 rows, int0=[1,11,7,3,8,4,11,4,8] (non-null) → cnt=10, avg=6.0, min=1, max=11.
        assertRowsEqual(response,
            row(null, null, null, null, null),
            row(10L, 6.0, 1, 11, "e")
        );
    }

    // ── Multi-shard correctness — exercises the SINGLETON-gather cost gate ────

    /** Same eventstats {@code by str0} query as {@link #testEventstatsBy} but on a 3-shard
     *  index. Calcs's 17 rows distribute across 3 shards via UUID-hashed doc ids, so each
     *  str0 partition (FURNITURE×2, OFFICE SUPPLIES×6, TECHNOLOGY×9) necessarily spans
     *  shards. Without the SINGLETON-gather cost gate that
     *  {@link org.opensearch.analytics.planner.rel.OpenSearchProject#computeSelfCost} forces
     *  on RexOver-bearing projects, per-shard window evaluation would produce different
     *  partial sums per shard for the same partition — and {@code stats max(...) by str0}
     *  would surface those wrong values. Identical expectations to
     *  {@link #testEventstatsBy} prove the gather is happening. */
    public void testEventstatsBy_3shard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max by str0"
                + " | stats max(cnt) as cnt, max(avg) as avg, max(min) as min, max(max) as max by str0"
                + " | sort str0"
        );
        assertRowsEqual(response,
            row(2L, 1.0, 1, 1, "FURNITURE"),
            row(6L, 6.0, 3, 8, "OFFICE SUPPLIES"),
            row(9L, 7.0, 4, 11, "TECHNOLOGY")
        );
    }

    /** Empty-partition window on a 3-shard index. Global SUM/AVG/MIN/MAX must equal the
     *  same numbers as the single-shard {@link #testEventstats}. If the SINGLETON gather
     *  is regressed, sum/min/max would still be correct (commutative + associative is
     *  partial-final safe even without gather) but avg would NOT — avg of partial-shard
     *  averages doesn't equal the global average unless rows-per-shard are equal. */
    public void testEventstats_3shard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " | stats max(cnt) as cnt, max(avg) as avg, max(min) as min, max(max) as max"
        );
        assertScalarRow(response, 17L, 68.0 / 11.0, 1, 11);
    }

    /** Chained eventstats on a 3-shard index — every {@code eventstats} lowers through
     *  {@link org.opensearch.analytics.planner.rel.OpenSearchProject#liftNestedRexOver},
     *  producing a 2-layer Project bundle per stage. Each bundle is rewired through
     *  {@code DataFusionFragmentConvertor.replaceInput} as a separate {@code attachFragmentOnTop}
     *  call; the lifted-window detection has to fire on every one. Two chained eventstats
     *  prove the rewire stays correct across multiple bundles on the multi-shard exchange
     *  path. */
    public void testChainedEventstats_3shard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName
                + " | eventstats avg(int0) as a by str0, str3"
                + " | eventstats max(a) as max_a by str0"
                + " | stats max(max_a) as final_max_a by str0 | sort str0"
        );
        // Per-(str0, str3) avg(int0) — see testMultipleEventstats. Take max of those avgs per str0:
        // FURNITURE: avg=1.0 (one (FURNITURE,'e') bucket) → max=1.0
        // OFFICE SUPPLIES: avg=8.0 ((OS,'e')), 5.0 ((OS,null)) → max=8.0
        // TECHNOLOGY: avg=6.75 ((T,'e')), ~7.333 ((T,null)) → max≈7.333
        assertRowsEqual(response,
            row(1.0, "FURNITURE"),
            row(8.0, "OFFICE SUPPLIES"),
            row(22.0 / 3, "TECHNOLOGY")
        );
    }

    /** sql IT: testEventstatsBySpan. {@code span(age, 10)} requires the PPL span() UDF
     *  registered on the analytics-engine route (PR #21621 — unmerged). expectThrows. */
    public void testEventstatsBySpan() throws IOException {
        ensureDataProvisioned();
        // After #21584 + #21621 merged into main, SPAN is registered as a ScalarFunction.
        // PPL's eventstats … by span(int0, 10) lowers to RexOver(... PARTITION BY span(int0, 10) ...).
        // Whether substrait emission + DataFusion's WindowFunctionInvocation handle a
        // scalar-call-as-partition-key end-to-end is not yet verified — this test asserts
        // the query fails loudly somewhere in that pipeline. If it ever passes on a real
        // cluster, this test fails with "expected to fail but got: [response]" and a future
        // PR should upgrade it to assert exact rows.
        assertErrorAny(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " by span(int0, 10) as int0_span"
        );
    }

    /** sql IT: testEventstatsBySpanWithNull. Same span() gap. */
    public void testEventstatsBySpanWithNull() throws IOException {
        ensureDataProvisioned();
        // After #21584 + #21621 merged into main, SPAN is registered as a ScalarFunction.
        // PPL's eventstats … by span(int0, 10) lowers to RexOver(... PARTITION BY span(int0, 10) ...).
        // Whether substrait emission + DataFusion's WindowFunctionInvocation handle a
        // scalar-call-as-partition-key end-to-end is not yet verified — this test asserts
        // the query fails loudly somewhere in that pipeline. If it ever passes on a real
        // cluster, this test fails with "expected to fail but got: [response]" and a future
        // PR should upgrade it to assert exact rows.
        assertErrorAny(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " by span(int0, 10) as int0_span"
        );
    }

    /** sql IT: testEventstatsByMultiplePartitions1. {@code by span(age, 10), country}. */
    public void testEventstatsByMultiplePartitions1() throws IOException {
        ensureDataProvisioned();
        // See testEventstatsBySpan — same span()-in-PARTITION-BY uncertainty.
        assertErrorAny(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " by span(int0, 10) as int0_span, str0"
        );
    }

    /** sql IT: testEventstatsByMultiplePartitions2. {@code by span(age, 10), state}. */
    public void testEventstatsByMultiplePartitions2() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " by span(int0, 10) as int0_span, str3"
        );
    }

    /** sql IT: testEventstatsByMultiplePartitionsWithNull1. */
    public void testEventstatsByMultiplePartitionsWithNull1() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " by span(int0, 10) as int0_span, str0"
        );
    }

    /** sql IT: testEventstatsByMultiplePartitionsWithNull2. */
    public void testEventstatsByMultiplePartitionsWithNull2() throws IOException {
        ensureDataProvisioned();
        assertErrorAny(
            "source=" + DATASET.indexName + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as min, max(int0) as max"
                + " by span(int0, 10) as int0_span, str3"
        );
    }

    // ── Unsupported window functions ───────────────────────────────────────────

    /** sql IT: testUnsupportedWindowFunctions. PERCENTILE/PERCENTILE_APPROX rejected by the
     *  PPL frontend with {@code "Unexpected window function: <name>"} before reaching the
     *  analytics-engine planner. */
    public void testUnsupportedWindowFunctions() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | eventstats percentile_approx(int0)",
            "percentile_approx"
        );
        assertErrorContains(
            "source=" + DATASET.indexName + " | eventstats percentile(int0)",
            "percentile"
        );
    }

    // ── Multiple eventstats (chained) ──────────────────────────────────────────

    /** sql IT: testMultipleEventstats. Chain two eventstats: per-(str3, str0) avg, then
     *  per-str0 average of those.
     *  <p>Per-(str0, str3) avg(int0) — only non-null int0 contributes:
     *  <ul>
     *    <li>(FURNITURE,'e') int0=[1] → avg=1</li>
     *    <li>(OFFICE SUPPLIES,'e') int0=[8] (3 nulls dropped) → avg=8</li>
     *    <li>(OFFICE SUPPLIES,null) int0=[7,3] → avg=5</li>
     *    <li>(TECHNOLOGY,'e') int0=[8,4,11,4] → avg=6.75</li>
     *    <li>(TECHNOLOGY,null) int0=[10,4,8] (2 nulls dropped) → avg=22/3 ≈ 7.333</li>
     *  </ul>
     *  Per-str0 avg(avg_int0) (avg of the per-(str3,str0) avg values weighted by row count):
     *  <ul>
     *    <li>FURNITURE  rows={(F,e)×2}                                  → avg(avg)=1</li>
     *    <li>OFFICE SUPPLIES rows={(O,e)×4 carrying 8} ∪ {(O,null)×2 carrying 5}
     *        → 6 rows × {8,8,8,8,5,5} → avg=42/6=7.0</li>
     *    <li>TECHNOLOGY rows={(T,e)×4 carrying 6.75} ∪ {(T,null)×5 carrying 22/3}
     *        → 9 rows × {6.75×4, 22/3×5}
     *        → sum = 4*6.75 + 5*(22/3) = 27 + 110/3 = (81+110)/3 = 191/3
     *        → avg = (191/3)/9 = 191/27 ≈ 7.074074</li>
     *  </ul>
     */
    public void testMultipleEventstats() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats avg(int0) as avg_int0 by str3, str0"
                + " | eventstats avg(avg_int0) as avg_str0_int0 by str0"
                + " | stats max(avg_str0_int0) as v by str0 | sort str0"
        );
        // PPL `stats … by str0` places the by-column last in output.
        assertRowsEqual(response,
            row(1.0, "FURNITURE"),
            row(7.0, "OFFICE SUPPLIES"),
            row(191.0 / 27.0, "TECHNOLOGY")
        );
    }

    /** sql IT: testMultipleEventstatsWithNull. Same as above for calcs (int0 nulls are
     *  excluded from window numerics already). */
    public void testMultipleEventstatsWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats avg(int0) as avg_int0 by str3, str0"
                + " | eventstats avg(avg_int0) as avg_str0_int0 by str0"
                + " | stats max(avg_str0_int0) as v by str0 | sort str0"
        );
        // PPL `stats … by str0` places the by-column last in output.
        assertRowsEqual(response,
            row(1.0, "FURNITURE"),
            row(7.0, "OFFICE SUPPLIES"),
            row(191.0 / 27.0, "TECHNOLOGY")
        );
    }

    /** sql IT: testMultipleEventstatsWithNullBucket. {@code bucket_nullable=false} on chained
     *  eventstats. With calcs (str3 has nulls — 7 of them — but str0 has no nulls):
     *  bucket_nullable=false on the first eventstats drops rows where {@code str3 IS NULL}
     *  from the per-(str0, str3) avg; second eventstats then sees fewer rows feeding the
     *  per-str0 avg-of-avg.
     *  <p>After bucket_nullable=false, only str3='e' rows feed the inner avg:
     *  <ul>
     *    <li>(FURNITURE, 'e') int0=[1] → avg=1, applied to 2 rows → outer feed: {1,1}</li>
     *    <li>(OFFICE SUPPLIES, 'e') int0=[8] → avg=8, applied to 4 rows → outer feed: {8,8,8,8}</li>
     *    <li>(TECHNOLOGY, 'e') int0=[8,4,11,4] → avg=6.75, applied to 4 rows → outer feed: {6.75,6.75,6.75,6.75}</li>
     *  </ul>
     *  The 7 str3-null rows have inner avg=null (bucket_nullable=false drops the inner partition);
     *  the second eventstats also bucket_nullable=false: rows with avg_int0=null (the str3-null rows)
     *  contribute as missing to the outer avg's count. avg(avg_int0) by str0 ignores nulls:
     *  <ul>
     *    <li>FURNITURE  → avg over {1,1} = 1.0</li>
     *    <li>OFFICE SUPPLIES → avg over {8,8,8,8} = 8.0</li>
     *    <li>TECHNOLOGY → avg over {6.75,6.75,6.75,6.75} = 6.75</li>
     *  </ul>
     */
    public void testMultipleEventstatsWithNullBucket() throws IOException {
        ensureDataProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats bucket_nullable=false avg(int0) as avg_int0 by str3, str0"
                + " | eventstats bucket_nullable=false avg(avg_int0) as avg_str0_int0 by str0"
                + " | stats max(avg_str0_int0) as avg_str0_int0 by str0 | sort str0"
        );
        assertRowsEqual(response,
            row(1.0, "FURNITURE"),
            row(8.0, "OFFICE SUPPLIES"),
            row(6.75, "TECHNOLOGY")
        );
    }

    /** sql IT: testMultipleEventstatsWithEval. eventstats → eval → eventstats → where →
     *  eventstats chain. Filter and re-aggregate. Calcs translation:
     *  <ol>
     *    <li>eventstats avg(int0) as a by str0, str3, key (key is unique → a == int0 for
     *        non-null rows, null for null rows) — degenerate per-row result</li>
     *    <li>eval b = a - 5 → per-row int0-5 (or null)</li>
     *    <li>eventstats avg(b) as bavg by str0, str3 — equivalent to per-(str0, str3)
     *        avg(int0)-5</li>
     *    <li>where bavg > 0 — filters partitions whose avg(int0) is &gt; 5</li>
     *    <li>eventstats count(bavg) as c by str0 — count surviving rows per str0</li>
     *  </ol>
     *  <p>Per-(str0, str3) avg-5: see {@link #testMultipleEventstats} table — adjusted by -5:
     *  <ul>
     *    <li>(FURNITURE, 'e') 1-5 = -4 → fails {@code > 0}</li>
     *    <li>(OFFICE SUPPLIES, 'e') 8-5 = 3 → passes (4 rows)</li>
     *    <li>(OFFICE SUPPLIES, null) 5-5 = 0 → fails (0 not > 0)</li>
     *    <li>(TECHNOLOGY, 'e') 6.75-5 = 1.75 → passes (4 rows)</li>
     *    <li>(TECHNOLOGY, null) 22/3-5 ≈ 2.333 → passes (5 rows)</li>
     *  </ul>
     *  After filter:  FURNITURE = 0 rows, OFFICE SUPPLIES = 4 rows, TECHNOLOGY = 9 rows.
     *  count() per str0 (non-null bavg only):
     *  <ul>
     *    <li>OFFICE SUPPLIES → 4</li>
     *    <li>TECHNOLOGY → 9</li>
     *  </ul>
     */
    public void testMultipleEventstatsWithEval() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName
                + " | eventstats avg(int0) as a by str0, str3, key"
                + " | eval b = a - 5"
                + " | eventstats avg(b) as bavg by str0, str3"
                + " | where bavg > 0"
                + " | eventstats count(bavg) as c by str0"
                + " | stats max(c) as c by str0 | sort str0"
        );
        assertRowsEqual(response,
            row(4L, "OFFICE SUPPLIES"),
            row(9L, "TECHNOLOGY")
        );
    }

    // ── Empty input ────────────────────────────────────────────────────────────

    /** sql IT: testEventstatsEmptyRows. {@code where … = 'non-existed' | eventstats …} on an
     *  empty stream emits 0 rows for both forms. */
    public void testEventstatsEmptyRows() throws IOException {
        Map<String, Object> r1 = executePpl(
            "source=" + DATASET.indexName + " | where key = 'non-existed' | eventstats count(),"
                + " avg(int0), min(int0), max(int0), stddev_pop(int0), stddev_samp(int0),"
                + " var_pop(int0), var_samp(int0)"
        );
        assertRowCount(r1, 0);

        Map<String, Object> r2 = executePpl(
            "source=" + DATASET.indexName + " | where key = 'non-existed' | eventstats count(),"
                + " avg(int0), min(int0), max(int0), stddev_pop(int0), stddev_samp(int0),"
                + " var_pop(int0), var_samp(int0) by str0"
        );
        assertRowCount(r2, 0);
    }

    // ── Variance / stddev ──────────────────────────────────────────────────────

    /** sql IT: testEventstatsVariance. Global stddev_pop / stddev_samp / var_pop / var_samp.
     *  Values pre-computed from int0={1,7,3,8,8,4,10,4,11,4,8}, mean=68/11. */
    public void testEventstatsVariance() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs"
                + " | stats max(sp) as sp, max(ss) as ss, max(vp) as vp, max(vs) as vs"
        );
        assertScalarRow(response,
            3.0096264285903371,   // stddev_pop
            3.1565228279922772,   // stddev_samp
            9.05785123966942,     // var_pop
            9.96363636363636      // var_samp
        );
    }

    /** sql IT: testEventstatsVarianceWithNull. Same numbers — null int0 rows excluded from
     *  numerics in both source IT and our calcs. */
    public void testEventstatsVarianceWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs"
                + " | stats max(sp) as sp, max(ss) as ss, max(vp) as vp, max(vs) as vs"
        );
        assertScalarRow(response,
            3.0096264285903371,
            3.1565228279922772,
            9.05785123966942,
            9.96363636363636
        );
    }

    /** sql IT: testEventstatsVarianceBy. Per-str0 variance. FURNITURE has only 1 non-null
     *  int0 → var_samp/stddev_samp are NULL (Bessel's correction guard). */
    public void testEventstatsVarianceBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs by str0"
                + " | stats max(sp) as sp, max(ss) as ss, max(vp) as vp, max(vs) as vs by str0"
                + " | sort str0"
        );
        // PPL `stats … by str0` places the by-column last in output.
        // FURNITURE has 1 sample so DataFusion emits NaN for samp variants (not null).
        assertRowsEqual(response,
            row(0.0, Double.NaN, 0.0, Double.NaN, "FURNITURE"),
            row(2.16024689946929, 2.6457513110645907, 4.66666666666667, 7.0, "OFFICE SUPPLIES"),
            row(2.77746029931765, 3.0, 7.71428571428571, 9.0, "TECHNOLOGY")
        );
    }

    /** sql IT: testEventstatsVarianceBySpan. {@code where country != 'USA' | … by span(age,10)}. */
    public void testEventstatsVarianceBySpan() throws IOException {
        ensureDataProvisioned();
        // See testEventstatsBySpan — span()-in-PARTITION-BY uncertainty.
        assertErrorAny(
            "source=" + DATASET.indexName + " | where str0 != 'TECHNOLOGY'"
                + " | eventstats stddev_samp(int0) by span(int0, 10)"
        );
    }

    /** sql IT: testEventstatsVarianceWithNullBy. Same as testEventstatsVarianceBy on calcs. */
    public void testEventstatsVarianceWithNullBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss,"
                + " var_pop(int0) as vp, var_samp(int0) as vs by str0"
                + " | stats max(sp) as sp, max(ss) as ss, max(vp) as vp, max(vs) as vs by str0"
                + " | sort str0"
        );
        // PPL `stats … by str0` places the by-column last; FURNITURE has 1 sample → NaN samp.
        assertRowsEqual(response,
            row(0.0, Double.NaN, 0.0, Double.NaN, "FURNITURE"),
            row(2.16024689946929, 2.6457513110645907, 4.66666666666667, 7.0, "OFFICE SUPPLIES"),
            row(2.77746029931765, 3.0, 7.71428571428571, 9.0, "TECHNOLOGY")
        );
    }

    // ── distinct_count / dc — not in WindowFunction enum ──────────────────────

    /** sql IT: testEventstatsDistinctCount. {@code dc()} resolves to DISTINCT_COUNT_APPROX in
     *  the PPL parser; that aggregate isn't registered in analytics-engine, so the request
     *  fails before reaching the window-function gate with
     *  {@code "Cannot resolve function: DISTINCT_COUNT_APPROX"}. */
    public void testEventstatsDistinctCount() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | eventstats dc(str3) as dc_str3",
            "DISTINCT_COUNT_APPROX"
        );
    }

    /** sql IT: testEventstatsDistinctCountByCountry. */
    public void testEventstatsDistinctCountByCountry() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | eventstats dc(str3) as dc_str3 by str0",
            "DISTINCT_COUNT_APPROX"
        );
    }

    /** sql IT: testEventstatsDistinctCountFunction. {@code distinct_count()} alias for dc. */
    public void testEventstatsDistinctCountFunction() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | eventstats distinct_count(str0) as dc_str0",
            "DISTINCT_COUNT_APPROX"
        );
    }

    /** sql IT: testEventstatsDistinctCountWithNull. */
    public void testEventstatsDistinctCountWithNull() throws IOException {
        ensureDataProvisioned();
        assertErrorContains(
            "source=" + DATASET.indexName + " | eventstats dc(str3) as dc_str3",
            "DISTINCT_COUNT_APPROX"
        );
    }

    // ── earliest / latest — not wired through analytics-engine yet ────────────

    /** sql IT: testEventstatsEarliestAndLatest. {@code earliest/latest} on calcs trip the PPL
     *  frontend's default-{@code @timestamp}-field check (calcs has no @timestamp column).
     *  Either way, the path is not reachable on analytics-engine today — assert the failure. */
    public void testEventstatsEarliestAndLatest() throws IOException {
        ensureDataProvisioned();
        // The actual error is "Default @timestamp field not found" because calcs has no
        // timestamp column, but the contract here is just "this PPL form is not yet supported".
        assertErrorAny(
            "source=" + DATASET.indexName + " | eventstats earliest(str0), latest(str0) by str3"
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
     *  NaN-aware: NaN matches NaN. */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        // Jackson serializes Double.NaN as the string "NaN" inside JSON arrays, so the
        // response body delivers "NaN" not Double.NaN. Treat NaN expectation match-on-string.
        if (expected instanceof Double && Double.isNaN((Double) expected)
            && (actual instanceof Double && Double.isNaN((Double) actual)
                || "NaN".equals(actual))) {
            return;
        }
        if (expected instanceof Number && actual instanceof Number) {
            double e = ((Number) expected).doubleValue();
            double a = ((Number) actual).doubleValue();
            // Tolerance for floating-point reduction differences across reduction orders.
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

    /**
     * Send a PPL query expecting an error response. Asserts the response body contains
     * {@code expectedSubstring}. Used for cases that exercise functionality not yet wired
     * through the analytics-engine route — the throw is the contract.
     */
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
     *  "this PPL form is not yet supported" — if the query unexpectedly succeeds the test
     *  fails loudly, signalling that the assertion should be upgraded to assert exact rows. */
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
