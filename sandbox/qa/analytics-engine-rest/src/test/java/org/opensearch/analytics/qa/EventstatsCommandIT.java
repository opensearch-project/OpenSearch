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
 *   <li>{@code span()} as a PARTITION BY key works after BackendPlanAdapter is taught to
 *       recurse into RexOver.window.partitionKeys, so the SpanAdapter rewrites SPAN(field,
 *       n, NULL) → FLOOR(field/n)*n before substrait emission. The {@code bySpan} cases
 *       collapse via {@code | stats max(...) by int0} and assert exact per-bucket rows.</li>
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

    // ── Basic eventstats (no by) ───────────────────────────────────────────────

    /** sql IT: testEventstats. eventstats count(), avg, min, max — global window over all rows.
     *  count(*)=17, count(int0)=11 — eventstats count() counts every row including int0 nulls.
     *  Each output row carries the same partition-constant (cnt, avg, mn, mx) broadcast from
     *  the single global partition. */
    public void testEventstats() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx"
                + " | fields key, int0, cnt, avg, mn, mx"
        );
        double avg = 68.0 / 11.0;
        assertRowsEqual(response,
            row("key00", 1,    17L, avg, 1, 11),
            row("key01", null, 17L, avg, 1, 11),
            row("key02", null, 17L, avg, 1, 11),
            row("key03", null, 17L, avg, 1, 11),
            row("key04", 7,    17L, avg, 1, 11),
            row("key05", 3,    17L, avg, 1, 11),
            row("key06", 8,    17L, avg, 1, 11),
            row("key07", null, 17L, avg, 1, 11),
            row("key08", null, 17L, avg, 1, 11),
            row("key09", 8,    17L, avg, 1, 11),
            row("key10", 4,    17L, avg, 1, 11),
            row("key11", 10,   17L, avg, 1, 11),
            row("key12", null, 17L, avg, 1, 11),
            row("key13", 4,    17L, avg, 1, 11),
            row("key14", 11,   17L, avg, 1, 11),
            row("key15", 4,    17L, avg, 1, 11),
            row("key16", 8,    17L, avg, 1, 11)
        );
    }

    // ── eventstats … by ────────────────────────────────────────────────────────

    /** sql IT: testEventstatsBy. eventstats partitioned by str0. Per-partition int0 aggregates
     *  broadcast to every row in that partition:
     *  FURNITURE 2 rows (cnt=2, avg=1, min/max=1); OFFICE SUPPLIES 6 rows (cnt=6, avg=6, min=3, max=8);
     *  TECHNOLOGY 9 rows (cnt=9, avg=7, min=4, max=11). */
    public void testEventstatsBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by str0"
                + " | fields key, str0, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    2L, 1.0, 1, 1),
            row("key01", "FURNITURE",       null, 2L, 1.0, 1, 1),
            row("key02", "OFFICE SUPPLIES", null, 6L, 6.0, 3, 8),
            row("key03", "OFFICE SUPPLIES", null, 6L, 6.0, 3, 8),
            row("key04", "OFFICE SUPPLIES", 7,    6L, 6.0, 3, 8),
            row("key05", "OFFICE SUPPLIES", 3,    6L, 6.0, 3, 8),
            row("key06", "OFFICE SUPPLIES", 8,    6L, 6.0, 3, 8),
            row("key07", "OFFICE SUPPLIES", null, 6L, 6.0, 3, 8),
            row("key08", "TECHNOLOGY",      null, 9L, 7.0, 4, 11),
            row("key09", "TECHNOLOGY",      8,    9L, 7.0, 4, 11),
            row("key10", "TECHNOLOGY",      4,    9L, 7.0, 4, 11),
            row("key11", "TECHNOLOGY",      10,   9L, 7.0, 4, 11),
            row("key12", "TECHNOLOGY",      null, 9L, 7.0, 4, 11),
            row("key13", "TECHNOLOGY",      4,    9L, 7.0, 4, 11),
            row("key14", "TECHNOLOGY",      11,   9L, 7.0, 4, 11),
            row("key15", "TECHNOLOGY",      4,    9L, 7.0, 4, 11),
            row("key16", "TECHNOLOGY",      8,    9L, 7.0, 4, 11)
        );
    }

    /** sql IT: testEventstatsByWithNull. Default {@code bucket_nullable=true} mode — null-key
     *  rows form their own partition and aggregate normally over its non-null int0 values.
     *  {@code str3} has 7 nulls (int0 non-null=[7,3,10,4,8] → cnt=7, avg=6.4, min=3, max=10)
     *  and 10 'e' rows (int0 non-null=[1,8,8,4,11,4] → cnt=10, avg=6.0, min=1, max=11). */
    public void testEventstatsByWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by str3"
                + " | fields key, str3, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    10L, 6.0, 1, 11),
            row("key01", "e",  null, 10L, 6.0, 1, 11),
            row("key02", "e",  null, 10L, 6.0, 1, 11),
            row("key03", "e",  null, 10L, 6.0, 1, 11),
            row("key04", null, 7,    7L,  6.4, 3, 10),
            row("key05", null, 3,    7L,  6.4, 3, 10),
            row("key06", "e",  8,    10L, 6.0, 1, 11),
            row("key07", "e",  null, 10L, 6.0, 1, 11),
            row("key08", null, null, 7L,  6.4, 3, 10),
            row("key09", "e",  8,    10L, 6.0, 1, 11),
            row("key10", "e",  4,    10L, 6.0, 1, 11),
            row("key11", null, 10,   7L,  6.4, 3, 10),
            row("key12", null, null, 7L,  6.4, 3, 10),
            row("key13", null, 4,    7L,  6.4, 3, 10),
            row("key14", "e",  11,   10L, 6.0, 1, 11),
            row("key15", "e",  4,    10L, 6.0, 1, 11),
            row("key16", null, 8,    7L,  6.4, 3, 10)
        );
    }

    /** sql IT: testEventstatsByWithNullBucket. {@code bucket_nullable=false} skips the
     *  null-key partition: rows whose {@code by} key is null get NULL window output, while
     *  the non-null partition aggregates as usual. Same {@code by str3} as
     *  {@link #testEventstatsByWithNull} — null-key rows now show NULL aggregates instead of
     *  the (cnt=7, avg=6.4, min=3, max=10) the default mode would broadcast. */
    public void testEventstatsByWithNullBucket() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats bucket_nullable=false count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by str3"
                + " | fields key, str3, int0, cnt, avg, mn, mx"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    10L,  6.0,  1,    11),
            row("key01", "e",  null, 10L,  6.0,  1,    11),
            row("key02", "e",  null, 10L,  6.0,  1,    11),
            row("key03", "e",  null, 10L,  6.0,  1,    11),
            row("key04", null, 7,    null, null, null, null),
            row("key05", null, 3,    null, null, null, null),
            row("key06", "e",  8,    10L,  6.0,  1,    11),
            row("key07", "e",  null, 10L,  6.0,  1,    11),
            row("key08", null, null, null, null, null, null),
            row("key09", "e",  8,    10L,  6.0,  1,    11),
            row("key10", "e",  4,    10L,  6.0,  1,    11),
            row("key11", null, 10,   null, null, null, null),
            row("key12", null, null, null, null, null, null),
            row("key13", null, 4,    null, null, null, null),
            row("key14", "e",  11,   10L,  6.0,  1,    11),
            row("key15", "e",  4,    10L,  6.0,  1,    11),
            row("key16", null, 8,    null, null, null, null)
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
            "source=" + DATASET_MULTI.indexName + " | sort key"
                + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by str0"
                + " | fields key, str0, int0, cnt, avg, mn, mx"
        );
        // Same per-(str0) aggregates as testEventstatsBy on single-shard.
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    2L, 1.0, 1, 1),
            row("key01", "FURNITURE",       null, 2L, 1.0, 1, 1),
            row("key02", "OFFICE SUPPLIES", null, 6L, 6.0, 3, 8),
            row("key03", "OFFICE SUPPLIES", null, 6L, 6.0, 3, 8),
            row("key04", "OFFICE SUPPLIES", 7,    6L, 6.0, 3, 8),
            row("key05", "OFFICE SUPPLIES", 3,    6L, 6.0, 3, 8),
            row("key06", "OFFICE SUPPLIES", 8,    6L, 6.0, 3, 8),
            row("key07", "OFFICE SUPPLIES", null, 6L, 6.0, 3, 8),
            row("key08", "TECHNOLOGY",      null, 9L, 7.0, 4, 11),
            row("key09", "TECHNOLOGY",      8,    9L, 7.0, 4, 11),
            row("key10", "TECHNOLOGY",      4,    9L, 7.0, 4, 11),
            row("key11", "TECHNOLOGY",      10,   9L, 7.0, 4, 11),
            row("key12", "TECHNOLOGY",      null, 9L, 7.0, 4, 11),
            row("key13", "TECHNOLOGY",      4,    9L, 7.0, 4, 11),
            row("key14", "TECHNOLOGY",      11,   9L, 7.0, 4, 11),
            row("key15", "TECHNOLOGY",      4,    9L, 7.0, 4, 11),
            row("key16", "TECHNOLOGY",      8,    9L, 7.0, 4, 11)
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
            "source=" + DATASET_MULTI.indexName + " | sort key"
                + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx"
                + " | fields key, int0, cnt, avg, mn, mx"
        );
        double avg = 68.0 / 11.0;
        assertRowsEqual(response,
            row("key00", 1,    17L, avg, 1, 11),
            row("key01", null, 17L, avg, 1, 11),
            row("key02", null, 17L, avg, 1, 11),
            row("key03", null, 17L, avg, 1, 11),
            row("key04", 7,    17L, avg, 1, 11),
            row("key05", 3,    17L, avg, 1, 11),
            row("key06", 8,    17L, avg, 1, 11),
            row("key07", null, 17L, avg, 1, 11),
            row("key08", null, 17L, avg, 1, 11),
            row("key09", 8,    17L, avg, 1, 11),
            row("key10", 4,    17L, avg, 1, 11),
            row("key11", 10,   17L, avg, 1, 11),
            row("key12", null, 17L, avg, 1, 11),
            row("key13", 4,    17L, avg, 1, 11),
            row("key14", 11,   17L, avg, 1, 11),
            row("key15", 4,    17L, avg, 1, 11),
            row("key16", 8,    17L, avg, 1, 11)
        );
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
            "source=" + DATASET_MULTI.indexName + " | sort key"
                + " | eventstats avg(int0) as a by str0, str3"
                + " | eventstats max(a) as max_a by str0"
                + " | fields key, str0, str3, int0, a, max_a"
        );
        // Per-(str0, str3) avg(int0): see testMultipleEventstats. Per-str0 max of those:
        //   FURNITURE: max(1.0) = 1.0
        //   OFFICE SUPPLIES: max(8.0, 5.0) = 8.0
        //   TECHNOLOGY: max(6.75, 22/3 ≈ 7.333) = 22/3
        double techNullAvg = 22.0 / 3.0;
        assertRowsEqual(response,
            row("key00", "FURNITURE",       "e",  1,    1.0,         1.0),
            row("key01", "FURNITURE",       "e",  null, 1.0,         1.0),
            row("key02", "OFFICE SUPPLIES", "e",  null, 8.0,         8.0),
            row("key03", "OFFICE SUPPLIES", "e",  null, 8.0,         8.0),
            row("key04", "OFFICE SUPPLIES", null, 7,    5.0,         8.0),
            row("key05", "OFFICE SUPPLIES", null, 3,    5.0,         8.0),
            row("key06", "OFFICE SUPPLIES", "e",  8,    8.0,         8.0),
            row("key07", "OFFICE SUPPLIES", "e",  null, 8.0,         8.0),
            row("key08", "TECHNOLOGY",      null, null, techNullAvg, techNullAvg),
            row("key09", "TECHNOLOGY",      "e",  8,    6.75,        techNullAvg),
            row("key10", "TECHNOLOGY",      "e",  4,    6.75,        techNullAvg),
            row("key11", "TECHNOLOGY",      null, 10,   techNullAvg, techNullAvg),
            row("key12", "TECHNOLOGY",      null, null, techNullAvg, techNullAvg),
            row("key13", "TECHNOLOGY",      null, 4,    techNullAvg, techNullAvg),
            row("key14", "TECHNOLOGY",      "e",  11,   6.75,        techNullAvg),
            row("key15", "TECHNOLOGY",      "e",  4,    6.75,        techNullAvg),
            row("key16", "TECHNOLOGY",      null, 8,    techNullAvg, techNullAvg)
        );
    }

    /** sql IT: testEventstatsBySpan. Bucketing key has no nulls — uses {@code int3}, which is
     *  fully populated in calcs. {@code span(int3, 10)} produces two buckets:
     *  bucket 0 (int3 ∈ [0,10), 9 rows) and bucket 10 (int3 ∈ [10,20), 8 rows).
     *  Per-bucket int0 aggregates ignore int0 nulls.
     *  Pairs with {@link #testEventstatsBySpanWithNull} which reintroduces the null bucket
     *  via {@code span(int0, 10)}. */
    public void testEventstatsBySpan() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by span(int3, 10)"
                + " | fields key, int3, int0, cnt, avg, mn, mx"
        );
        // bucket 0: 9 rows, int0 non-null=[1,7,3,8,10,8] → cnt=9, avg=37/6, min=1, max=10
        // bucket 10: 8 rows, int0 non-null=[8,4,4,11,4] → cnt=8, avg=31/5, min=4, max=11
        double avg0 = 37.0 / 6;
        double avg10 = 31.0 / 5;
        assertRowsEqual(response,
            row("key00", 8,  1,    9L, avg0,  1, 10),
            row("key01", 13, null, 8L, avg10, 4, 11),
            row("key02", 2,  null, 9L, avg0,  1, 10),
            row("key03", 5,  null, 9L, avg0,  1, 10),
            row("key04", 9,  7,    9L, avg0,  1, 10),
            row("key05", 7,  3,    9L, avg0,  1, 10),
            row("key06", 18, 8,    8L, avg10, 4, 11),
            row("key07", 3,  null, 9L, avg0,  1, 10),
            row("key08", 17, null, 8L, avg10, 4, 11),
            row("key09", 2,  8,    9L, avg0,  1, 10),
            row("key10", 11, 4,    8L, avg10, 4, 11),
            row("key11", 2,  10,   9L, avg0,  1, 10),
            row("key12", 11, null, 8L, avg10, 4, 11),
            row("key13", 18, 4,    8L, avg10, 4, 11),
            row("key14", 18, 11,   8L, avg10, 4, 11),
            row("key15", 11, 4,    8L, avg10, 4, 11),
            row("key16", 0,  8,    9L, avg0,  1, 10)
        );
    }

    /** sql IT: testEventstatsBySpanWithNull. Bucketing key has nulls — uses {@code int0}
     *  (6 nulls), so {@code span(int0, 10)} produces a null bucket alongside bucket 0
     *  (int0 ∈ [0,10), 9 rows) and bucket 10 (int0 ∈ [10,20), 2 rows). The null bucket
     *  carries 6 null-key rows whose int0 is null → all aggregates are NULL. */
    public void testEventstatsBySpanWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt, avg(int0) as avg, min(int0) as mn, max(int0) as mx by span(int0, 10)"
                + " | fields key, int0, cnt, avg, mn, mx"
        );
        // bucket 0 (9 rows int0∈[1,8]): avg=47/9; bucket 10 (2 rows {10,11}): avg=10.5;
        // null bucket (6 rows int0=null): all aggregates NULL.
        double avg0 = 47.0 / 9;
        assertRowsEqual(response,
            row("key00", 1,    9L, avg0, 1,    8),
            row("key01", null, 6L, null, null, null),
            row("key02", null, 6L, null, null, null),
            row("key03", null, 6L, null, null, null),
            row("key04", 7,    9L, avg0, 1,    8),
            row("key05", 3,    9L, avg0, 1,    8),
            row("key06", 8,    9L, avg0, 1,    8),
            row("key07", null, 6L, null, null, null),
            row("key08", null, 6L, null, null, null),
            row("key09", 8,    9L, avg0, 1,    8),
            row("key10", 4,    9L, avg0, 1,    8),
            row("key11", 10,   2L, 10.5, 10,   11),
            row("key12", null, 6L, null, null, null),
            row("key13", 4,    9L, avg0, 1,    8),
            row("key14", 11,   2L, 10.5, 10,   11),
            row("key15", 4,    9L, avg0, 1,    8),
            row("key16", 8,    9L, avg0, 1,    8)
        );
    }

    /** sql IT: testEventstatsByMultiplePartitions1. {@code by span(int0, 10), str0} — splits
     *  span buckets further by str0. */
    public void testEventstatsByMultiplePartitions1() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt by span(int0, 10), str0"
                + " | fields key, str0, int0, cnt"
        );
        // (str0, span(int0,10)) buckets — cnt is the bucket size:
        // (FURNITURE,null)=1, (FURNITURE,0)=1; (OS,null)=3, (OS,0)=3; (TECH,null)=2, (TECH,0)=5, (TECH,10)=2.
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    1L),
            row("key01", "FURNITURE",       null, 1L),
            row("key02", "OFFICE SUPPLIES", null, 3L),
            row("key03", "OFFICE SUPPLIES", null, 3L),
            row("key04", "OFFICE SUPPLIES", 7,    3L),
            row("key05", "OFFICE SUPPLIES", 3,    3L),
            row("key06", "OFFICE SUPPLIES", 8,    3L),
            row("key07", "OFFICE SUPPLIES", null, 3L),
            row("key08", "TECHNOLOGY",      null, 2L),
            row("key09", "TECHNOLOGY",      8,    5L),
            row("key10", "TECHNOLOGY",      4,    5L),
            row("key11", "TECHNOLOGY",      10,   2L),
            row("key12", "TECHNOLOGY",      null, 2L),
            row("key13", "TECHNOLOGY",      4,    5L),
            row("key14", "TECHNOLOGY",      11,   2L),
            row("key15", "TECHNOLOGY",      4,    5L),
            row("key16", "TECHNOLOGY",      8,    5L)
        );
    }

    /** sql IT: testEventstatsByMultiplePartitions2. {@code by span(int0, 10), str3} — splits
     *  span buckets further by str3 (which has 7 nulls + 10 'e's). */
    public void testEventstatsByMultiplePartitions2() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt by span(int0, 10), str3"
                + " | fields key, str3, int0, cnt"
        );
        // (str3, span(int0,10)) buckets: (e,0)=5, (e,null)=4, (e,10)=1;
        // (null,0)=4, (null,null)=2, (null,10)=1.
        assertRowsEqual(response,
            row("key00", "e",  1,    5L),
            row("key01", "e",  null, 4L),
            row("key02", "e",  null, 4L),
            row("key03", "e",  null, 4L),
            row("key04", null, 7,    4L),
            row("key05", null, 3,    4L),
            row("key06", "e",  8,    5L),
            row("key07", "e",  null, 4L),
            row("key08", null, null, 2L),
            row("key09", "e",  8,    5L),
            row("key10", "e",  4,    5L),
            row("key11", null, 10,   1L),
            row("key12", null, null, 2L),
            row("key13", null, 4,    4L),
            row("key14", "e",  11,   1L),
            row("key15", "e",  4,    5L),
            row("key16", null, 8,    4L)
        );
    }

    /** sql IT: testEventstatsByMultiplePartitionsWithNull1. Same query as
     *  {@link #testEventstatsByMultiplePartitions1}. */
    public void testEventstatsByMultiplePartitionsWithNull1() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt by span(int0, 10), str0"
                + " | fields key, str0, int0, cnt"
        );
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    1L),
            row("key01", "FURNITURE",       null, 1L),
            row("key02", "OFFICE SUPPLIES", null, 3L),
            row("key03", "OFFICE SUPPLIES", null, 3L),
            row("key04", "OFFICE SUPPLIES", 7,    3L),
            row("key05", "OFFICE SUPPLIES", 3,    3L),
            row("key06", "OFFICE SUPPLIES", 8,    3L),
            row("key07", "OFFICE SUPPLIES", null, 3L),
            row("key08", "TECHNOLOGY",      null, 2L),
            row("key09", "TECHNOLOGY",      8,    5L),
            row("key10", "TECHNOLOGY",      4,    5L),
            row("key11", "TECHNOLOGY",      10,   2L),
            row("key12", "TECHNOLOGY",      null, 2L),
            row("key13", "TECHNOLOGY",      4,    5L),
            row("key14", "TECHNOLOGY",      11,   2L),
            row("key15", "TECHNOLOGY",      4,    5L),
            row("key16", "TECHNOLOGY",      8,    5L)
        );
    }

    /** sql IT: testEventstatsByMultiplePartitionsWithNull2. Same query as
     *  {@link #testEventstatsByMultiplePartitions2}. */
    public void testEventstatsByMultiplePartitionsWithNull2() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats count() as cnt by span(int0, 10), str3"
                + " | fields key, str3, int0, cnt"
        );
        assertRowsEqual(response,
            row("key00", "e",  1,    5L),
            row("key01", "e",  null, 4L),
            row("key02", "e",  null, 4L),
            row("key03", "e",  null, 4L),
            row("key04", null, 7,    4L),
            row("key05", null, 3,    4L),
            row("key06", "e",  8,    5L),
            row("key07", "e",  null, 4L),
            row("key08", null, null, 2L),
            row("key09", "e",  8,    5L),
            row("key10", "e",  4,    5L),
            row("key11", null, 10,   1L),
            row("key12", null, null, 2L),
            row("key13", null, 4,    4L),
            row("key14", "e",  11,   1L),
            row("key15", "e",  4,    5L),
            row("key16", null, 8,    4L)
        );
    }

    // ── Unsupported window functions ───────────────────────────────────────────

    /** sql IT: testUnsupportedWindowFunctions. PERCENTILE/PERCENTILE_APPROX rejected by the
     *  PPL frontend with {@code "Unexpected window function: <name>"} before reaching the
     *  analytics-engine planner. */
    public void testUnsupportedWindowFunctions() throws IOException {
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
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats avg(int0) as avg_int0 by str3, str0"
                + " | eventstats avg(avg_int0) as avg_str0_int0 by str0"
                + " | fields key, str0, str3, int0, avg_int0, avg_str0_int0"
        );
        // Per-(str0, str3) avg(int0): F/'e'=1, OS/'e'=8, OS/null=5, T/'e'=6.75, T/null=22/3.
        // Per-str0 avg(avg_int0): F=1.0, OS=7.0, T=191/27.
        double tNull = 22.0 / 3.0;
        double osAvgOuter = 7.0;
        double techAvgOuter = 191.0 / 27.0;
        assertRowsEqual(response,
            row("key00", "FURNITURE",       "e",  1,    1.0,   1.0),
            row("key01", "FURNITURE",       "e",  null, 1.0,   1.0),
            row("key02", "OFFICE SUPPLIES", "e",  null, 8.0,   osAvgOuter),
            row("key03", "OFFICE SUPPLIES", "e",  null, 8.0,   osAvgOuter),
            row("key04", "OFFICE SUPPLIES", null, 7,    5.0,   osAvgOuter),
            row("key05", "OFFICE SUPPLIES", null, 3,    5.0,   osAvgOuter),
            row("key06", "OFFICE SUPPLIES", "e",  8,    8.0,   osAvgOuter),
            row("key07", "OFFICE SUPPLIES", "e",  null, 8.0,   osAvgOuter),
            row("key08", "TECHNOLOGY",      null, null, tNull, techAvgOuter),
            row("key09", "TECHNOLOGY",      "e",  8,    6.75,  techAvgOuter),
            row("key10", "TECHNOLOGY",      "e",  4,    6.75,  techAvgOuter),
            row("key11", "TECHNOLOGY",      null, 10,   tNull, techAvgOuter),
            row("key12", "TECHNOLOGY",      null, null, tNull, techAvgOuter),
            row("key13", "TECHNOLOGY",      null, 4,    tNull, techAvgOuter),
            row("key14", "TECHNOLOGY",      "e",  11,   6.75,  techAvgOuter),
            row("key15", "TECHNOLOGY",      "e",  4,    6.75,  techAvgOuter),
            row("key16", "TECHNOLOGY",      null, 8,    tNull, techAvgOuter)
        );
    }

    /** sql IT: testMultipleEventstatsWithNull. Same query as {@link #testMultipleEventstats} —
     *  calcs has int0 nulls already, so window numerics ignore them by default. */
    public void testMultipleEventstatsWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats avg(int0) as avg_int0 by str3, str0"
                + " | eventstats avg(avg_int0) as avg_str0_int0 by str0"
                + " | fields key, str0, str3, int0, avg_int0, avg_str0_int0"
        );
        double tNull = 22.0 / 3.0;
        double osAvgOuter = 7.0;
        double techAvgOuter = 191.0 / 27.0;
        assertRowsEqual(response,
            row("key00", "FURNITURE",       "e",  1,    1.0,   1.0),
            row("key01", "FURNITURE",       "e",  null, 1.0,   1.0),
            row("key02", "OFFICE SUPPLIES", "e",  null, 8.0,   osAvgOuter),
            row("key03", "OFFICE SUPPLIES", "e",  null, 8.0,   osAvgOuter),
            row("key04", "OFFICE SUPPLIES", null, 7,    5.0,   osAvgOuter),
            row("key05", "OFFICE SUPPLIES", null, 3,    5.0,   osAvgOuter),
            row("key06", "OFFICE SUPPLIES", "e",  8,    8.0,   osAvgOuter),
            row("key07", "OFFICE SUPPLIES", "e",  null, 8.0,   osAvgOuter),
            row("key08", "TECHNOLOGY",      null, null, tNull, techAvgOuter),
            row("key09", "TECHNOLOGY",      "e",  8,    6.75,  techAvgOuter),
            row("key10", "TECHNOLOGY",      "e",  4,    6.75,  techAvgOuter),
            row("key11", "TECHNOLOGY",      null, 10,   tNull, techAvgOuter),
            row("key12", "TECHNOLOGY",      null, null, tNull, techAvgOuter),
            row("key13", "TECHNOLOGY",      null, 4,    tNull, techAvgOuter),
            row("key14", "TECHNOLOGY",      "e",  11,   6.75,  techAvgOuter),
            row("key15", "TECHNOLOGY",      "e",  4,    6.75,  techAvgOuter),
            row("key16", "TECHNOLOGY",      null, 8,    tNull, techAvgOuter)
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
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats bucket_nullable=false avg(int0) as avg_int0 by str3, str0"
                + " | eventstats bucket_nullable=false avg(avg_int0) as avg_str0_int0 by str0"
                + " | fields key, str0, str3, int0, avg_int0, avg_str0_int0"
        );
        // bucket_nullable=false drops str3-null partition's inner avg → avg_int0=null on those rows.
        // Outer eventstats on str0 then averages only the non-null avg_int0 within each str0:
        //   FURNITURE (only str3='e'): avg=1.0
        //   OFFICE SUPPLIES (only str3='e' contributes): avg=8.0
        //   TECHNOLOGY (only str3='e' contributes): avg=6.75
        assertRowsEqual(response,
            row("key00", "FURNITURE",       "e",  1,    1.0,  1.0),
            row("key01", "FURNITURE",       "e",  null, 1.0,  1.0),
            row("key02", "OFFICE SUPPLIES", "e",  null, 8.0,  8.0),
            row("key03", "OFFICE SUPPLIES", "e",  null, 8.0,  8.0),
            row("key04", "OFFICE SUPPLIES", null, 7,    null, 8.0),
            row("key05", "OFFICE SUPPLIES", null, 3,    null, 8.0),
            row("key06", "OFFICE SUPPLIES", "e",  8,    8.0,  8.0),
            row("key07", "OFFICE SUPPLIES", "e",  null, 8.0,  8.0),
            row("key08", "TECHNOLOGY",      null, null, null, 6.75),
            row("key09", "TECHNOLOGY",      "e",  8,    6.75, 6.75),
            row("key10", "TECHNOLOGY",      "e",  4,    6.75, 6.75),
            row("key11", "TECHNOLOGY",      null, 10,   null, 6.75),
            row("key12", "TECHNOLOGY",      null, null, null, 6.75),
            row("key13", "TECHNOLOGY",      null, 4,    null, 6.75),
            row("key14", "TECHNOLOGY",      "e",  11,   6.75, 6.75),
            row("key15", "TECHNOLOGY",      "e",  4,    6.75, 6.75),
            row("key16", "TECHNOLOGY",      null, 8,    null, 6.75)
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
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats avg(int0) as a by str0, str3, key"
                + " | eval b = a - 5"
                + " | eventstats avg(b) as bavg by str0, str3"
                + " | where bavg > 0"
                + " | eventstats count(bavg) as c by str0"
                + " | fields key, str0, str3, int0, a, b, bavg, c"
        );
        // bavg = avg(int0) by (str0, str3) - 5; filter `bavg > 0` keeps only str0/str3 buckets
        // whose avg(int0)>5: (OS,'e')=8, (T,'e')=6.75, (T,null)=22/3. Each row carries the
        // count(bavg) grouped by str0: OS=4 (the 4 'e' rows in OS), T=9 (all 9 T rows).
        // FURNITURE entirely filtered out.
        double techNullBavg = 22.0 / 3.0 - 5;  // 7.333... - 5 = 2.333...
        assertRowsEqual(response,
            row("key02", "OFFICE SUPPLIES", "e",  null, null, null, 3.0,          4L),
            row("key03", "OFFICE SUPPLIES", "e",  null, null, null, 3.0,          4L),
            row("key06", "OFFICE SUPPLIES", "e",  8,    8.0,  3.0,  3.0,          4L),
            row("key07", "OFFICE SUPPLIES", "e",  null, null, null, 3.0,          4L),
            row("key08", "TECHNOLOGY",      null, null, null, null, techNullBavg, 9L),
            row("key09", "TECHNOLOGY",      "e",  8,    8.0,  3.0,  1.75,         9L),
            row("key10", "TECHNOLOGY",      "e",  4,    4.0,  -1.0, 1.75,         9L),
            row("key11", "TECHNOLOGY",      null, 10,   10.0, 5.0,  techNullBavg, 9L),
            row("key12", "TECHNOLOGY",      null, null, null, null, techNullBavg, 9L),
            row("key13", "TECHNOLOGY",      null, 4,    4.0,  -1.0, techNullBavg, 9L),
            row("key14", "TECHNOLOGY",      "e",  11,   11.0, 6.0,  1.75,         9L),
            row("key15", "TECHNOLOGY",      "e",  4,    4.0,  -1.0, 1.75,         9L),
            row("key16", "TECHNOLOGY",      null, 8,    8.0,  3.0,  techNullBavg, 9L)
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

    /** sql IT: testEventstatsVariance. Global stddev_pop / stddev_samp / var_pop / var_samp
     *  broadcast to every row from the single global partition. Values pre-computed from
     *  int0={1,7,3,8,8,4,10,4,11,4,8}, mean=68/11. */
    public void testEventstatsVariance() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs"
                + " | fields key, int0, sp, ss, vp, vs"
        );
        double sp = 3.009626428590336;
        double ss = 3.1565228279922772;
        double vp = 9.05785123966942;
        double vs = 9.963636363636363;
        assertRowsEqual(response,
            row("key00", 1,    sp, ss, vp, vs),
            row("key01", null, sp, ss, vp, vs),
            row("key02", null, sp, ss, vp, vs),
            row("key03", null, sp, ss, vp, vs),
            row("key04", 7,    sp, ss, vp, vs),
            row("key05", 3,    sp, ss, vp, vs),
            row("key06", 8,    sp, ss, vp, vs),
            row("key07", null, sp, ss, vp, vs),
            row("key08", null, sp, ss, vp, vs),
            row("key09", 8,    sp, ss, vp, vs),
            row("key10", 4,    sp, ss, vp, vs),
            row("key11", 10,   sp, ss, vp, vs),
            row("key12", null, sp, ss, vp, vs),
            row("key13", 4,    sp, ss, vp, vs),
            row("key14", 11,   sp, ss, vp, vs),
            row("key15", 4,    sp, ss, vp, vs),
            row("key16", 8,    sp, ss, vp, vs)
        );
    }

    /** sql IT: testEventstatsVarianceWithNull. Same query and result as
     *  {@link #testEventstatsVariance} — null int0 rows are already excluded from numerics. */
    public void testEventstatsVarianceWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs"
                + " | fields key, int0, sp, ss, vp, vs"
        );
        double sp = 3.009626428590336;
        double ss = 3.1565228279922772;
        double vp = 9.05785123966942;
        double vs = 9.963636363636363;
        assertRowsEqual(response,
            row("key00", 1,    sp, ss, vp, vs),
            row("key01", null, sp, ss, vp, vs),
            row("key02", null, sp, ss, vp, vs),
            row("key03", null, sp, ss, vp, vs),
            row("key04", 7,    sp, ss, vp, vs),
            row("key05", 3,    sp, ss, vp, vs),
            row("key06", 8,    sp, ss, vp, vs),
            row("key07", null, sp, ss, vp, vs),
            row("key08", null, sp, ss, vp, vs),
            row("key09", 8,    sp, ss, vp, vs),
            row("key10", 4,    sp, ss, vp, vs),
            row("key11", 10,   sp, ss, vp, vs),
            row("key12", null, sp, ss, vp, vs),
            row("key13", 4,    sp, ss, vp, vs),
            row("key14", 11,   sp, ss, vp, vs),
            row("key15", 4,    sp, ss, vp, vs),
            row("key16", 8,    sp, ss, vp, vs)
        );
    }

    /** sql IT: testEventstatsVarianceBy. Per-str0 variance broadcast to every row.
     *  FURNITURE has only 1 non-null int0 (key00=1, key01=null) → var_samp/stddev_samp emit
     *  NaN (DataFusion's behavior under Bessel's correction with n=1). */
    public void testEventstatsVarianceBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs by str0"
                + " | fields key, str0, int0, sp, ss, vp, vs"
        );
        double osSp = 2.160246899469287, osSs = 2.6457513110645907, osVp = 4.666666666666667, osVs = 7.0;
        double tSp  = 2.7774602993176543, tSs = 3.0, tVp = 7.714285714285714, tVs = 9.0;
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    0.0,  (Double) null, 0.0,  (Double) null),
            row("key01", "FURNITURE",       null, 0.0,  (Double) null, 0.0,  (Double) null),
            row("key02", "OFFICE SUPPLIES", null, osSp, osSs,       osVp, osVs),
            row("key03", "OFFICE SUPPLIES", null, osSp, osSs,       osVp, osVs),
            row("key04", "OFFICE SUPPLIES", 7,    osSp, osSs,       osVp, osVs),
            row("key05", "OFFICE SUPPLIES", 3,    osSp, osSs,       osVp, osVs),
            row("key06", "OFFICE SUPPLIES", 8,    osSp, osSs,       osVp, osVs),
            row("key07", "OFFICE SUPPLIES", null, osSp, osSs,       osVp, osVs),
            row("key08", "TECHNOLOGY",      null, tSp,  tSs,        tVp,  tVs),
            row("key09", "TECHNOLOGY",      8,    tSp,  tSs,        tVp,  tVs),
            row("key10", "TECHNOLOGY",      4,    tSp,  tSs,        tVp,  tVs),
            row("key11", "TECHNOLOGY",      10,   tSp,  tSs,        tVp,  tVs),
            row("key12", "TECHNOLOGY",      null, tSp,  tSs,        tVp,  tVs),
            row("key13", "TECHNOLOGY",      4,    tSp,  tSs,        tVp,  tVs),
            row("key14", "TECHNOLOGY",      11,   tSp,  tSs,        tVp,  tVs),
            row("key15", "TECHNOLOGY",      4,    tSp,  tSs,        tVp,  tVs),
            row("key16", "TECHNOLOGY",      8,    tSp,  tSs,        tVp,  tVs)
        );
    }

    /** sql IT: testEventstatsVarianceBySpan. After filtering out TECHNOLOGY, 8 rows remain
     *  (FURNITURE×2 + OFFICE SUPPLIES×6). All non-null int0 values are in span bucket 0
     *  ({1,7,3,8}): stddev_samp({1,7,3,8}) ≈ 3.304. The 4 null-int0 rows form the null-bucket
     *  → NULL stddev. */
    public void testEventstatsVarianceBySpan() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | where str0 != 'TECHNOLOGY' | sort key"
                + " | eventstats stddev_samp(int0) as ss by span(int0, 10)"
                + " | fields key, str0, int0, ss"
        );
        double ss = 3.304037933599835;
        assertRowsEqual(response,
            row("key00", "FURNITURE",       1,    ss),
            row("key01", "FURNITURE",       null, null),
            row("key02", "OFFICE SUPPLIES", null, null),
            row("key03", "OFFICE SUPPLIES", null, null),
            row("key04", "OFFICE SUPPLIES", 7,    ss),
            row("key05", "OFFICE SUPPLIES", 3,    ss),
            row("key06", "OFFICE SUPPLIES", 8,    ss),
            row("key07", "OFFICE SUPPLIES", null, null)
        );
    }

    /** sql IT: testEventstatsVarianceWithNullBy. Variance partitioned by {@code str3} which has
     *  7 nulls + 10 'e' rows — covers null-key partition handling. */
    public void testEventstatsVarianceWithNullBy() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key"
                + " | eventstats stddev_pop(int0) as sp, stddev_samp(int0) as ss, var_pop(int0) as vp, var_samp(int0) as vs by str3"
                + " | fields key, str3, int0, sp, ss, vp, vs"
        );
        // str3='e':  10 rows, int0 non-null=[1,8,8,4,11,4] → sp,ss,vp,vs
        // str3=null: 7 rows, int0 non-null=[7,3,10,4,8] → sp,ss,vp,vs
        double eSp = 3.3166247903554, eSs = 3.63318042491699, eVp = 11.0, eVs = 13.2;
        double nSp = 2.576819745345025, nSs = 2.880972058177586, nVp = 6.639999999999998, nVs = 8.299999999999997;
        assertRowsEqual(response,
            row("key00", "e",  1,    eSp, eSs, eVp, eVs),
            row("key01", "e",  null, eSp, eSs, eVp, eVs),
            row("key02", "e",  null, eSp, eSs, eVp, eVs),
            row("key03", "e",  null, eSp, eSs, eVp, eVs),
            row("key04", null, 7,    nSp, nSs, nVp, nVs),
            row("key05", null, 3,    nSp, nSs, nVp, nVs),
            row("key06", "e",  8,    eSp, eSs, eVp, eVs),
            row("key07", "e",  null, eSp, eSs, eVp, eVs),
            row("key08", null, null, nSp, nSs, nVp, nVs),
            row("key09", "e",  8,    eSp, eSs, eVp, eVs),
            row("key10", "e",  4,    eSp, eSs, eVp, eVs),
            row("key11", null, 10,   nSp, nSs, nVp, nVs),
            row("key12", null, null, nSp, nSs, nVp, nVs),
            row("key13", null, 4,    nSp, nSs, nVp, nVs),
            row("key14", "e",  11,   eSp, eSs, eVp, eVs),
            row("key15", "e",  4,    eSp, eSs, eVp, eVs),
            row("key16", null, 8,    nSp, nSs, nVp, nVs)
        );
    }

    // ── distinct_count / dc ────────────────────────────────────────────────────
    // BackendPlanAdapter rewrites RexOver(DISTINCT_COUNT_APPROX) → APPROX_COUNT_DISTINCT,
    // and the DataFusion plugin's approx_count_distinct wrapper UDAF aliases that name to
    // DataFusion's built-in approx_distinct (HyperLogLog). At calcs's scale (17 rows, low
    // cardinality) HLL is exact, so we can assert the exact dc count.
    //
    // calcs.str3 is "e" on every non-null row → dc(str3)=1. calcs.str0 has three values
    // {FURNITURE, OFFICE SUPPLIES, TECHNOLOGY} on every row → dc(str0)=3. eventstats
    // broadcasts the global aggregate to every row.

    /** sql IT: testEventstatsDistinctCount. */
    public void testEventstatsDistinctCount() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | eventstats dc(str3) as dc_str3 | fields key, dc_str3"
        );
        // Every row sees the same global dc(str3) = 1 (only "e" appears as a non-null value).
        assertRowsEqual(response,
            row("key00", 1L), row("key01", 1L), row("key02", 1L), row("key03", 1L),
            row("key04", 1L), row("key05", 1L), row("key06", 1L), row("key07", 1L),
            row("key08", 1L), row("key09", 1L), row("key10", 1L), row("key11", 1L),
            row("key12", 1L), row("key13", 1L), row("key14", 1L), row("key15", 1L),
            row("key16", 1L)
        );
    }

    /** sql IT: testEventstatsDistinctCountByCountry. */
    public void testEventstatsDistinctCountByCountry() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | eventstats dc(str3) as dc_str3 by str0 | fields key, str0, dc_str3"
        );
        // Per-partition dc(str3): FURNITURE has 2 rows of "e" (dc=1); OFFICE SUPPLIES has 4 of
        // "e" + 2 nulls (dc=1); TECHNOLOGY has 4 of "e" + 5 nulls (dc=1).
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
            row("key08", "TECHNOLOGY", 1L),
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

    /** sql IT: testEventstatsDistinctCountFunction. {@code distinct_count()} alias for dc. */
    public void testEventstatsDistinctCountFunction() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | eventstats distinct_count(str0) as dc_str0 | fields key, dc_str0"
        );
        // dc(str0) = 3 across all 17 rows (FURNITURE, OFFICE SUPPLIES, TECHNOLOGY).
        assertRowsEqual(response,
            row("key00", 3L), row("key01", 3L), row("key02", 3L), row("key03", 3L),
            row("key04", 3L), row("key05", 3L), row("key06", 3L), row("key07", 3L),
            row("key08", 3L), row("key09", 3L), row("key10", 3L), row("key11", 3L),
            row("key12", 3L), row("key13", 3L), row("key14", 3L), row("key15", 3L),
            row("key16", 3L)
        );
    }

    /** sql IT: testEventstatsDistinctCountWithNull. Same query as {@link #testEventstatsDistinctCount}
     *  — sql-plugin's variant uses STATE_COUNTRY_WITH_NULL to exercise null handling, but this QA
     *  module only ships the {@code calcs} dataset (whose {@code str3} already has 7 nulls). The
     *  aggregate semantics are identical. */
    public void testEventstatsDistinctCountWithNull() throws IOException {
        Map<String, Object> response = executePpl(
            "source=" + DATASET.indexName + " | sort key | eventstats dc(str3) as dc_str3 | fields key, dc_str3"
        );
        assertRowsEqual(response,
            row("key00", 1L), row("key01", 1L), row("key02", 1L), row("key03", 1L),
            row("key04", 1L), row("key05", 1L), row("key06", 1L), row("key07", 1L),
            row("key08", 1L), row("key09", 1L), row("key10", 1L), row("key11", 1L),
            row("key12", 1L), row("key13", 1L), row("key14", 1L), row("key15", 1L),
            row("key16", 1L)
        );
    }

    /** Multi-shard variant of {@link #testEventstatsDistinctCount} — exercises the
     *  PARTIAL/FINAL split + SINGLETON-gather path for distinct-count. dc is set-based:
     *  every shard contributes a partial HLL sketch, the coordinator merges them into a
     *  single global sketch, then broadcasts {@code dc(str3)=1} to every row. The same
     *  exact rows as single-shard are expected because HLL is exact at calcs's scale. */
    public void testEventstatsDistinctCount_3shard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName + " | sort key | eventstats dc(str3) as dc_str3 | fields key, dc_str3"
        );
        assertRowsEqual(response,
            row("key00", 1L), row("key01", 1L), row("key02", 1L), row("key03", 1L),
            row("key04", 1L), row("key05", 1L), row("key06", 1L), row("key07", 1L),
            row("key08", 1L), row("key09", 1L), row("key10", 1L), row("key11", 1L),
            row("key12", 1L), row("key13", 1L), row("key14", 1L), row("key15", 1L),
            row("key16", 1L)
        );
    }

    /** Multi-shard variant of {@link #testEventstatsDistinctCountByCountry} — partitioned
     *  dc with {@code by str0}. Per-partition HLL state is built per shard, merged at the
     *  coordinator, then broadcast. Each {@code str0} group sees the same {@code dc(str3)=1}
     *  as single-shard. */
    public void testEventstatsDistinctCountByCountry_3shard() throws IOException {
        ensureMultiShardProvisioned();
        Map<String, Object> response = executePpl(
            "source=" + DATASET_MULTI.indexName
                + " | sort key | eventstats dc(str3) as dc_str3 by str0 | fields key, str0, dc_str3"
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
            row("key08", "TECHNOLOGY", 1L),
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

    // ── earliest / latest — not wired through analytics-engine yet ────────────

    /** sql IT: testEventstatsEarliestAndLatest. {@code earliest/latest} on calcs trip the PPL
     *  frontend's default-{@code @timestamp}-field check (calcs has no @timestamp column).
     *  Either way, the path is not reachable on analytics-engine today — assert the failure. */
    public void testEventstatsEarliestAndLatest() throws IOException {
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
     *  NaN-aware: NaN matches NaN. */
    private static void assertCellEquals(String message, Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertEquals(message, expected, actual);
            return;
        }
        // Jackson serializes (Double) null as the string "NaN" inside JSON arrays, so the
        // response body delivers "NaN" not (Double) null. Treat NaN expectation match-on-string.
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


    /**
     * Send a PPL query expecting an error response. Asserts the response body contains
     * {@code expectedSubstring}. Used for cases that exercise functionality not yet wired
     * through the analytics-engine route — the throw is the contract.
     */
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
     *  "this PPL form is not yet supported" — if the query unexpectedly succeeds the test
     *  fails loudly, signalling that the assertion should be upgraded to assert exact rows. */
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
