/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.canmatch;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end tests for dispatch-gated sort early termination: for a bounded sort
 * ({@code sort <field> | head N}) the coordinator keeps a heap of the best {@code N} keys seen so far
 * and refuses to open a stream to a shard whose whole reported range is strictly worse than the worst
 * of them. Each test asserts both halves: the gated answer equals the wait-for-all answer (via
 * {@link #runAgainstWaitForAll}), and the expected number of shards was dispatched.
 *
 * <p>Fixture notes: the per-node window is pinned to 1 so elimination is deterministic — the veto only
 * acts on shards still queued, and at the default window of 5 the whole fan-out is on the wire first.
 * One single-shard parquet index per day, addressed as a PPL comma-list, because a {@code logs-*}
 * wildcard bypasses can-match. Queries project {@code | fields ts} so rows can't tie ambiguously;
 * {@link #testLateMaterializationPathMatchesBaseline} projects an extra column to take the QTF path.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(
    scope = OpenSearchIntegTestCase.Scope.SUITE,
    numDataNodes = 1,
    numClientNodes = 0,
    supportsDedicatedMasters = false
)
public class SortEarlyTerminationIT extends OpenSearchIntegTestCase {

    // A week of daily rollover, one shard each: 2026-07-08 .. 2026-07-14.
    private static final int TOTAL_DAYS = 7;
    private static final int FIRST_DAY = 8;
    private static final int DOCS_PER_DAY = 5;

    /** The baseline {@code head} — past every fixture's row count, so the gate can never arm. */
    private static final int BASELINE_LIMIT = 1000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            TestPPLPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            MockCommitterEnginePlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            // One in-flight shard request per node — see the class javadoc.
            .put(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.getKey(), 1)
            .build();
    }

    /**
     * A week of disjoint daily indices, {@code sort - ts | head 3}. The newest day is dispatched first
     * and fills the heap, so the six older days are skipped.
     */
    public void testDisjointDaysDescendingSkipsAllButTheNewestDay() throws Exception {
        String indices = createIndices("sort_et_desc_", dailyTimestamps());

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runAgainstWaitForAll("source = " + indices + " | sort - ts | head %d | fields ts", 3, dispatches);

            assertEquals("head 3 must return 3 rows", 3, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open every day's shard", TOTAL_DAYS, outcome.baselineDispatches());
            assertEquals("only the newest day can contribute the top 3", 1, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * Same fixture ascending: {@code sort + ts | head 3} keeps only the oldest day, eliminating on
     * {@code min} rather than {@code max}.
     */
    public void testDisjointDaysAscendingSkipsAllButTheOldestDay() throws Exception {
        String indices = createIndices("sort_et_asc_", dailyTimestamps());

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runAgainstWaitForAll("source = " + indices + " | sort + ts | head %d | fields ts", 3, dispatches);

            assertEquals("head 3 must return 3 rows", 3, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open every day's shard", TOTAL_DAYS, outcome.baselineDispatches());
            assertEquals("only the oldest day can contribute the bottom 3", 1, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * The bar is {@code 03:00}. A shard whose max ties it may hold a tying row and must be dispatched;
     * only the shard strictly below it is skipped.
     */
    public void testShardTyingTheBarIsStillDispatched() throws Exception {
        String indices = createIndices(
            "sort_et_tie_",
            List.of(
                List.of(day(10, 5), day(10, 4), day(10, 3), day(10, 3)),   // fills the heap; bar = 03:00
                List.of(day(10, 3), day(10, 2)),                            // max ties the bar exactly
                List.of(day(10, 2))                                         // max strictly below the bar
            )
        );

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runAgainstWaitForAll("source = " + indices + " | sort - ts | head %d | fields ts", 3, dispatches);

            assertEquals("head 3 must return 3 rows", 3, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open all three shards", 3, outcome.baselineDispatches());
            assertEquals(
                "the shard tying the bar must still be dispatched; only the strictly-worse one is skipped",
                2,
                outcome.gatedDispatches()
            );
        } finally {
            clearTransportRules();
        }
    }

    /**
     * Three indices whose key ranges interleave. No shard's range is disjoint from the top 3, so every
     * shard must be dispatched.
     */
    public void testOverlappingRangesDispatchEveryShard() throws Exception {
        String indices = createIndices(
            "sort_et_overlap_",
            List.of(
                List.of(day(10, 0), day(10, 5), day(10, 10), day(10, 15), day(10, 20)),
                List.of(day(10, 1), day(10, 6), day(10, 11), day(10, 16), day(10, 21)),
                List.of(day(10, 2), day(10, 7), day(10, 12), day(10, 17), day(10, 22))
            )
        );

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runAgainstWaitForAll("source = " + indices + " | sort - ts | head %d | fields ts", 3, dispatches);

            assertEquals("head 3 must return 3 rows", 3, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open all three shards", 3, outcome.baselineDispatches());
            assertEquals("no shard's range is disjoint from the top 3 — none may be skipped", 3, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    /** {@code head 100} against 35 docs: the heap never fills, so there is no bar and no shard is skipped. */
    public void testLimitLargerThanTheDataDispatchesEveryShard() throws Exception {
        String indices = createIndices("sort_et_wide_", dailyTimestamps());

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runAgainstWaitForAll("source = " + indices + " | sort - ts | head %d | fields ts", 100, dispatches);

            assertEquals("every doc is inside the window", TOTAL_DAYS * DOCS_PER_DAY, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open every day's shard", TOTAL_DAYS, outcome.baselineDispatches());
            assertEquals("an unfilled heap has no bar — every shard is still needed", TOTAL_DAYS, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * A shard whose non-null range sits below the bar but which holds a null must still be dispatched:
     * {@code DESC} maps to {@code NULLS FIRST}, so that null is the top row of the answer.
     */
    public void testNullBearingShardIsNeverEliminated() throws Exception {
        String indices = createIndices(
            "sort_et_nulls_",
            Arrays.asList(
                Arrays.asList(day(10, 5), day(10, 4), day(10, 3)),   // fills the heap; bar = 03:00
                Arrays.asList(null, day(10, 1))                        // range below the bar, but null-bearing
            )
        );

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runAgainstWaitForAll("source = " + indices + " | sort - ts | head %d | fields ts", 3, dispatches);

            assertEquals("head 3 must return 3 rows", 3, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open both shards", 2, outcome.baselineDispatches());
            assertEquals("a shard that may hold a null can never be eliminated", 2, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    /** An un-limited {@code sort} yields no {@code SortSpec}, so there is no gate and every shard runs. */
    public void testUnboundedSortDispatchesEveryShard() throws Exception {
        String indices = createIndices("sort_et_unbounded_", dailyTimestamps());

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runOnce("source = " + indices + " | sort - ts | fields ts", dispatches);

            assertEquals("no limit — every doc comes back", TOTAL_DAYS * DOCS_PER_DAY, outcome.rows().size());
            assertEquals("an un-limited sort is not gateable", TOTAL_DAYS, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * Aggregate top-K sorts a computed value with no shard statistic behind it, so no gate is built and
     * every shard runs — despite sharing the same {@code sort | head} syntax.
     */
    public void testAggregateTopKIsNotGated() throws Exception {
        String indices = createIndices("sort_et_agg_", dailyTimestamps());

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            // Sort on the group key, not the count, so the top-3 is deterministic.
            Outcome outcome = runAgainstWaitForAll(
                "source = " + indices + " | stats count() as c by host | sort - host | head %d",
                3,
                dispatches
            );

            assertEquals("head 3 must return 3 rows", 3, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open every day's shard", TOTAL_DAYS, outcome.baselineDispatches());
            assertEquals("aggregate top-K is not gateable", TOTAL_DAYS, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * The same elimination on the other merge path: projecting {@code host} while sorting on {@code ts}
     * takes the QTF / late-materialization rewrite, under which the gate must behave identically.
     */
    public void testLateMaterializationPathMatchesBaseline() throws Exception {
        String indices = createIndices("sort_et_qtf_", dailyTimestamps());

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            Outcome outcome = runAgainstWaitForAll("source = " + indices + " | sort - ts | head %d | fields ts, host", 3, dispatches);

            assertEquals("head 3 must return 3 rows", 3, outcome.rows().size());
            assertEquals("the wait-for-all baseline must open every day's shard", TOTAL_DAYS, outcome.baselineDispatches());
            assertEquals("only the newest day can contribute the top 3", 1, outcome.gatedDispatches());
        } finally {
            clearTransportRules();
        }
    }

    // ── the two-run harness ──────────────────────────────────────────────

    /** What the gated run returned, next to what the wait-for-all run cost in dispatches. */
    private record Outcome(List<String> rows, int baselineDispatches, int gatedDispatches) {}

    /**
     * Runs {@code pplTemplate} twice — once with {@code limit}, once with {@link #BASELINE_LIMIT} — and
     * asserts the gated rows equal the baseline's leading {@code limit} rows, then returns both dispatch
     * counts. The gate has no off switch, so the oversized {@code head} is what makes the second run a
     * wait-for-all reference.
     */
    private Outcome runAgainstWaitForAll(String pplTemplate, int limit, AtomicInteger dispatches) {
        String baselinePpl = String.format(Locale.ROOT, pplTemplate, BASELINE_LIMIT);
        String gatedPpl = String.format(Locale.ROOT, pplTemplate, limit);

        dispatches.set(0);
        List<String> baselineRows = renderRows(executePPL(baselinePpl));
        int baselineDispatches = dispatches.getAndSet(0);

        List<String> gatedRows = renderRows(executePPL(gatedPpl));
        int gatedDispatches = dispatches.getAndSet(0);

        logger.info("sort-et: [{}] dispatches baseline={} gated={}", gatedPpl, baselineDispatches, gatedDispatches);
        assertEquals(
            "the gate must never change the answer",
            baselineRows.subList(0, Math.min(limit, baselineRows.size())),
            gatedRows
        );
        return new Outcome(gatedRows, baselineDispatches, gatedDispatches);
    }

    /** One run, for shapes with no {@code head} and so no gated/baseline pair to compare. */
    private Outcome runOnce(String ppl, AtomicInteger dispatches) {
        dispatches.set(0);
        List<String> rows = renderRows(executePPL(ppl));
        int sent = dispatches.getAndSet(0);
        logger.info("sort-et: [{}] dispatches={}", ppl, sent);
        return new Outcome(rows, sent, sent);
    }

    /** One string per row, so row sets compare by value regardless of column types. */
    private static List<String> renderRows(PPLResponse response) {
        List<String> rendered = new ArrayList<>(response.getRows().size());
        for (Object[] row : response.getRows()) {
            rendered.add(Arrays.deepToString(row));
        }
        return rendered;
    }

    // ── fixture ──────────────────────────────────────────────────────────

    /** Counts {@link FragmentExecutionAction} requests handled on every data node. */
    private AtomicInteger countFragmentDispatches() {
        AtomicInteger counter = new AtomicInteger();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                counter.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
        }
        return counter;
    }

    private void clearTransportRules() {
        for (String node : internalCluster().getDataNodeNames()) {
            ((MockTransportService) internalCluster().getInstance(TransportService.class, node)).clearAllRules();
        }
    }

    /** {@code 2026-07-<day>T<hour>:00:00Z}, the only timestamp shape this fixture uses. */
    private static String day(int day, int hour) {
        return String.format(Locale.ROOT, "2026-07-%02dT%02d:00:00Z", day, hour);
    }

    /** A week of daily indices: day {@code d} holds {@link #DOCS_PER_DAY} docs inside day {@code d}. */
    private static List<List<String>> dailyTimestamps() {
        List<List<String>> perIndex = new ArrayList<>(TOTAL_DAYS);
        for (int d = FIRST_DAY; d < FIRST_DAY + TOTAL_DAYS; d++) {
            List<String> docs = new ArrayList<>(DOCS_PER_DAY);
            for (int hour = 0; hour < DOCS_PER_DAY; hour++) {
                docs.add(day(d, hour));
            }
            perIndex.add(docs);
        }
        return perIndex;
    }

    /**
     * Creates one single-shard parquet index per entry in {@code perIndexTimestamps}, named
     * {@code <prefix><ordinal>}, and returns them as a comma-separated PPL {@code source =} list. A
     * {@code null} timestamp indexes a doc with no {@code ts} field, so those lists must be
     * {@link Arrays#asList}. The cluster is SUITE-scoped, so each test needs its own prefix.
     */
    private String createIndices(String prefix, List<List<String>> perIndexTimestamps) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        StringBuilder commaList = new StringBuilder();
        for (int i = 0; i < perIndexTimestamps.size(); i++) {
            String index = prefix + i;

            CreateIndexResponse response = client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                // index=false keeps host off the lucene secondary format; parquet covers it.
                .setMapping("ts", "type=date", "host", "type=keyword,index=false")
                .get();
            assertTrue("index creation must be acknowledged", response.isAcknowledged());

            List<String> timestamps = perIndexTimestamps.get(i);
            for (int doc = 0; doc < timestamps.size(); doc++) {
                String host = "host-" + i + "-" + doc;
                String ts = timestamps.get(doc);
                if (ts == null) {
                    client().prepareIndex(index).setSource("host", host).get();
                } else {
                    client().prepareIndex(index).setSource("ts", ts, "host", host).get();
                }
            }
            client().admin().indices().prepareRefresh(index).get();
            client().admin().indices().prepareFlush(index).get();

            if (i > 0) {
                commaList.append(',');
            }
            commaList.append(index);
        }
        ensureGreen();
        return commaList.toString();
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
