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
import org.opensearch.analytics.exec.DefaultPlanExecutor;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Measurement harness, not a correctness test: counts how many fragment dispatches the sort gate
 * avoids across a grid of shard count × {@code head} size × per-node dispatch window. Each row is
 * logged as a {@code MEASURE} line and transcribed to {@code debug/can-match/sort-et-observations.md}.
 *
 * <p>Asserts only that the measurement is trustworthy: gated dispatches ≤ baseline, and the gated run
 * returns the same rows as the wait-for-all baseline.
 *
 * <p>Run it explicitly; it's slower than its assertions justify in CI:
 * <pre>
 * ./gradlew :sandbox:qa:analytics-engine-coordinator:internalClusterTest \
 *     --tests "org.opensearch.analytics.canmatch.SortEarlyTerminationMeasurementIT" \
 *     -Dsandbox.enabled=true -PrustDebug
 * </pre>
 *
 * <p>Node count is a class annotation, not a grid axis — the recorded 3-node numbers came from editing
 * {@code numDataNodes} and re-running.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(
    scope = OpenSearchIntegTestCase.Scope.SUITE,
    numDataNodes = 1,
    numClientNodes = 0,
    supportsDedicatedMasters = false
)
public class SortEarlyTerminationMeasurementIT extends OpenSearchIntegTestCase {

    private static final int DOCS_PER_SHARD = 20;

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
            .build();
    }

    /**
     * Walks the grid and logs a row per point. Data is disjoint and descending — the gate's best case,
     * so these numbers are a ceiling.
     */
    public void testAvoidedSendsSweep() throws Exception {
        List<int[]> grid = List.of(
            // { shards, head, window (-1 = leave at the 5 default) }
            new int[] { 10, 10, -1 },
            new int[] { 10, 10, 1 },
            new int[] { 10, 100, 1 },
            new int[] { 50, 10, -1 },
            new int[] { 50, 10, 1 },
            new int[] { 50, 100, -1 },
            new int[] { 50, 100, 1 },
            new int[] { 200, 10, -1 },
            new int[] { 200, 10, 1 },
            new int[] { 200, 100, 1 },
            new int[] { 200, 10000, 1 }
        );

        int nodes = internalCluster().getDataNodeNames().size();
        logger.info("MEASURE header: nodes={} docsPerShard={}", nodes, DOCS_PER_SHARD);
        logger.info("MEASURE  shards  head  window  baseline  gated  avoided  avoided%  rows");

        List<String> report = new ArrayList<>();
        for (int[] row : grid) {
            report.add(measure(row[0], row[1], row[2]));
        }

        logger.info("MEASURE ===== summary =====");
        for (String line : report) {
            logger.info("MEASURE {}", line);
        }
    }

    /**
     * Runs one grid point against a wait-for-all baseline and returns its report line.
     *
     * <p>The gate has no off switch, so the baseline is the same query with {@code head} set past the
     * fixture's total row count — the heap never fills, so it never arms and every shard is dispatched.
     *
     * <p>Indices are shared across grid points with the same shard count, both to stay under the test
     * cluster's open-shard cap and so both runs of a point see identical data.
     */
    private String measure(int shards, int head, int window) throws Exception {
        String indices = indicesFor(shards);
        String ppl = "source = " + indices + " | sort - ts | head " + head + " | fields ts";
        String baselinePpl = "source = " + indices + " | sort - ts | head " + (shards * DOCS_PER_SHARD + 1) + " | fields ts";

        setWindow(window);
        AtomicInteger dispatches = countFragmentDispatches();
        try {
            dispatches.set(0);
            List<String> baselineRows = renderRows(executePPL(baselinePpl));
            int baseline = dispatches.getAndSet(0);

            List<String> gatedRows = renderRows(executePPL(ppl));
            int gated = dispatches.getAndSet(0);

            assertEquals(
                "the gated run must return the same rows as the baseline",
                baselineRows.subList(0, Math.min(head, baselineRows.size())),
                gatedRows
            );
            assertEquals("the baseline must be the full fan-out", shards, baseline);
            assertTrue("gated dispatches must never exceed baseline", gated <= baseline);

            int avoided = baseline - gated;
            double pct = baseline == 0 ? 0.0 : (100.0 * avoided / baseline);
            String line = String.format(
                Locale.ROOT,
                "%7d %5d %7s %9d %6d %8d %8.1f %5d",
                shards,
                head,
                window < 0 ? "5(def)" : String.valueOf(window),
                baseline,
                gated,
                avoided,
                pct,
                gatedRows.size()
            );
            logger.info("MEASURE {}", line);
            return line;
        } finally {
            clearTransportRules();
            clearOverride(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.getKey());
        }
    }

    // ── settings plumbing ────────────────────────────────────────────────

    private void setWindow(int window) throws Exception {
        if (window < 0) {
            return;   // leave the node-settings default in place
        }
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.getKey(), window).build()
                )
                .get()
                .isAcknowledged()
        );
        DefaultPlanExecutor executor = executor();
        assertBusy(() -> assertEquals("executor must observe the window update", window, executor.maxConcurrentShardRequestsPerNode()));
    }

    private void clearOverride(String key) {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(key).build())
            .get();
    }

    private DefaultPlanExecutor executor() {
        return internalCluster().getInstance(DefaultPlanExecutor.class, internalCluster().getNodeNames()[0]);
    }

    // ── fixture ──────────────────────────────────────────────────────────

    /** Index sets by shard count, built on first use and shared — see {@link #measure}. */
    private final Map<Integer, String> indicesByShardCount = new HashMap<>();

    private String indicesFor(int shards) {
        return indicesByShardCount.computeIfAbsent(shards, n -> createDisjointIndices("m" + n + "_", n));
    }

    /**
     * Creates {@code shards} single-shard indices with descending, non-overlapping key ranges — index
     * {@code 0} owns the highest block. Returns them as a comma-separated list for the PPL source.
     */
    private String createDisjointIndices(String prefix, int shards) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        StringBuilder commaList = new StringBuilder();
        for (int i = 0; i < shards; i++) {
            String index = prefix + i;
            CreateIndexResponse response = client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .setMapping("ts", "type=long", "host", "type=keyword,index=false")
                .get();
            assertTrue("index creation must be acknowledged", response.isAcknowledged());

            long base = (long) (shards - 1 - i) * DOCS_PER_SHARD;
            var bulk = client().prepareBulk();
            for (int doc = 0; doc < DOCS_PER_SHARD; doc++) {
                bulk.add(client().prepareIndex(index).setSource("ts", base + doc, "host", "h-" + i + "-" + doc));
            }
            assertFalse("bulk seeding must not fail", bulk.get().hasFailures());

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

    private static List<String> renderRows(PPLResponse response) {
        List<String> rendered = new ArrayList<>(response.getRows().size());
        for (Object[] row : response.getRows()) {
            rendered.add(java.util.Arrays.deepToString(row));
        }
        return rendered;
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
