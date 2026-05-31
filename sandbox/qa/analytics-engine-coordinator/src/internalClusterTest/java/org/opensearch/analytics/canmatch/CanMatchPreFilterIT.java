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
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end coverage for the can-match pre-filter wired into
 * {@code ShardFragmentStageExecution.materializeTasks()}.
 *
 * <p>The dataset is a 2-shard parquet index with disjoint per-shard value ranges
 * (shard A holds rows in {@code [0, 50]}, shard B holds rows in {@code [1000, 1050]}).
 * The IT issues queries whose range predicate is satisfiable only by one shard;
 * if can-match is wired correctly the other shard never scans, but the functional
 * assertion holds either way — pruning is a correctness-safe optimisation.
 *
 * <p>This suite exists to verify the integration is wired (no NPE, no transport
 * errors, no result corruption); shard-elimination unit-tests cover the actual
 * filtering semantics under controlled mock inputs.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(reason = "verify can-match prune fires", value = "org.opensearch.analytics.exec.stage.shard.ShardFragmentStageExecution:DEBUG")
public class CanMatchPreFilterIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "canmatch_idx";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_RANGE = 20;
    private static final int LOW_RANGE_BASE = 0;
    private static final int HIGH_RANGE_BASE = 1_000;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

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
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
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
     * Range predicate that selects only the low-range shard.
     * Both shards exist; pruning the high-range one is the point.
     */
    public void testRangeFilterSelectingOneShard() throws Exception {
        createAndSeedIndex();

        // Both shards have docs visible to the engine: sanity-check the seed.
        assertBusy(() -> {
            PPLResponse r = executePPL("source = " + INDEX + " | stats count() as c");
            long total = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
            assertEquals("total seeded docs", (long) DOCS_PER_RANGE * 2, total);
        }, 30, TimeUnit.SECONDS);

        // value < 100 → only the low-range shard's docs match. The high-range shard
        // (values in [1000, 1050]) should be eliminated by can-match.
        PPLResponse r = executePPL("source = " + INDEX + " | where value < 100 | stats count() as c", QUERY_TIMEOUT);
        long lowOnly = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("only low-range shard docs match", DOCS_PER_RANGE, lowOnly);
    }

    /**
     * Range predicate that no shard can match — exercises the all-shards-eliminated
     * short-circuit path, which leaves {@code materializeTasks} with an empty list
     * and the stage completes immediately.
     */
    public void testRangeFilterEliminatesAllShards() throws Exception {
        createAndSeedIndex();
        PPLResponse r = executePPL("source = " + INDEX + " | where value > 5000 | stats count() as c", QUERY_TIMEOUT);
        long none = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("no shard can match the predicate", 0L, none);
    }

    /**
     * Range predicate that both shards satisfy — pruning must NOT drop either shard.
     * Regression guard: ensures fail-open / wide-enough range correctly skips pruning.
     */
    public void testRangeFilterMatchingBothShardsIsUntouched() throws Exception {
        createAndSeedIndex();
        PPLResponse r = executePPL("source = " + INDEX + " | where value >= 0 | stats count() as c", QUERY_TIMEOUT);
        long all = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("both shards contribute", (long) DOCS_PER_RANGE * 2, all);
    }

    // ── fixture ────────────────────────────────────────────────────────────

    private void createAndSeedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("value", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        // Force-route so low values land on one shard and high values on the other.
        // Routing keys "lo" and "hi" hash to different shards in a 2-shard index
        // (verified manually); even if they collided the test would still pass
        // functionally — the canMatch path would simply have nothing to prune.
        for (int i = 0; i < DOCS_PER_RANGE; i++) {
            client().prepareIndex(INDEX)
                .setId("lo-" + i)
                .setRouting("lo")
                .setSource("value", LOW_RANGE_BASE + i)
                .get();
            client().prepareIndex(INDEX)
                .setId("hi-" + i)
                .setRouting("hi")
                .setSource("value", HIGH_RANGE_BASE + i)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }
}
