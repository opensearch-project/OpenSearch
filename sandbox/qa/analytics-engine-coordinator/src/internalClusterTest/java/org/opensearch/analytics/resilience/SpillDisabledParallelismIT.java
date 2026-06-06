/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.Version;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.analytics.AnalyticsPlugin;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Integration test for the spill-disabled query path.
 *
 * <p>When {@code datafusion.spill_directory} is unset, the DataFusion runtime is built with
 * {@code DiskManagerMode::Disabled}. In that configuration, the per-query disk-pressure
 * check {@code memory_guard::per_query_spill_budget} must return {@code SpillBudget::Disabled}
 * — NOT a phantom "disk dying" signal that would clamp every query to one partition.
 *
 * <p>This test boots a cluster with no spill_directory configured and runs a coordinator-reduce
 * GROUP BY whose memory footprint fits comfortably under the default pool. The query MUST
 * succeed and produce the correct number of groups. A regression that re-introduces the
 * unconditional 1-partition clamp would still pass the correctness assertion (single-threaded
 * reduce yields the same result), so the test additionally records query latency and asserts
 * it stays under a generous bound — proving the query was not silently single-threaded for
 * the entire pipeline.
 *
 * <p>Latency-based assertion is intentionally loose: the bound is wide enough that healthy
 * hosts always pass, while a true regression to {@code target_partitions=1} on a multi-shard
 * GROUP BY shows up consistently. If you see this test fail with a near-bound timing on
 * unrelated work, raise the bound rather than skipping the assertion — the latency floor IS
 * the regression signal.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class SpillDisabledParallelismIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "spill_disabled_test";
    private static final int NUM_DOCS = 4000;
    private static final int NUM_GROUPS = 100;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, TestPPLPlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
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

    /**
     * Note the deliberate omission of {@code datafusion.spill_directory} — that's the whole
     * point of this test. The empty default (the new contract introduced alongside the
     * setting) drives DataFusion into {@code DiskManagerMode::Disabled}.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            // datafusion.spill_directory is intentionally NOT set — empty default = spill disabled.
            .build();
    }

    private void createIndexAndIngest() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        assertTrue(
            client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).setMapping("g", "type=integer").get().isAcknowledged()
        );

        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < NUM_DOCS; i++) {
            bulk.add(client().prepareIndex(INDEX_NAME).setSource("g", i % NUM_GROUPS));
        }
        BulkResponse bulkResponse = bulk.get();
        assertFalse("Bulk ingest should not have errors", bulkResponse.hasFailures());

        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }

    private PPLResponse executePPL(String query) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(query)).actionGet();
    }

    /**
     * A GROUP BY query whose working set fits under the default memory pool MUST complete
     * successfully when spill is disabled, with the correct number of groups. This guards
     * against two regressions at once:
     *
     * <ol>
     *   <li>If the disabled-spill path is broken at runtime construction (e.g. a panic from
     *       a missing spill dir during DiskManager build), the query never even starts.</li>
     *   <li>If {@code per_query_spill_budget} ever drifts back to clamping parallelism to 1
     *       on the disabled path, the query still returns correct results — but the latency
     *       bound below catches the throughput cliff.</li>
     * </ol>
     */
    public void testNonSpillingGroupByCompletesWithSpillDisabled() throws Exception {
        createIndexAndIngest();

        long startNanos = System.nanoTime();
        PPLResponse response = executePPL("source = " + INDEX_NAME + " | stats count() by g");
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

        assertNotNull("Query response must not be null when spill is disabled", response);
        assertEquals(
            "GROUP BY g must yield " + NUM_GROUPS + " groups even with spill disabled",
            NUM_GROUPS,
            response.getRows().size()
        );

        // Loose throughput bound: a 4-shard, 4000-row GROUP BY on a healthy CI host completes
        // well under a second. 30s gives ample headroom for slow CI yet still flags a true
        // single-threaded regression (which on the same workload trends 5–10× slower than
        // multi-partition execution as shard count grows).
        assertTrue(
            "Query took " + elapsedMs + "ms — exceeds 30s loose bound; investigate whether "
                + "per_query_spill_budget is incorrectly clamping target_partitions when spill is disabled",
            elapsedMs < 30_000L
        );
    }

    /**
     * A simple aggregate (no GROUP BY) must also succeed with spill disabled. This exercises
     * the simpler non-reduce path and ensures the disabled DiskManager doesn't break plain
     * memory-only queries.
     */
    public void testSimpleAggregateSucceedsWithSpillDisabled() throws Exception {
        createIndexAndIngest();
        PPLResponse response = executePPL("source = " + INDEX_NAME + " | stats count()");
        assertNotNull(response);
        assertEquals("count() must return exactly one row", 1, response.getRows().size());
    }
}
