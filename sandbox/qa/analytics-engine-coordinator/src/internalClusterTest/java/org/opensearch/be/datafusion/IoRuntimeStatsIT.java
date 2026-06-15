/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.sql.SqlPlanRunner;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsActionType;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodeResponse;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodesRequest;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodesResponse;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * End-to-end integration test verifying — through the DataFusion {@code _stats}
 * node-stats API — that the dedicated IO runtime actually services a parquet
 * scan's object-store reads.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class IoRuntimeStatsIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "io_runtime_stats_idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
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

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createAndSeedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
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
            .setMapping("account_number", "type=integer", "balance", "type=long")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        for (int i = 1; i <= 50; i++) {
            client().prepareIndex(INDEX)
                .setId(String.valueOf(i))
                .setSource("account_number", i, "balance", (long) (i * 1000))
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    /**
     * Run a SQL query that forces a parquet scan through DataFusion. The
     * {@code WHERE account_number > 0} predicate is required because the default
     * {@code datafusion.indexed.query_strategy=indexed} rejects plans with no
     * filter to attach an index_filter() against.
     */
    private void runScan() {
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql(
            "SELECT account_number, balance FROM " + INDEX + " WHERE account_number > 0"
        );
        assertFalse("scan must produce rows so the IO runtime did real work", rows.isEmpty());
    }

    /**
     * Aggregate IO-runtime activity across all nodes' DataFusion stats.
     *
     * <p>Returns the summed {@code total_polls_count} — the counter that advances
     * whenever an IO-runtime worker polls a task. Before the generic SpawnIoStore
     * fix, this stayed flat at 0 (the 72-thread IO runtime was idle; all reads ran
     * on the CPU runtime). We assert on polls (and busy-time below) rather than
     * {@code spawned_tasks_count} because those are the counters that move for ANY
     * IO done on the runtime, regardless of spawn-vs-poll bookkeeping.
     */
    private long[] ioRuntimeActivity() {
        DataFusionStatsNodesResponse stats = client().execute(
            DataFusionStatsActionType.INSTANCE,
            new DataFusionStatsNodesRequest(new String[0], Set.of())
        ).actionGet();

        assertFalse("at least one node must report DataFusion stats", stats.getNodes().isEmpty());

        long polls = 0;
        long busyMs = 0;
        boolean sawExecutors = false;
        for (DataFusionStatsNodeResponse node : stats.getNodes()) {
            DataFusionStats df = node.getStats();
            if (df == null) {
                continue;
            }
            NativeExecutorsStats executors = df.getNativeExecutorsStats();
            if (executors == null) {
                continue;
            }
            sawExecutors = true;
            RuntimeMetrics io = executors.getIoRuntime();
            assertNotNull("IO runtime metrics must always be present", io);
            assertTrue("IO runtime must report worker threads", io.workersCount > 0);
            polls += io.totalPollsCount;
            busyMs += io.totalBusyDurationMs;
        }
        assertTrue("at least one node must report native executor stats", sawExecutors);
        return new long[] { polls, busyMs };
    }

    public void testIoRuntimeServicesParquetScanReads() throws Exception {
        createAndSeedIndex();

        long[] before = ioRuntimeActivity();

        runScan();
        runScan();

        long[] after = ioRuntimeActivity();

        // total_polls_count must advance: a parquet scan now dispatches its
        // object-store reads onto the IO runtime (via SpawnIoStore), so IO workers
        // poll tasks. A flat count means IO is still running on the CPU runtime.
        assertTrue(
            "IO runtime total_polls_count must increase across parquet scans "
                + "(polls before=" + before[0] + ", after=" + after[0]
                + "; busy_ms before=" + before[1] + ", after=" + after[1]
                + ") — object-store reads are not being dispatched to the IO runtime",
            after[0] > before[0]
        );
    }
}
