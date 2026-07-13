/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.cancellation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Verifies the analytics query path honors {@code search.cancel_after_time_interval} and cleans up
 * its task tree when the timeout fires. Regression guard for the {@code AnalyticsQueryTask}
 * null→MINUS_ONE coercion that silently disabled the cluster timeout.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.TEST)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering(linger = 5000)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = org.opensearch.analytics.resilience.FlightTransportThreadLeakFilter.class)
public class AnalyticsTimeoutCancellationIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AnalyticsTimeoutCancellationIT.class);

    private static final String INDEX = "analytics_timeout_idx";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 50;
    private static final int TOTAL_DOCS = NUM_SHARDS * DOCS_PER_SHARD;
    private static final int VALUE = 7;
    private static final long EXPECTED_SUM = (long) TOTAL_DOCS * VALUE;
    private static final TimeValue HARNESS_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            TestPPLPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            MockCommitterEnginePlugin.class,
            MockDataFormatPlugin.class
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

    // ---------------------------------------------------------------- fixture

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

        for (int i = 0; i < TOTAL_DOCS; i++) {
            client().prepareIndex(INDEX).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        try {
            assertBusy(() -> {
                PPLResponse r = executePPL("source = " + INDEX + " | stats sum(value) as total");
                long actual = ((Number) r.getRows().get(0)[r.getColumns().indexOf("total")]).longValue();
                assertEquals("seed not yet visible", EXPECTED_SUM, actual);
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("timed out waiting for seed visibility", e);
        }
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }

    private void setClusterCancelAfter(String value) {
        Settings.Builder s = Settings.builder();
        s.put("search.cancel_after_time_interval", value);
        assertTrue(
            client().admin().cluster().prepareUpdateSettings().setTransientSettings(s).get().isAcknowledged()
        );
    }

    private void clearClusterCancelAfter() {
        Settings.Builder s = Settings.builder();
        s.putNull("search.cancel_after_time_interval");
        client().admin().cluster().prepareUpdateSettings().setTransientSettings(s).get();
    }

    private void assertNoResidualTasks(String action) throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(action).get();
            assertTrue("residual " + action + " tasks: " + tasks.getTasks(), tasks.getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);
    }

    // ---------------------------------------------------------------- tests

    /** A query that can't finish in time (data node blocked) is cancelled by the timeout, no residual tasks. */
    public void testClusterTimeoutCancelsInFlightQueryAndCleansUp() throws Exception {
        createAndSeedIndex();
        setClusterCancelAfter("1s");
        try {
            // Block one data node's fragment handler so the query stays in-flight well past 1s.
            String victim = randomFrom(internalCluster().getDataNodeNames());
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
            CountDownLatch released = new CountDownLatch(1);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                try {
                    released.await(HARNESS_TIMEOUT.seconds(), TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                handler.messageReceived(request, channel, task);
            });

            ExecutorService exec = Executors.newSingleThreadExecutor();
            try {
                long start = System.nanoTime();
                Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total"));

                // The query must terminate due to the 1s timeout — well before the harness block
                // would be released — rather than hanging for the full HARNESS_TIMEOUT.
                try {
                    fut.get(HARNESS_TIMEOUT.seconds() - 5, TimeUnit.SECONDS);
                    fail("query should have been cancelled by the cluster timeout, not completed");
                } catch (ExecutionException e) {
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    logger.info("query terminated by timeout after {}ms: {}", elapsedMs, e.getMessage());
                    assertTrue(
                        "query should terminate shortly after the 1s timeout, took " + elapsedMs + "ms",
                        elapsedMs < (HARNESS_TIMEOUT.seconds() - 5) * 1000
                    );
                } catch (TimeoutException te) {
                    fail("timeout did not fire — query hung past the cluster cancel_after interval");
                } finally {
                    released.countDown();
                }
            } finally {
                released.countDown();
                mts.clearAllRules();
                exec.shutdownNow();
                exec.awaitTermination(5, TimeUnit.SECONDS);
            }

            assertNoResidualTasks(AnalyticsQueryAction.NAME);
            assertNoResidualTasks(FragmentExecutionAction.NAME);
        } finally {
            clearClusterCancelAfter();
        }
    }

    /** Control: with no timeout set, the query completes normally (timeout machinery is inert when disabled). */
    public void testNoTimeoutQueryCompletesNormally() throws Exception {
        createAndSeedIndex();
        clearClusterCancelAfter();

        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total");
        long actual = ((Number) response.getRows().get(0)[response.getColumns().indexOf("total")]).longValue();
        assertEquals("query must return the correct sum when no timeout is set", EXPECTED_SUM, actual);

        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
    }
}
