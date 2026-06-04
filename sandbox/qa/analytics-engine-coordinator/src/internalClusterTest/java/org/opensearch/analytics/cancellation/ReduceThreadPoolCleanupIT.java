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
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
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
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;
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
 * Verifies the coordinator-reduce thread pool ({@code analytics_reduce}) is released after a query
 * — both on the normal-completion path and the cancellation path.
 *
 * <p>The reduce drain runs on a thread from this fixed pool (one task per coordinator-reduce
 * stage). If {@code DatafusionReduceSink.reduce} fails to unwind on cancellation (the drain parks
 * in {@code stream_next} and is never cancelled, or {@code closeImpl} double-frees / deadlocks),
 * the drain thread leaks: the pool's {@code active} count stays elevated and eventually the pool
 * saturates. These tests assert the pool drains back to {@code active=0} after each query, which
 * is what this PR's {@code closeImpl}/cancel teardown guarantees.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.TEST)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering(linger = 5000)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = org.opensearch.analytics.resilience.FlightTransportThreadLeakFilter.class)
public class ReduceThreadPoolCleanupIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(ReduceThreadPoolCleanupIT.class);

    private static final String INDEX = "reduce_pool_cleanup_idx";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 50;
    private static final int TOTAL_DOCS = NUM_SHARDS * DOCS_PER_SHARD;
    private static final int VALUE = 7;
    private static final long EXPECTED_SUM = (long) TOTAL_DOCS * VALUE;
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

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }

    /** Max {@code active}/{@code queue} for the {@code analytics_reduce} pool across all nodes. */
    private int reducePoolActiveAcrossNodes() {
        int maxActive = 0;
        for (ThreadPool tp : internalCluster().getInstances(ThreadPool.class)) {
            for (ThreadPoolStats.Stats s : tp.stats()) {
                if (AnalyticsPlugin.REDUCE_THREAD_POOL_NAME.equals(s.getName())) {
                    maxActive = Math.max(maxActive, s.getActive() + s.getQueue());
                }
            }
        }
        return maxActive;
    }

    private void assertReducePoolDrains() throws Exception {
        assertBusy(() -> {
            int active = reducePoolActiveAcrossNodes();
            assertEquals("analytics_reduce pool must drain to 0 active+queued; got " + active, 0, active);
        }, 30, TimeUnit.SECONDS);
    }

    private void assertNoResidualTasks(String action) throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(action).get();
            assertTrue("residual " + action + " tasks: " + tasks.getTasks(), tasks.getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * Normal completion: after a coordinator-reduce query finishes, the drain thread it borrowed
     * from {@code analytics_reduce} must be returned — the pool drains to zero active.
     */
    public void testReducePoolReleasedAfterSuccessfulQuery() throws Exception {
        createAndSeedIndex();

        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        long actual = ((Number) response.getRows().get(0)[response.getColumns().indexOf("total")]).longValue();
        assertEquals("query must return the correct sum", EXPECTED_SUM, actual);

        assertReducePoolDrains();
        assertNoResidualTasks(AnalyticsQueryAction.NAME);
    }

    /**
     * Cancellation: a query cancelled while a shard handler is blocked must still release its reduce
     * drain thread — the cancel propagates into {@code DatafusionReduceSink.reduce}, which unwinds
     * and frees the pool thread. Without that, the parked drain leaks the thread permanently.
     */
    public void testReducePoolReleasedAfterCancelledQuery() throws Exception {
        createAndSeedIndex();

        // Block one data node's shard handler so cancellation lands while the query is in-flight
        // (the reduce drain is parked waiting for this shard's input).
        String victim = randomFrom(internalCluster().getDataNodeNames());
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        CountDownLatch released = new CountDownLatch(1);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            try {
                released.await(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            handler.messageReceived(request, channel, task);
        });

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total"));
            assertBusy(() -> {
                ListTasksResponse live = client().admin().cluster().prepareListTasks().setActions(AnalyticsQueryAction.NAME).get();
                assertFalse("analytics/query task should be running", live.getTasks().isEmpty());
            }, 10, TimeUnit.SECONDS);

            CancelTasksResponse cancel = client().admin().cluster().prepareCancelTasks().setActions(AnalyticsQueryAction.NAME).get();
            assertFalse(
                "cancel must not report node failures",
                cancel.getNodeFailures() != null && cancel.getNodeFailures().isEmpty() == false
            );

            released.countDown();
            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                logger.info("query terminated as expected after cancel: {}", e.getMessage());
            }
        } finally {
            released.countDown();
            mts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        // The cancelled query's reduce drain must have unwound and returned its pool thread.
        assertReducePoolDrains();
        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
    }
}
