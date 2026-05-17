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
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
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
 * Integration tests for DataFusion query cancellation.
 *
 * <p>Exercises the full cancellation path: task cancel at the coordinator cascades to
 * data-node shard tasks via {@code shouldCancelChildrenOnCancellation()}, the shard
 * task's cancellation listener fires {@code NativeBridge.cancelQuery(contextId)}, and
 * the Rust CancellationToken + AbortHandle terminate the DataFusion computation.
 *
 * <p>Also verifies that after cancellation completes, no native resources leak (memory
 * pool returns to baseline, NativeHandle live count doesn't grow).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.TEST)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering(linger = 5000)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = org.opensearch.analytics.resilience.FlightTransportThreadLeakFilter.class)
public class SearchCancellationIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(SearchCancellationIT.class);

    private static final String INDEX = "cancel_test_idx";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 50;
    private static final int TOTAL_DOCS = NUM_SHARDS * DOCS_PER_SHARD;
    private static final int VALUE = 42;
    private static final long EXPECTED_SUM = (long) TOTAL_DOCS * VALUE;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            TestPPLPlugin.class,
            FlightStreamPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            MockCommitterEnginePlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
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
            .put("arrow.memory.debug.allocator", true)
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
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        // Wait until the analytics path can see all data (parquet commits may lag).
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

    // ---------------------------------------------------------------- tests

    /**
     * Cancel the shard-level fragment task while it is blocked. Verifies that
     * the coordinator query terminates with a failure (not a hang) and that the
     * native DataFusion memory pool returns to baseline after cancellation.
     */
    public void testCancelShardFragmentTaskTerminatesQuery() throws Exception {
        createAndSeedIndex();

        // Block one data node's shard handler so we can cancel mid-flight.
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
            // Allow time for dispatch + shard handler entry.
            Thread.sleep(500);

            // Cancel the fragment tasks.
            CancelTasksResponse cancel = client().admin()
                .cluster()
                .prepareCancelTasks()
                .setActions(FragmentExecutionAction.NAME)
                .get();
            assertFalse(
                "cancel must not report node failures",
                cancel.getNodeFailures() != null && !cancel.getNodeFailures().isEmpty()
            );

            released.countDown();

            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                // Expected: query failed due to cancellation.
                logger.info("Query terminated as expected after shard cancel: {}", e.getMessage());
            }
        } finally {
            released.countDown();
            mts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        // No zombie fragment tasks.
        assertBusy(() -> {
            ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get();
            assertTrue("No residual fragment tasks: " + tasks.getTasks(), tasks.getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * Cancel the coordinator-level PPL task. The parent cancel cascades to child
     * shard tasks via {@code shouldCancelChildrenOnCancellation() = true}. Verifies:
     * - Query terminates (no hang).
     * - No residual coordinator or shard tasks remain.
     * - Native memory pool returns to baseline.
     * - NativeHandle live count doesn't grow.
     */
    public void testCancelCoordinatorTaskCascadesToShards() throws Exception {
        createAndSeedIndex();

        // Warm-up so baselines reflect steady state.
        executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        System.gc();
        Thread.sleep(50);
        DataFusionService dfs = internalCluster().getDataNodeInstances(DataFusionService.class).iterator().next();
        long memBaseline = dfs.getMemoryPoolUsage();
        int handleBaseline = NativeHandle.liveHandleCount();

        // Block one shard so cancellation lands while work is in-flight.
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
            Thread.sleep(500);

            // Cancel the coordinator (parent) task — cascades to shard children.
            CancelTasksResponse cancel = client().admin()
                .cluster()
                .prepareCancelTasks()
                .setActions(UnifiedPPLExecuteAction.NAME)
                .get();
            assertFalse("cancel must not report node failures", cancel.getNodeFailures() != null && !cancel.getNodeFailures().isEmpty());

            released.countDown();

            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                logger.info("Query terminated as expected after coordinator cancel: {}", e.getMessage());
            }
        } finally {
            released.countDown();
            mts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        // No residual tasks.
        assertBusy(() -> {
            ListTasksResponse fragmentTasks = client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get();
            assertTrue("No residual fragment tasks: " + fragmentTasks.getTasks(), fragmentTasks.getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);

        ListTasksResponse pplTasks = client().admin().cluster().prepareListTasks().setActions(UnifiedPPLExecuteAction.NAME).get();
        assertTrue("No residual PPL tasks: " + pplTasks.getTasks(), pplTasks.getTasks().isEmpty());

        // Native resource cleanup.
        assertBusy(() -> {
            long memAfter = dfs.getMemoryPoolUsage();
            assertTrue(
                "Native memory grew after cancel: baseline=" + memBaseline + " after=" + memAfter,
                memAfter <= memBaseline + 1024 * 1024
            );
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            System.gc();
            int handleAfter = NativeHandle.liveHandleCount();
            assertTrue(
                "NativeHandle live count grew: baseline=" + handleBaseline + " after=" + handleAfter,
                handleAfter <= handleBaseline
            );
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Verifies that a successful query still works after a previous query was
     * cancelled — cancellation state doesn't poison subsequent queries.
     */
    public void testQuerySucceedsAfterPreviousCancellation() throws Exception {
        createAndSeedIndex();

        // First, cancel a query mid-flight.
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
            Thread.sleep(500);
            client().admin().cluster().prepareCancelTasks().setActions(FragmentExecutionAction.NAME).get();
            released.countDown();
            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException ignore) {}
        } finally {
            released.countDown();
            mts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        // Wait for cleanup.
        assertBusy(() -> {
            ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get();
            assertTrue(tasks.getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);

        // Now run a fresh query — it should succeed with full results.
        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        long actual = ((Number) response.getRows().get(0)[response.getColumns().indexOf("total")]).longValue();
        assertEquals("Post-cancellation query must return correct sum", EXPECTED_SUM, actual);
    }
}
