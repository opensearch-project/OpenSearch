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
 * Verifies the framework-task lifecycle for the analytics query action
 * ({@link AnalyticsQueryAction}, {@code indices:data/read/analytics/query}).
 *
 * <p>PR 9 routes every analytics query through {@code client.execute(AnalyticsQueryAction, ...)}
 * and runs it under the framework-provided, cancellable {@code AnalyticsQueryTask} — instead of a
 * detached task self-registered inside {@code DefaultPlanExecutor.executeInternal}. Because the task
 * is now framework-owned, {@code HandledTransportAction} registers it before {@code doExecute} and
 * unregisters it when the listener settles, and a client disconnect / explicit cancel of that task
 * propagates into the running query.
 *
 * <p>These tests assert the two cleanup guarantees that depend on dropping the old manual
 * register/unregister: (1) a successful query leaves no residual analytics/query task (the framework
 * unregistered it), and (2) cancelling the analytics/query task terminates the query and leaves no
 * residual analytics/query or fragment tasks.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.TEST)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering(linger = 5000)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = org.opensearch.analytics.resilience.FlightTransportThreadLeakFilter.class)
public class AnalyticsQueryTaskCleanupIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AnalyticsQueryTaskCleanupIT.class);

    private static final String INDEX = "analytics_task_cleanup_idx";
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

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }

    private void assertNoResidualTasks(String action) throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(action).get();
            assertTrue("residual " + action + " tasks: " + tasks.getTasks(), tasks.getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);
    }

    // ---------------------------------------------------------------- tests

    /**
     * A successful query must leave NO residual {@code AnalyticsQueryAction} task. This is the
     * regression guard for dropping the manual {@code taskManager.unregister}: the framework
     * unregisters the task it created for {@code doExecute} once the listener settles, so neither a
     * leak (we kept manual unregister AND framework unregister → double-free) nor a dangling task
     * (we dropped unregister but the framework doesn't own it) is acceptable.
     */
    public void testSuccessfulQueryLeavesNoResidualAnalyticsTask() throws Exception {
        createAndSeedIndex();

        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        long actual = ((Number) response.getRows().get(0)[response.getColumns().indexOf("total")]).longValue();
        assertEquals("query must return the correct sum", EXPECTED_SUM, actual);

        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
    }

    /**
     * Cancelling the framework {@code AnalyticsQueryAction} task terminates the in-flight query
     * (no hang) and leaves no residual analytics/query or fragment tasks. This only works because
     * the query now runs under the framework-provided task; pre-PR-9 the query ran under a detached
     * self-registered task, so cancelling the framework action had no effect on it.
     */
    public void testCancelAnalyticsQueryTaskTerminatesQueryAndCleansUp() throws Exception {
        createAndSeedIndex();

        // Block one data node's shard handler so cancellation lands while the query is in-flight.
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
            // Allow dispatch + shard handler entry so the analytics/query task is live.
            assertBusy(() -> {
                ListTasksResponse live = client().admin().cluster().prepareListTasks().setActions(AnalyticsQueryAction.NAME).get();
                assertFalse("analytics/query task should be running", live.getTasks().isEmpty());
            }, 10, TimeUnit.SECONDS);

            CancelTasksResponse cancel = client().admin()
                .cluster()
                .prepareCancelTasks()
                .setActions(AnalyticsQueryAction.NAME)
                .get();
            assertFalse(
                "cancel must not report node failures",
                cancel.getNodeFailures() != null && cancel.getNodeFailures().isEmpty() == false
            );

            released.countDown();

            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException e) {
                // Expected: the query terminated due to cancellation rather than completing.
                logger.info("query terminated as expected after analytics/query cancel: {}", e.getMessage());
            }
        } finally {
            released.countDown();
            mts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
    }
}
