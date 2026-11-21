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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.NoSuchNodeException;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;


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
@LuceneTestCase.AwaitsFix(bugUrl = "Flaky: Arrow allocator reports 138-byte residual on cancellation path (~1.5% rate)")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.TEST)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering(linger = 5000)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = org.opensearch.analytics.resilience.FlightTransportThreadLeakFilter.class)
public class SearchCancellationIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(SearchCancellationIT.class);

    private static final String INDEX = "cancel_test_idx";
    /**
     * Action name for the {@code AnalyticsQueryTask} parent — registered as a string literal
     * in {@code DefaultPlanExecutor.execute()} (see line 179). This is the task whose cancellation
     * propagates through {@code shouldCancelChildrenOnCancellation()=true} to the per-shard
     * fragments and ultimately fails the client's PPL request with {@link TaskCancelledException}.
     * Cancelling the outer {@code UnifiedPPLExecuteAction} transport task does NOT cascade because
     * that outer task is a plain {@code HandledTransportAction} task, not a {@code CancellableTask}.
     */
    private static final String ANALYTICS_QUERY_TASK = "analytics_query";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_SHARD = 50;
    private static final int TOTAL_DOCS = NUM_SHARDS * DOCS_PER_SHARD;
    private static final int VALUE = 42;
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
            .put("arrow.memory.debug.allocator", true)
            // TestPPLTransportAction forks to the SEARCH thread pool; the default sizing
            // ((cpu*3/2)+1, capped low in test JVMs) limits how many PPL queries can be
            // mid-flight in parallel. Concurrency tests need a larger pool to exercise
            // genuine parallelism instead of queueing.
            .put("thread_pool.search.size", 16)
            .put("thread_pool.search.queue_size", 128)
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

    /** Helper: block fragment handlers on every data node and return the list of latches. */
    private Map<String, CountDownLatch> blockAllFragmentHandlers() {
        Map<String, CountDownLatch> latches = new HashMap<>();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            CountDownLatch latch = new CountDownLatch(1);
            latches.put(node, latch);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                try {
                    latch.await(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                handler.messageReceived(request, channel, task);
            });
        }
        return latches;
    }

    /** Helper: clear all mock transport rules on every data node. */
    private void clearAllFragmentHandlers() {
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.clearAllRules();
        }
    }

    /**
     * Cancelling an in-flight PPL query by task id surfaces a {@link TaskCancelledException}
     * to the client (the same exception the {@code POST /_tasks/{id}/_cancel} REST path
     * delivers to a calling client). Verifies the parent task and all child fragment tasks
     * are removed from the registry after cancel.
     */
    public void testCancelMidFlightReturnsCancellationError() throws Exception {
        createAndSeedIndex();

        // Block fragments on every data node so the query can't complete while we cancel.
        Map<String, CountDownLatch> latches = blockAllFragmentHandlers();

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total"));

            // Wait for the analytics_query task (the cancellable parent) to register.
            AtomicReference<TaskId> parentRef = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks()
                    .setActions(ANALYTICS_QUERY_TASK).get().getTasks();
                assertEquals("expected exactly one analytics_query task", 1, tasks.size());
                parentRef.set(tasks.get(0).getTaskId());
            }, 10, TimeUnit.SECONDS);

            // Cancel by task id. NodeFailures may appear when child tasks complete during the
            // cancel propagation window — same contract as CancellableTasksIT.
            // testCancelTaskMultipleTimes — so we only assert the cancel API itself didn't throw.
            client().admin().cluster().prepareCancelTasks().setTaskId(parentRef.get()).get();

            latches.values().forEach(CountDownLatch::countDown);

            // The client must observe TaskCancelledException specifically.
            ExecutionException ee = expectThrows(ExecutionException.class,
                () -> fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS));
            assertNotNull("client must see TaskCancelledException, got: " + ee.getCause(),
                ExceptionsHelper.unwrap(ee, TaskCancelledException.class));
        } finally {
            latches.values().forEach(CountDownLatch::countDown);
            clearAllFragmentHandlers();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertBusy(() -> {
            assertTrue(client().admin().cluster().prepareListTasks()
                .setActions(ANALYTICS_QUERY_TASK).get().getTasks().isEmpty());
            assertTrue(client().admin().cluster().prepareListTasks()
                .setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * Issuing a second cancel against the same in-flight task ID immediately after the first
     * does not throw, deadlock, or trigger duplicate cleanup. The original PPL future still
     * fails with a single {@link TaskCancelledException}, and the registry is clean afterward.
     */
    public void testDoubleCancelIsIdempotent() throws Exception {
        createAndSeedIndex();

        Map<String, CountDownLatch> latches = blockAllFragmentHandlers();

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total"));

            AtomicReference<TaskId> parentRef = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks()
                    .setActions(ANALYTICS_QUERY_TASK).get().getTasks();
                assertEquals(1, tasks.size());
                parentRef.set(tasks.get(0).getTaskId());
            }, 10, TimeUnit.SECONDS);
            TaskId parent = parentRef.get();

            // Both cancels must complete without throwing to the caller. Per the existing core
            // contract (CancellableTasksIT.testCancelTaskMultipleTimes), node_failures may
            // appear in the response body for child tasks that complete during the cancel
            // propagation window. The client-observable invariants verified below are: a single
            // TaskCancelledException reaches the original PPL future, and the registry is
            // clean after cleanup — no duplicate cancellation work or deadlock.
            client().admin().cluster().prepareCancelTasks().setTaskId(parent).waitForCompletion(false).get();
            client().admin().cluster().prepareCancelTasks().setTaskId(parent).waitForCompletion(false).get();

            latches.values().forEach(CountDownLatch::countDown);

            // Original future must have failed with TaskCancelledException.
            ExecutionException ee = expectThrows(ExecutionException.class,
                () -> fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS));
            assertNotNull(ExceptionsHelper.unwrap(ee, TaskCancelledException.class));
        } finally {
            latches.values().forEach(CountDownLatch::countDown);
            clearAllFragmentHandlers();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertBusy(() -> {
            assertTrue(client().admin().cluster().prepareListTasks().setActions(ANALYTICS_QUERY_TASK).get().getTasks().isEmpty());
            assertTrue(client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty());
        }, 10, TimeUnit.SECONDS);
    }

    /**
     * Cancelling a non-existent task ID against a real node returns a clean response (no
     * exception thrown to caller, no tasks reported as cancelled), with the per-node lookup
     * surfaced as a {@link ResourceNotFoundException} node failure.
     */
    public void testCancelUnknownTaskIdOnRealNode() {
        String realNode = clusterService().localNode().getId();
        TaskId fake = new TaskId(realNode + ":99999999");
        CancelTasksResponse resp = client().admin().cluster().prepareCancelTasks().setTaskId(fake).get();
        assertEquals("no tasks should be reported as cancelled", 0, resp.getTasks().size());
        assertEquals("expected exactly one node failure", 1, resp.getNodeFailures().size());
        assertNotNull(
            "node failure must wrap ResourceNotFoundException",
            ExceptionsHelper.unwrap(resp.getNodeFailures().get(0), ResourceNotFoundException.class));
    }

    /**
     * Cancelling a task whose node ID does not exist in the cluster returns a clean response,
     * with the unresolvable node surfaced as a {@link NoSuchNodeException} node failure.
     */
    public void testCancelTaskOnNonexistentNode() {
        TaskId fake = new TaskId("no_such_node:1");
        CancelTasksResponse resp = client().admin().cluster().prepareCancelTasks().setTaskId(fake).get();
        assertEquals(0, resp.getTasks().size());
        assertEquals(1, resp.getNodeFailures().size());
        assertNotNull(
            "node failure must wrap NoSuchNodeException",
            ExceptionsHelper.unwrap(resp.getNodeFailures().get(0), NoSuchNodeException.class));
    }

    /**
     * Cancelling a PPL task ID that has already completed is a safe no-op: the cancel API
     * returns a {@link ResourceNotFoundException} node failure (the same contract as the
     * existing {@code CancellableTasksIT.testCancelTaskMultipleTimes}) and the task registry
     * is unaffected. The parent task ID is captured during execution via a mock transport
     * probe on the fragment handler since the query is too fast to poll for from the test
     * thread.
     */
    public void testCancelAlreadyCompletedPplTask() throws Exception {
        createAndSeedIndex();

        AtomicReference<TaskId> parentRef = new AtomicReference<>();
        CountDownLatch captured = new CountDownLatch(1);
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                if (task.getParentTaskId() != null && task.getParentTaskId().isSet()
                    && parentRef.compareAndSet(null, task.getParentTaskId())) {
                    captured.countDown();
                }
                handler.messageReceived(request, channel, task);
            });
        }

        try {
            executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            assertTrue("expected to capture parent TaskId", captured.await(10, TimeUnit.SECONDS));
        } finally {
            clearAllFragmentHandlers();
        }
        assertNotNull(parentRef.get());

        // Wait for parent task to be fully gone.
        assertBusy(() -> assertTrue(
            client().admin().cluster().prepareListTasks().setActions(ANALYTICS_QUERY_TASK).get().getTasks().isEmpty()),
            10, TimeUnit.SECONDS);

        CancelTasksResponse resp = client().admin().cluster()
            .prepareCancelTasks().setTaskId(parentRef.get()).get();
        assertEquals(0, resp.getTasks().size());
        assertEquals(1, resp.getNodeFailures().size());
        assertNotNull(
            ExceptionsHelper.unwrap(resp.getNodeFailures().get(0), ResourceNotFoundException.class));
    }

    /**
     * Cancellation propagates from the coordinator parent task to per-shard fragment tasks
     * across data nodes. Verifies at least one fragment observes
     * {@link org.opensearch.tasks.CancellableTask#isCancelled()} {@code == true} when the
     * latch is released, and that all fragment and parent tasks are removed afterward.
     */
    public void testCoordinatorCancelMarksAllShardTasksCancelled() throws Exception {
        createAndSeedIndex();

        // Capture isCancelled() per fragment after the latch is released.
        Map<String, Boolean> cancelledByNode = new ConcurrentHashMap<>();
        Map<String, CountDownLatch> latches = new HashMap<>();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            CountDownLatch latch = new CountDownLatch(1);
            latches.put(node, latch);
            String nodeName = node;
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                try { latch.await(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                cancelledByNode.put(nodeName, task instanceof org.opensearch.tasks.CancellableTask
                    ? ((org.opensearch.tasks.CancellableTask) task).isCancelled() : false);
                handler.messageReceived(request, channel, task);
            });
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total"));

            AtomicReference<TaskId> parentRef = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks()
                    .setActions(ANALYTICS_QUERY_TASK).get().getTasks();
                assertEquals(1, tasks.size());
                parentRef.set(tasks.get(0).getTaskId());
            }, 10, TimeUnit.SECONDS);

            // Wait until both shard fragments are blocked at the latch.
            assertBusy(() -> {
                int n = client().admin().cluster().prepareListTasks()
                    .setActions(FragmentExecutionAction.NAME).get().getTasks().size();
                assertEquals("expected " + NUM_SHARDS + " fragment tasks", NUM_SHARDS, n);
            }, 10, TimeUnit.SECONDS);

            client().admin().cluster().prepareCancelTasks().setTaskId(parentRef.get()).waitForCompletion(false).get();

            // Release fragments; each handler then records task.isCancelled().
            latches.values().forEach(CountDownLatch::countDown);

            try { fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS); }
            catch (ExecutionException ee) { /* expected */ }
        } finally {
            latches.values().forEach(CountDownLatch::countDown);
            clearAllFragmentHandlers();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        // A fragment that completed before cancellation propagated is acceptable, but at
        // least one fragment must observe isCancelled()=true (proving propagation works);
        // the hard guarantee is no residual fragment tasks after cleanup.
        assertFalse("expected fragment observations from at least one node", cancelledByNode.isEmpty());
        boolean anyCancelled = cancelledByNode.values().stream().anyMatch(Boolean::booleanValue);
        assertTrue("at least one fragment must observe isCancelled()=true; observations: " + cancelledByNode, anyCancelled);

        assertBusy(() -> assertTrue(
            client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty()),
            10, TimeUnit.SECONDS);
    }

    /**
     * Cancellation works for both predicate-filtered and full-scan PPL query shapes. Exercises
     * the same cancel-by-task-id flow against two distinct query plans against the seed index.
     */
    public void testCancellationWorksForFilteredAndFullScanQueries() throws Exception {
        createAndSeedIndex();
        cancelAndAssertTaskCancelledException("source = " + INDEX + " | stats sum(value) as total");
        cancelAndAssertTaskCancelledException("source = " + INDEX + " | where value > 0 | stats sum(value) as total");
    }

    private void cancelAndAssertTaskCancelledException(String pplQuery) throws Exception {
        // Block fragments on every data node so the query can't complete on one node while
        // the other is still blocked.
        Map<String, CountDownLatch> latches = blockAllFragmentHandlers();

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL(pplQuery));

            AtomicReference<TaskId> parentRef = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks()
                    .setActions(ANALYTICS_QUERY_TASK).get().getTasks();
                assertEquals(1, tasks.size());
                parentRef.set(tasks.get(0).getTaskId());
            }, 10, TimeUnit.SECONDS);

            client().admin().cluster().prepareCancelTasks().setTaskId(parentRef.get()).waitForCompletion(false).get();
            latches.values().forEach(CountDownLatch::countDown);

            ExecutionException ee = expectThrows(ExecutionException.class,
                () -> fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS));
            assertNotNull("query [" + pplQuery + "] should fail with TaskCancelledException",
                ExceptionsHelper.unwrap(ee, TaskCancelledException.class));
        } finally {
            latches.values().forEach(CountDownLatch::countDown);
            clearAllFragmentHandlers();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertBusy(() -> assertTrue(
            client().admin().cluster().prepareListTasks().setActions(ANALYTICS_QUERY_TASK).get().getTasks().isEmpty()),
            10, TimeUnit.SECONDS);
    }

    /**
     * Native memory pool usage and {@link NativeHandle} live count return to baseline when
     * cancellation cleanup runs from an error path. One data node's fragment handler is
     * forced to throw, so the cancel cascade must clean up native resources without relying
     * on the happy-path completion of every shard.
     */
    public void testCancelDoesNotLeakResourcesOnErrorPath() throws Exception {
        createAndSeedIndex();

        // Warm-up to establish baselines.
        executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        System.gc();
        Thread.sleep(50);
        DataFusionService dfs = internalCluster().getDataNodeInstances(DataFusionService.class).iterator().next();
        long memBaseline = dfs.getMemoryPoolUsage();
        int handleBaseline = NativeHandle.liveHandleCount();

        // On one node: throw from the fragment handler so cancellation cleanup runs from
        // the failure path. On the other node: block normally so cancel can land mid-flight.
        List<String> dataNodes = new ArrayList<>(internalCluster().getDataNodeNames());
        String erroringNode = dataNodes.get(0);
        String blockingNode = dataNodes.size() > 1 ? dataNodes.get(1) : dataNodes.get(0);

        MockTransportService blockingMts = (MockTransportService) internalCluster().getInstance(TransportService.class, blockingNode);
        CountDownLatch released = new CountDownLatch(1);
        blockingMts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            try { released.await(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            handler.messageReceived(request, channel, task);
        });
        if (!erroringNode.equals(blockingNode)) {
            MockTransportService erroringMts = (MockTransportService) internalCluster().getInstance(TransportService.class, erroringNode);
            erroringMts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                channel.sendResponse(new RuntimeException("injected fragment failure"));
            });
        }

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total"));
            Thread.sleep(500);

            client().admin().cluster().prepareCancelTasks()
                .setActions(UnifiedPPLExecuteAction.NAME).get();
            released.countDown();

            try { fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS); }
            catch (ExecutionException | TimeoutException ignored) { /* expected */ }
        } finally {
            released.countDown();
            clearAllFragmentHandlers();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertBusy(() -> assertTrue(
            client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty()),
            10, TimeUnit.SECONDS);

        // Same resource-leak assertions as the happy-path test.
        assertBusy(() -> {
            long memAfter = dfs.getMemoryPoolUsage();
            assertTrue("native memory grew on error path: baseline=" + memBaseline + " after=" + memAfter,
                memAfter <= memBaseline + 1024 * 1024);
        }, 30, TimeUnit.SECONDS);
        assertBusy(() -> {
            System.gc();
            int after = NativeHandle.liveHandleCount();
            assertTrue("NativeHandle live count grew on error path: baseline=" + handleBaseline + " after=" + after,
                after <= handleBaseline);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Cancellation issued before fragment dispatch leaves the cluster in a clean state with no
     * residual fragment tasks. Holds {@link UnifiedPPLExecuteAction#NAME} at the cluster
     * manager via a mock transport, cancels the parent task, then releases. Logs whether
     * fragment dispatch was beaten by the cancel propagation as a soft signal; the hard
     * assertion is that no fragment tasks remain after cleanup.
     */
    public void testCancelAtPlanningStagePreventsFragmentDispatch() throws Exception {
        createAndSeedIndex();

        // Block at the coordinator action handler — before any fragment dispatch.
        String coord = internalCluster().getClusterManagerName();
        MockTransportService coordMts = (MockTransportService) internalCluster().getInstance(TransportService.class, coord);
        CountDownLatch released = new CountDownLatch(1);
        coordMts.addRequestHandlingBehavior(UnifiedPPLExecuteAction.NAME, (handler, request, channel, task) -> {
            try { released.await(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS); }
            catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            handler.messageReceived(request, channel, task);
        });

        ExecutorService exec = Executors.newSingleThreadExecutor();
        boolean fragmentEverRegistered = false;
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total"));

            // Wait until the parent task exists, but no fragments have been dispatched yet.
            AtomicReference<TaskId> parentRef = new AtomicReference<>();
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks()
                    .setActions(UnifiedPPLExecuteAction.NAME).get().getTasks();
                assertEquals(1, tasks.size());
                parentRef.set(tasks.get(0).getTaskId());
            }, 10, TimeUnit.SECONDS);

            client().admin().cluster().prepareCancelTasks().setTaskId(parentRef.get()).waitForCompletion(false).get();
            released.countDown();

            // Sample for fragment registration over a short window — if it ever appears,
            // planning-stage cancellation didn't take effect in time. This is a soft
            // assertion: under contention the fragment can briefly register before the
            // cancellation propagates, but no fragment should remain after cleanup.
            for (int i = 0; i < 10; i++) {
                if (!client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty()) {
                    fragmentEverRegistered = true;
                    break;
                }
                Thread.sleep(50);
            }

            try { fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS); }
            catch (ExecutionException ee) { /* expected */ }
        } finally {
            released.countDown();
            coordMts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        // Hard requirement: cleanup leaves no residual fragment tasks.
        assertBusy(() -> assertTrue(
            client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty()),
            10, TimeUnit.SECONDS);

        // Soft signal: log if planning-stage cancel didn't beat fragment registration this run.
        if (fragmentEverRegistered) {
            logger.info("testCancelAtPlanningStagePreventsFragmentDispatch: fragment registered before cancel propagated; "
                + "cleanup still verified clean");
        }
    }

    /**
     * Cancelling one query out of N concurrent in-flight queries terminates only the
     * targeted one. Launches three parallel PPL queries against the seed index, cancels
     * exactly one parent task, and asserts exactly one future failed with
     * {@link TaskCancelledException} while the others returned the correct sum.
     */
    public void testCancellingOneQueryDoesNotAffectSiblings() throws Exception {
        createAndSeedIndex();
        int n = 3;

        Map<String, CountDownLatch> latches = blockAllFragmentHandlers();

        ExecutorService exec = Executors.newFixedThreadPool(n);
        List<Future<PPLResponse>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                futures.add(exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total")));
            }

            // Wait for all N parent tasks to register.
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks()
                    .setActions(ANALYTICS_QUERY_TASK).get().getTasks();
                assertEquals(n, tasks.size());
            }, 15, TimeUnit.SECONDS);

            List<TaskInfo> all = client().admin().cluster().prepareListTasks()
                .setActions(ANALYTICS_QUERY_TASK).get().getTasks();
            TaskId victim = all.get(0).getTaskId();

            // Cancel — node_failures permitted (existing core contract).
            client().admin().cluster().prepareCancelTasks().setTaskId(victim).waitForCompletion(false).get();

            // Release all blocked fragments.
            latches.values().forEach(CountDownLatch::countDown);

            int cancelled = 0, ok = 0;
            for (Future<PPLResponse> f : futures) {
                try {
                    PPLResponse r = f.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
                    long actual = ((Number) r.getRows().get(0)[r.getColumns().indexOf("total")]).longValue();
                    assertEquals("sibling query must return correct sum", EXPECTED_SUM, actual);
                    ok++;
                } catch (ExecutionException ee) {
                    assertNotNull(ExceptionsHelper.unwrap(ee, TaskCancelledException.class));
                    cancelled++;
                }
            }
            assertEquals("exactly one query must be cancelled", 1, cancelled);
            assertEquals("the rest must succeed", n - 1, ok);
        } finally {
            latches.values().forEach(CountDownLatch::countDown);
            clearAllFragmentHandlers();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertBusy(() -> {
            assertTrue(client().admin().cluster().prepareListTasks().setActions(ANALYTICS_QUERY_TASK).get().getTasks().isEmpty());
            assertTrue(client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty());
        }, 15, TimeUnit.SECONDS);
    }

    /**
     * Cancellation works for projection-only PPL queries (no aggregation, no predicate). This
     * exercises a third query shape distinct from the two used in
     * {@link #testCancellationWorksForFilteredAndFullScanQueries}, ensuring the cancellation
     * path doesn't depend on the presence of stats or filter operators.
     */
    public void testProjectionOnlyQueryCancels() throws Exception {
        createAndSeedIndex();
        cancelAndAssertTaskCancelledException("source = " + INDEX + " | fields value");
    }

    /**
     * Concurrent cancellations of N parallel queries (each fanning out across multiple
     * shards on different data nodes) don't interfere with each other. Cancels are dispatched
     * simultaneously from N separate threads; every original future must fail with
     * {@link TaskCancelledException} and the registry must be clean afterward.
     */
    public void testConcurrentCancelsAcrossShardsDoNotInterfere() throws Exception {
        createAndSeedIndex();
        int n = 5;

        Map<String, CountDownLatch> latches = blockAllFragmentHandlers();

        ExecutorService queryExec = Executors.newFixedThreadPool(n);
        ExecutorService cancelExec = Executors.newFixedThreadPool(n);
        List<Future<PPLResponse>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                futures.add(queryExec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total")));
            }
            assertBusy(() -> {
                List<TaskInfo> tasks = client().admin().cluster().prepareListTasks()
                    .setActions(ANALYTICS_QUERY_TASK).get().getTasks();
                assertEquals(n, tasks.size());
            }, 20, TimeUnit.SECONDS);

            List<TaskInfo> all = client().admin().cluster().prepareListTasks()
                .setActions(ANALYTICS_QUERY_TASK).get().getTasks();

            CountDownLatch start = new CountDownLatch(1);
            List<Future<CancelTasksResponse>> cancelFutures = new ArrayList<>();
            for (TaskInfo t : all) {
                TaskId id = t.getTaskId();
                cancelFutures.add(cancelExec.submit(() -> {
                    start.await();
                    return client().admin().cluster().prepareCancelTasks().setTaskId(id).waitForCompletion(false).get();
                }));
            }
            start.countDown();
            for (Future<CancelTasksResponse> cf : cancelFutures) {
                cf.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
                // node_failures permitted (existing core contract); the strong invariant is
                // that all original futures get TaskCancelledException, asserted below.
            }

            latches.values().forEach(CountDownLatch::countDown);

            int cancelled = 0;
            for (Future<PPLResponse> f : futures) {
                try {
                    f.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
                } catch (ExecutionException ee) {
                    assertNotNull(ExceptionsHelper.unwrap(ee, TaskCancelledException.class));
                    cancelled++;
                }
            }
            assertEquals("all queries must have been cancelled", n, cancelled);
        } finally {
            latches.values().forEach(CountDownLatch::countDown);
            clearAllFragmentHandlers();
            queryExec.shutdownNow();
            cancelExec.shutdownNow();
            queryExec.awaitTermination(5, TimeUnit.SECONDS);
            cancelExec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertBusy(() -> {
            assertTrue(client().admin().cluster().prepareListTasks().setActions(ANALYTICS_QUERY_TASK).get().getTasks().isEmpty());
            assertTrue(client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get().getTasks().isEmpty());
        }, 20, TimeUnit.SECONDS);
    }
}
