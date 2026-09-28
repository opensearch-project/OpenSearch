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
import org.opensearch.ExceptionsHelper;
import org.opensearch.Version;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.action.AnalyticsQueryAction;
import org.opensearch.analytics.exec.action.FetchByRowIdsAction;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
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
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

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
import java.util.concurrent.atomic.AtomicBoolean;

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
            .put("datafusion.spill_directory", createTempDir().toString())
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

    // ---- QTF (query-then-fetch / late-materialization) index ----
    // A `sortkey` to order by + a fetch-only `payload` (index=false) projected ABOVE the sort anchor.
    // `source = QTF_INDEX | sort sortkey | head N | fields payload` makes the LateMaterialization
    // rewriter fire: the query phase emits row-ids, the fetch phase (FetchByRowIdsAction) materializes
    // `payload`. Multi-shard so the rewriter engages.
    private static final String QTF_INDEX = "analytics_qtf_cleanup_idx";
    private static final int QTF_DOCS = 200;

    private void createAndSeedQtfIndex() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
        assertTrue(
            client().admin()
                .indices()
                .prepareCreate(QTF_INDEX)
                .setSettings(indexSettings)
                .setMapping("sortkey", "type=integer", "payload", "type=keyword,index=false")
                .get()
                .isAcknowledged()
        );
        ensureGreen(QTF_INDEX);
        for (int i = 0; i < QTF_DOCS; i++) {
            client().prepareIndex(QTF_INDEX).setSource("sortkey", i, "payload", "row-" + i).get();
        }
        client().admin().indices().prepareRefresh(QTF_INDEX).get();
        client().admin().indices().prepareFlush(QTF_INDEX).get();
        // Force the parquet commit visible before measuring.
        assertBusy(() -> {
            PPLResponse r = executePPL("source = " + QTF_INDEX + " | stats count() as c");
            assertEquals("QTF seed not yet visible", (long) QTF_DOCS, ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue());
        }, 30, TimeUnit.SECONDS);
    }

    /** The QTF query: sort by sortkey, head N, project the fetch-only payload column → fires late materialization. */
    private static String qtfQuery() {
        return "source = " + QTF_INDEX + " | sort sortkey | head 20 | fields payload";
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }

    /** Returns {@code target} if any OpenSearchException in {@code t}'s cause chain reports it, else null. */
    private static RestStatus firstStatus(Throwable t, RestStatus target) {
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            if (c instanceof OpenSearchException ose && ose.status() == target) {
                return ose.status();
            }
        }
        return null;
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
     * The REAL native pool-exhaustion path (no injection): squeeze {@code datafusion.memory_pool_limit_bytes}
     * to 1 so a GROUP BY aggregation trips the native memory pool on the shard. The native error
     * ("Failed to allocate …") is NOT a {@code CircuitBreakingException} object — it is converted to one on
     * the data node by the backend's {@code convertException} SPI ({@code NativeErrorConverter}) before it
     * crosses transport, then tagged RESOURCE_EXHAUSTED ({@code AnalyticsTransportErrors.toWireError}) and
     * rebuilt into a breaker at the coordinator ({@code AnalyticsTransportErrors.fromWireError}). Without the
     * data-node conversion the raw native error crosses as a typeless INTERNAL error and surfaces as HTTP 500
     * ("Stage 0 failed"). This is the path {@code MemoryGuardIT} exercises; here we assert the unwrapped
     * 429 and clean teardown.
     */
    public void testRealNativePoolExhaustionSurfacesAs429AndCleansUp() throws Exception {
        createAndSeedIndex();
        // Squeeze the native pool so any aggregation allocation trips it.
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.memory_pool_limit_bytes", 1L))
                .get()
                .isAcknowledged()
        );
        try {
            Throwable failure = null;
            PPLResponse response = null;
            try {
                response = executePPL("source = " + INDEX + " | stats count() by value", QUERY_TIMEOUT);
            } catch (Throwable t) {
                failure = t;
            }
            assertNotNull("native pool exhaustion must fail the query, not return (response=" + response + ")", failure);
            // The contract is the STATUS (429), not a specific class: a native pool/admission trip is
            // converted on the data node to either a CircuitBreakingException or an OpenSearchStatusException
            // (admission-rejected), both of which report TOO_MANY_REQUESTS. Find the 429-bearing exception
            // anywhere in the surfaced cause chain.
            RestStatus status = firstStatus(failure, RestStatus.TOO_MANY_REQUESTS);
            assertEquals(
                "a native memory-pool trip must surface as HTTP 429 (got " + failure.getClass().getName() + ": " + failure
                    .getMessage() + ")",
                RestStatus.TOO_MANY_REQUESTS,
                status
            );
            assertNoNativeLeak(failure);
        } finally {
            // Reset so other tests / teardown aren't starved.
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull("datafusion.memory_pool_limit_bytes"))
                .get();
        }
        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
    }

    /**
     * Real native pool exhaustion through a SORT/LIMIT (TopK) query on a MULTI-SHARD index — the exact
     * staging shape (high-q07..q10). The shard fragment trips the pool, the breaker crosses stream
     * transport, and must surface to the user as HTTP 429 with NO leaked native allocator dump
     * ("top memory consumers / query_untracked(...) consumed N MB"). Exercises BOTH the shard→coordinator
     * transport path and the cause sanitization.
     */
    public void testShardSortPoolExhaustionSurfacesAs429WithoutLeak() throws Exception {
        createAndSeedIndex();
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.memory_pool_limit_bytes", 1L))
                .get()
                .isAcknowledged()
        );
        try {
            Throwable failure = null;
            PPLResponse response = null;
            try {
                response = executePPL("source = " + INDEX + " | sort value | head 50", QUERY_TIMEOUT);
            } catch (Throwable t) {
                failure = t;
            }
            assertNotNull("shard sort pool exhaustion must fail the query (response=" + response + ")", failure);
            assertEquals(
                "a shard-side native pool trip must surface as HTTP 429 (got " + failure.getClass().getName() + ": " + failure
                    .getMessage() + ")",
                RestStatus.TOO_MANY_REQUESTS,
                firstStatus(failure, RestStatus.TOO_MANY_REQUESTS)
            );
            assertNoNativeLeak(failure);
        } finally {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull("datafusion.memory_pool_limit_bytes"))
                .get();
        }
        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
    }

    /** Flattens an exception's cause chain to a single string (class:message per level) for substring checks. */
    private static String renderedChain(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            sb.append(c.getClass().getName()).append(':').append(c.getMessage()).append('\n');
        }
        return sb.toString();
    }

    /** Asserts no raw native allocator internals leak into the rendered exception chain shown to the user. */
    private static void assertNoNativeLeak(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
            sb.append(c.getClass().getName()).append(':').append(c.getMessage()).append('\n');
        }
        String rendered = sb.toString();
        for (String leak : new String[] {
            "top memory consumers",
            "query_untracked",
            "can spill",
            "already reserved",
            "GroupedHashAggregateStream",
            "RepartitionExec",
            "batch_size",
            "avg_row_bytes" }) {
            assertFalse("native detail '" + leak + "' must not leak to the user, got: " + rendered, rendered.contains(leak));
        }
    }

    /**
     * A shard fragment returning a {@link TaskCancelledException} over stream transport — exactly what
     * Search BackPressure does when it cancels a leaf {@code AnalyticsShardTask} (the bottom-up cancel
     * that does NOT touch the coordinator action) — must tear down the whole query cleanly: the
     * coordinator query task AND the fragment tasks must both unregister. Without the
     * StageExecution attachChildren CANCELLED-propagation fix, a cancelled shard stage strands its
     * parent and the coordinator task leaks (phantom). This is the closest IT analogue of the
     * production SBP-cancel path.
     */
    public void testShardTaskCancelledExceptionTearsDownQueryAndCleansUp() throws Exception {
        createAndSeedIndex();

        // Inject on every data node: with 2 shards / 0 replicas across 2 nodes, allocation may place both
        // shards on one node, so a single-victim injection can miss the fragment and the query succeeds (flake).
        List<MockTransportService> mtsList = new java.util.ArrayList<>();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                // Replacing the handler bypasses the production classify seam (channelResponseHandler.onFailure
                // → AnalyticsTransportErrors.toWireError), so we inject the post-classification wire form: a
                // StreamException(CANCELLED). This exercises the half that crosses the wire + the coordinator's
                // fromWireError, mirroring what production produces when SBP cancels a shard fragment.
                channel.sendResponse(
                    new StreamException(StreamErrorCode.CANCELLED, "task cancelled by search backpressure on " + node)
                );
            });
            mtsList.add(mts);
        }
        try {
            Throwable failure = null;
            PPLResponse response = null;
            try {
                response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            } catch (Throwable t) {
                failure = t;
            }
            // Either it surfaces as a failure (expected) or returns — but it must NOT hang past the
            // timeout, and the surfaced error must reflect cancellation, not a generic 500/hang.
            logger.info(
                "[sbp-cancel] shard TaskCancelledException -> coordinator surfaced: response={}, failure={}",
                response,
                failure == null ? "none" : failure.getClass().getName() + ": " + failure.getMessage()
            );
            assertNotNull("a cancelled shard must fail the query, not silently return a result (response=" + response + ")", failure);
            // Contract: the shard cancellation must reach the coordinator as a recognizable cancellation,
            // NOT the old bare RuntimeException("Stage N failed") ISE a client would retry. There are two
            // valid race outcomes depending on which shard's failure wins:
            //   1. the cancel propagates as a TaskCancelledException (fromWireError rebuilds it), or
            //   2. with multiple shards, the failure cascade cancels a sibling's gRPC stream before its
            //      typed CANCELLED error is flushed (sendError no-ops on an already-cancelled channel), so
            //      that stream surfaces gRPC's generic cancellation teardown ("Internal error [task_id=N]").
            // Both are acceptable; a plain "Stage N failed" with no cancellation signal is the bug.
            boolean isTaskCancelled = ExceptionsHelper.unwrap(failure, TaskCancelledException.class) != null;
            boolean isGrpcCancelTeardown = renderedChain(failure).contains("Internal error [task_id=");
            assertTrue(
                "shard cancellation must surface as TaskCancelledException or a gRPC cancellation teardown, not a generic "
                    + "'Stage N failed' ISE (got chain: " + renderedChain(failure) + ")",
                isTaskCancelled || isGrpcCancelTeardown
            );
        } finally {
            mtsList.forEach(MockTransportService::clearAllRules);
        }
        // The contract that matters for the stuck-task issue: NO phantom coordinator query task, no
        // residual fragment tasks, after a bottom-up shard cancel.
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

        // Block every data node's shard handler so cancellation lands while the query is in-flight,
        // regardless of which node(s) the 2 shards were allocated to.
        CountDownLatch released = new CountDownLatch(1);
        List<MockTransportService> mtsList = new java.util.ArrayList<>();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                try {
                    released.await(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                handler.messageReceived(request, channel, task);
            });
            mtsList.add(mts);
        }

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
            mtsList.forEach(MockTransportService::clearAllRules);
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
    }

    // ---------------------------------------------------------------- QTF (query-then-fetch) tests
    // A QTF query has THREE stage levels (shard fragment → late-materialization → reduce) and a second
    // round of shard work: the fetch phase ({@link FetchByRowIdsAction}). The phantom-task strand the
    // StageExecution attachChildren fix addresses is reachable here in production: a cancelled shard
    // stage (SBP) or a breaker mid-fetch leaves the LateMaterialization stage draining with a cancelled
    // child, stranding the parent unless CANCELLED is propagated. These tests inject failures on BOTH
    // the query phase (FragmentExecutionAction) and the fetch phase (FetchByRowIdsAction), and assert no
    // residual tasks of any of the three actions.

    /**
     * QTF sanity + cleanup. Confirms the late-materialization rewriter actually fires on this index +
     * query shape (we flip a flag from a pass-through {@code FetchByRowIdsAction} behavior — if the
     * rewriter didn't engage, no fetch is dispatched and the assertion fails, telling us the QTF tests
     * below are testing the wrong thing). A successful QTF query must leave no residual query, fragment,
     * or fetch tasks.
     */
    public void testQtfQueryFiresFetchPhaseAndLeavesNoResidualTasks() throws Exception {
        createAndSeedQtfIndex();

        AtomicBoolean fetchDispatched = new AtomicBoolean(false);
        List<MockTransportService> mtsList = new java.util.ArrayList<>();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(FetchByRowIdsAction.NAME, (handler, request, channel, task) -> {
                fetchDispatched.set(true);
                handler.messageReceived(request, channel, task);
            });
            mtsList.add(mts);
        }
        try {
            PPLResponse response = executePPL(qtfQuery(), QUERY_TIMEOUT);
            assertFalse("QTF query must return rows", response.getRows().isEmpty());
            assertTrue(
                "late-materialization rewriter must fire (FetchByRowIdsAction dispatched) — otherwise the QTF "
                    + "cancel/breaker tests below exercise a non-QTF plan",
                fetchDispatched.get()
            );
        } finally {
            mtsList.forEach(MockTransportService::clearAllRules);
        }
        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
        assertNoResidualTasks(FetchByRowIdsAction.NAME);
    }

    /** SBP stand-in on the QTF QUERY phase: a cancelled shard fragment must tear down the whole 3-level QTF tree. */
    public void testQtfFragmentCancelTearsDownAndCleansUp() throws Exception {
        createAndSeedQtfIndex();
        assertQtfInjectedFailureSurfacesAndCleansUp(
            FragmentExecutionAction.NAME,
            (channel, victim) -> channel.sendResponse(new TaskCancelledException("sbp cancel (qtf query phase) on " + victim))
        );
    }

    /** SBP stand-in on the QTF FETCH phase: a cancelled fetch must tear down the whole 3-level QTF tree. */
    public void testQtfFetchCancelTearsDownAndCleansUp() throws Exception {
        createAndSeedQtfIndex();
        assertQtfInjectedFailureSurfacesAndCleansUp(
            FetchByRowIdsAction.NAME,
            (channel, victim) -> channel.sendResponse(new TaskCancelledException("sbp cancel (qtf fetch phase) on " + victim))
        );
    }

    /** Injects {@code injector} on {@code action} of a QTF query, asserts it fails, and verifies no residual tasks. */
    private void assertQtfInjectedFailureSurfacesAndCleansUp(String action, FailureInjector injector) throws Exception {
        // Inject on EVERY data node, not a single random victim: with 2 shards / 0 replicas across 2 nodes,
        // allocation may place both shards on one node, so a single-victim injection can miss every fragment
        // (the query then succeeds and the test flakes). Injecting everywhere fails the fragment wherever it runs.
        List<MockTransportService> mtsList = new java.util.ArrayList<>();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(action, (handler, request, channel, task) -> injector.inject(channel, node));
            mtsList.add(mts);
        }
        try {
            Throwable failure = null;
            PPLResponse response = null;
            try {
                response = executePPL(qtfQuery(), QUERY_TIMEOUT);
            } catch (Throwable t) {
                failure = t;
            }
            logger.info(
                "[qtf-inject] action={} -> response={}, failure={}",
                action,
                response,
                failure == null ? "none" : failure.getClass().getName() + ": " + failure.getMessage()
            );
            assertNotNull("injected failure on " + action + " must fail the QTF query, not return (response=" + response + ")", failure);
        } finally {
            mtsList.forEach(MockTransportService::clearAllRules);
        }
        // The phantom-task contract: no stranded coordinator query task, no residual fragment/fetch tasks.
        assertNoResidualTasks(AnalyticsQueryAction.NAME);
        assertNoResidualTasks(FragmentExecutionAction.NAME);
        assertNoResidualTasks(FetchByRowIdsAction.NAME);
    }

    /** Sends an injected error on a transport channel for the named victim node. */
    @FunctionalInterface
    private interface FailureInjector {
        void inject(org.opensearch.transport.TransportChannel channel, String victim) throws Exception;
    }
}
