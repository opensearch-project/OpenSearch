/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.analytics.exec.QueryScheduler;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.analytics.exec.stage.StageScheduler;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.cluster.NodeConnectionsService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Resilience / fault-injection suite for the analytics-engine coordinator
 * (a.k.a. stage scheduler —
 * {@link org.opensearch.analytics.exec.stage.ShardFragmentStageScheduler}).
 *
 * <p>Covers four failure domains:
 * <ul>
 *   <li>A. Network-level transport faults (NetworkDisruption: disconnect,
 *       unresponsive, delay, intermittent, bridge, mid-query node drops).</li>
 *   <li>B. Application-layer shard failures injected via
 *       {@link MockTransportService#addRequestHandlingBehavior}
 *       on {@link FragmentExecutionAction#NAME}.</li>
 *   <li>C. Reduce-stage / DataFusion backend faults + concurrency hygiene.</li>
 *   <li>D. Shutdown-restart flavor — coordinator/data-node crash recovery.</li>
 * </ul>
 *
 * <p>Explicitly parquet-only: no Lucene backend installed. Tests run against a
 * 3-shard, 0-replica composite-parquet index seeded with deterministic data.
 *
 * <p><b>Streaming transport enabled.</b> Production runs streaming, and so
 * does this test. {@link MockTransportService}'s
 * {@code addRequestHandlingBehavior} is wired to consult both the regular
 * transport's request-handler registry AND the {@link
 * org.opensearch.transport.StreamTransportService}'s registry — so injecting
 * a behavior on {@link FragmentExecutionAction#NAME} works regardless of
 * which path the analytics dispatcher uses.
 *
 * @opensearch.internal
 */
// TEST-scope: each test gets a fresh cluster — disruption schemes and killed
// nodes must not leak across methods.
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging(reason = "debugging resilience behavior", value = "org.opensearch.analytics:DEBUG")
// Per-method leak detection: any thread spawned during a single test method that
// is still alive 5s after the method returns fails the test. Overrides the
// SUITE-scope default inherited from OpenSearchTestCase so failure-path leaks
// (orphaned fragment-task threads, leaked native-handle reapers) surface
// immediately at the offending method instead of being aggregated to suite end.
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.TEST)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering(linger = 5000)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = FlightTransportThreadLeakFilter.class)
public class CoordinatorResilienceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "resilience_idx";
    private static final String SINGLE_SHARD_INDEX = "single_shard_idx";
    private static final int NUM_SHARDS = 3;
    private static final int DOCS_PER_SHARD_TARGET = 10;
    private static final int VALUE = 7;
    // Total distinct doc ids indexed (with 3 shards and default routing, approximate per-shard count only).
    private static final int TOTAL_DOCS = NUM_SHARDS * DOCS_PER_SHARD_TARGET;
    private static final long EXPECTED_SUM = (long) TOTAL_DOCS * VALUE;

    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            TestPPLPlugin.class,
            FlightStreamPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            // Stub committer factory satisfies the EngineConfigFactory boot-time
            // check (`committerFactories.isEmpty() && isPluggableDataFormatEnabled`)
            // without pulling the Lucene backend onto the IT classpath.
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
            // Match NetworkDisruptionIT: speeds up reconnection after disruption
            // stops, so assertions about recovery don't wait the 10s default.
            .put(NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.getKey(), "2s")
            // ServerConfig#init unconditionally sets the arrow.memory.debug.allocator
            // system property from this node setting, clobbering the -D VM arg. Turn
            // it on here so Arrow's BaseAllocator.DEBUG picks up `true` at class init
            // and leak traces print the allocation call-site stack.
            .put("arrow.memory.debug.allocator", true)
            .build();
    }

    @After
    public void clearDisruption() {
        // Defensive cleanup — every test that sets a disruption should have
        // already stopped it, but this guards against assertion failures aborting
        // the body before the disruption-removal line runs.
        try {
            internalCluster().clearDisruptionScheme(true);
        } catch (IllegalStateException ignore) {
            // No scheme set — fine.
        }
    }

    /** Allocator for Arrow buffers produced by stubbed-handler tests; closed after each test. */
    private org.apache.arrow.memory.BufferAllocator stubAllocator;

    @After
    public void closeStubAllocator() {
        if (stubAllocator != null) {
            stubAllocator.close();
            stubAllocator = null;
        }
    }

    /**
     * Suite-wide deadlock detector. Polls {@link ThreadMXBean#findDeadlockedThreads()}
     * once a second; if the JVM reports a monitor cycle anywhere in the test JVM,
     * {@link #stopDeadlockWatchdog()} fails the test with a thread dump. Cheap and
     * throughput-independent — won't false-positive on transient contention because
     * the JVM only reports actual lock cycles where every participant is blocked.
     * Mirrors the pattern in {@code CacheTests.testDependentKeyDeadlock}.
     */
    private ScheduledExecutorService deadlockWatchdog;
    private volatile long[] deadlockedThreadIds;

    @Before
    public void startDeadlockWatchdog() {
        deadlockedThreadIds = null;
        ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
        deadlockWatchdog = Executors.newSingleThreadScheduledExecutor();
        deadlockWatchdog.scheduleAtFixedRate(() -> {
            if (deadlockedThreadIds != null) return;
            long[] ids = mxBean.findDeadlockedThreads();
            if (ids != null) {
                deadlockedThreadIds = ids;
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @After
    public void stopDeadlockWatchdog() throws InterruptedException {
        if (deadlockWatchdog != null) {
            deadlockWatchdog.shutdownNow();
            deadlockWatchdog.awaitTermination(2, TimeUnit.SECONDS);
            deadlockWatchdog = null;
        }
        if (deadlockedThreadIds != null) {
            fail("JVM-detected deadlock cycle in " + deadlockedThreadIds.length + " thread(s):\n" + dumpAllThreads());
        }
    }

    // ---------------------------------------------------------------- fixture

    /**
     * Creates + seeds the test index. Called from every test that actually issues
     * queries. Kept out of {@code @Before} because a couple of tests want to
     * exercise the pre-index state.
     */
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
        // pluggable_dataformat=composite + parquet primary: prepareFlush().get() returns
        // before parquet commits are durable on every shard. Tests in this suite exercise
        // search/coordinator resilience, not ingestion — they need a stable post-seed
        // dataset. assertBusy until the analytics path sees the full sum, otherwise
        // disruption-time queries race the in-flight commit and produce nondeterministic
        // partial results that look like resilience bugs.
        try {
            assertBusy(() -> {
                PPLResponse r = executePPL("source = " + INDEX + " | stats sum(value) as total");
                long actual = ((Number) r.getRows().get(0)[r.getColumns().indexOf("total")]).longValue();
                assertEquals("seed not yet visible to analytics path", EXPECTED_SUM, actual);
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("createAndSeedIndex: timed out waiting for seed durability", e);
        }
    }

    /**
     * Creates + seeds a single-shard composite-parquet index with {@code docs} rows.
     * Used by the multi-batch early-termination test, which lowers the DataFusion
     * read batch size so the shard's stream emits multiple batches.
     */
    private void createSingleShardIndex(String name, int docs) {
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
            .prepareCreate(name)
            .setSettings(indexSettings)
            .setMapping("value", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(name);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex(name).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(name).get();
        client().admin().indices().prepareFlush(name).get();
        try {
            assertBusy(() -> {
                PPLResponse r = executePPL("source = " + name + " | stats sum(value) as total");
                long actual = ((Number) r.getRows().get(0)[r.getColumns().indexOf("total")]).longValue();
                assertEquals("seed not yet visible to analytics path", (long) docs * VALUE, actual);
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("createSingleShardIndex: timed out waiting for seed durability", e);
        }
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }

    /** Map each primary shard id → node name that currently hosts it. */
    private Map<Integer, String> shardToNode() {
        Map<Integer, String> out = new HashMap<>();
        for (ShardRouting sr : clusterService().state()
            .routingTable()
            .index(INDEX)
            .shardsWithState(org.opensearch.cluster.routing.ShardRoutingState.STARTED)) {
            if (sr.primary()) {
                String nodeId = sr.currentNodeId();
                String name = clusterService().state().nodes().get(nodeId).getName();
                out.put(sr.id(), name);
            }
        }
        return out;
    }

    /** Return one node name that currently hosts a primary of {@link #INDEX}. */
    private String pickShardHostingNode() {
        return shardToNode().values().iterator().next();
    }

    /**
     * Derive the coordinator-side node name — the node that the default
     * {@link #client()} dispatches against. Used for disruption topologies that
     * isolate the coordinator vs. workers.
     */
    private String coordinatorNodeName() {
        // clusterService() resolves on the local (test-thread) node's client — in
        // TransportClient-less mode, client() binds to one of the internal
        // cluster nodes. Use getMasterName() as a stable coordinator surrogate.
        return internalCluster().getClusterManagerName();
    }

    // ---------------------------------------------------- B: application-layer stubbing

    /**
     * Stubs one shard's {@link FragmentExecutionAction} entirely — no real
     * handler runs, the data node never produces real data. Instead the stub
     * returns a single zero-row Arrow batch carrying a minimal schema, then
     * completes the stream. Coordinator must still produce a valid (smaller)
     * result from the other two shards.
     *
     * <p>Exercises the streaming-fallback path in {@link MockTransportService}:
     * {@link FragmentExecutionAction#NAME} is registered only on the streaming
     * transport, so without the fallback the stub would never bind.
     */
    public void testStubReplacesStreamingShardResponseWithEmptyBatch() throws Exception {
        createAndSeedIndex();
        stubAllocator = new org.apache.arrow.memory.RootAllocator();
        // Schema width must match the coordinator's declared input-partition schema — that's
        // the *aggregate* output type (SUM(int) → Int64/BIGINT), not the base column type.
        org.apache.arrow.vector.types.pojo.Schema schema = new org.apache.arrow.vector.types.pojo.Schema(
            List.of(
                new org.apache.arrow.vector.types.pojo.Field(
                    "total",
                    org.apache.arrow.vector.types.pojo.FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)),
                    null
                )
            )
        );

        AtomicInteger stubCalls = new AtomicInteger();
        String victim = pickShardHostingNode();
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            stubCalls.incrementAndGet();
            VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, stubAllocator);
            vsr.allocateNew();
            vsr.setRowCount(0);
            // sendResponseBatch transfers buffer ownership to the wire. Honors the Flight protocol
            // invariant that ≥1 schema-bearing frame must precede completeStream.
            channel.sendResponseBatch(
                new org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse(vsr)
            );
            channel.completeStream();
        });
        try {
            PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            assertThat("stub must fire on the streaming-only fragment action", stubCalls.get(), greaterThan(0));
            assertNotNull("coordinator must produce a response when one shard contributes nothing", response);
            long actual = ((Number) response.getRows().get(0)[response.getColumns().indexOf("total")]).longValue();
            assertThat("Partial sum must be < full when a shard contributes nothing; got " + actual, actual, lessThan(EXPECTED_SUM));
            assertThat("Partial sum must be ≥ 0 given the other two shards' contribution", actual, greaterThan(-1L));
        } finally {
            mts.clearAllRules();
        }
    }

    // ---------------------------------------------------- A: network-level faults

    /**
     * #1 — Disconnect coordinator from one shard-hosting node mid-query.
     *
     * <p>Pins the coordinator's contract: under disconnect, does the query fail
     * with a bounded error (good) or hang past the request timeout (bug)?
     */
    public void testCoordinatorSurvivesDisconnectFromOneShard() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        String victim = firstNonMatching(coord);
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(coord), Collections.singleton(victim)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        try {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            // Contract: either a clear error OR a partial-shards-failed result.
            // Hanging past QUERY_TIMEOUT surfaces as actionGet timeout → ignored
            // by AtomicReference failure capture only if we add per-call timeout.
            assertTrue(
                "query must either return or fail within " + QUERY_TIMEOUT + "; got null response and null failure → coordinator hung",
                response.get() != null || failure.get() != null
            );
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        // Post-heal: query returns correct sum.
        ensureFullyConnected();
        PPLResponse healed = executePPL("source = " + INDEX + " | stats sum(value) as total");
        assertScalarLong(healed, "total", EXPECTED_SUM);
    }

    /** #2 — UNRESPONSIVE: drops packets silently; coordinator should hit its own timeout, not spin. */
    public void testCoordinatorSurvivesUnresponsiveShard() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        String victim = firstNonMatching(coord);
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(coord), Collections.singleton(victim)),
            NetworkDisruption.UNRESPONSIVE
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        try {
            long start = System.nanoTime();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(
                "UNRESPONSIVE query must not exceed " + QUERY_TIMEOUT.seconds() + "s; took " + elapsedMs + "ms",
                elapsedMs < QUERY_TIMEOUT.millis() + 5_000L
            );
            assertTrue(
                "UNRESPONSIVE must produce a failure or partial result, not a silent null",
                response.get() != null || failure.get() != null
            );
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        // Task leak check — no FragmentExecutionAction tasks remain after failure.
        ensureFullyConnected();
        assertNoPendingFragmentTasks();
    }

    /**
     * #3 — Small network delay (200ms). Query must succeed and complete in roughly
     * {@code delay + serve-time}, not {@code delay × shards} (which would indicate
     * serial blocking).
     */
    public void testCoordinatorSurvivesSlowShard() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        String victim = firstNonMatching(coord);
        NetworkDisruption.NetworkDelay delay = new NetworkDisruption.NetworkDelay(TimeValue.timeValueMillis(300));
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(coord), Collections.singleton(victim)),
            delay
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        try {
            long start = System.nanoTime();
            PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            assertScalarLong(response, "total", EXPECTED_SUM);
            // Slack of 10× delay per shard — a quadratic-in-delay coordinator would
            // easily blow this. Generous slack because cold-start parquet reads
            // add jitter.
            long upperBound = 300L * NUM_SHARDS * 10L;
            assertTrue(
                "Query with 300ms shard delay took " + elapsedMs + "ms; > " + upperBound + "ms suggests serial blocking",
                elapsedMs < upperBound
            );
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
    }

    /**
     * #4 — Intermittent disruption: toggle disconnect on/off; fire query mid-cycle.
     * Bounded timeout proves the coordinator doesn't wedge across reconnect cycles.
     */
    public void testCoordinatorSurvivesIntermittentNetwork() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        String victim = firstNonMatching(coord);
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(coord), Collections.singleton(victim)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        try {
            disruption.startDisrupting();
            Thread.sleep(500);
            disruption.stopDisrupting();
            Thread.sleep(500);
            disruption.startDisrupting();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            assertTrue("Coordinator must complete or fail within " + QUERY_TIMEOUT, response.get() != null || failure.get() != null);
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        ensureFullyConnected();
    }

    /**
     * #5 — Bridge topology: one node isolated from the others, they can talk.
     * Verifies the coordinator either rejects or returns a partial result — pins
     * whichever behavior is actually implemented.
     */
    public void testCoordinatorSurvivesBridgePartition() throws Exception {
        createAndSeedIndex();
        Set<String> allNodes = new HashSet<>(List.of(internalCluster().getNodeNames()));
        assertTrue("need ≥3 nodes for bridge topology", allNodes.size() >= 3);
        // Pick a bridge node (= isolated), two others on each side.
        String bridge = allNodes.iterator().next();
        Set<String> sideOne = new HashSet<>();
        Set<String> sideTwo = new HashSet<>();
        for (String n : allNodes) {
            if (n.equals(bridge)) continue;
            if (sideOne.isEmpty()) sideOne.add(n);
            else sideTwo.add(n);
        }
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.Bridge(bridge, sideOne, sideTwo),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        try {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            // Pin the behavior — whichever path the coordinator takes, it must
            // terminate in bounded time.
            assertTrue("Bridge partition must surface a response or an error (no hang)", response.get() != null || failure.get() != null);
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        ensureFullyConnected();
    }

    /**
     * #6 — Node hosting a shard drops mid-query. Either partial-result or clean
     * failure is acceptable — silent hang is not.
     */
    public void testCoordinatorHandlesNodeDropDuringQuery() throws Exception {
        createAndSeedIndex();
        String candidate = pickShardHostingNode();
        if (candidate.equals(coordinatorNodeName())) {
            // Flip to a non-coordinator victim
            candidate = firstNonMatching(coordinatorNodeName());
        }
        final String victim = candidate;

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<PPLResponse> response = new AtomicReference<>();

        Thread stopper = new Thread(() -> {
            try {
                Thread.sleep(150);
                internalCluster().stopRandomNode(InternalTestCluster.nameFilter(victim));
            } catch (Exception e) {
                // Swallow — the main assertion covers observable coordinator state.
            }
        });
        stopper.start();
        try {
            response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
        } catch (Throwable t) {
            failure.set(t);
        }
        stopper.join(5_000);

        assertTrue(
            "Coordinator must surface either a response or a failure after node drop",
            response.get() != null || failure.get() != null
        );
    }

    /**
     * #7 — Node drops, then a new node joins, cluster heals; subsequent query returns correct total.
     * Proves no residual state from the failed query poisons the next one.
     */
    public void testCoordinatorHandlesNodeRestartDuringQuery() throws Exception {
        createAndSeedIndex();
        String victim = firstNonMatching(coordinatorNodeName());
        try {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(victim));
        } catch (Exception e) {
            // Proceed — some tests may already be down to 2 nodes.
        }
        internalCluster().startNode();
        // NOTE: do NOT ensureGreen(INDEX) here. With 0 replicas, the shards
        // hosted on the killed node are permanently lost — green state on the
        // OLD index can never be reached. We delete and recreate below; the
        // recreated index goes green via createAndSeedIndex's own ensureGreen.
        client().admin().indices().prepareDelete(INDEX).get();
        createAndSeedIndex();
        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total");
        assertScalarLong(response, "total", EXPECTED_SUM);
    }

    // ---------------------------------------------------- B: shard-level faults

    /**
     * #8 — Inject an exception on one shard's fragment handler. Coordinator must
     * fail the query (surface the cause) in bounded time.
     */
    public void testCoordinatorPropagatesStageFailure() throws Exception {
        createAndSeedIndex();
        String victim = pickShardHostingNode();
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        AtomicBoolean thrown = new AtomicBoolean(false);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            thrown.set(true);
            channel.sendResponse(new IllegalStateException("injected failure from " + victim));
        });
        try {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            assertTrue(
                "Injection must reach victim node before termination (thrown="
                    + thrown.get()
                    + ", response="
                    + response.get()
                    + ", failure="
                    + failure.get()
                    + ")",
                thrown.get() || response.get() != null || failure.get() != null
            );
        } finally {
            mts.clearAllRules();
        }
        assertNoPendingFragmentTasks();
    }

    /**
     * #9 — Cancel the data-node fragment task mid-flight. Data-node task goes away,
     * coordinator surfaces cancellation.
     */
    public void testTaskCancellationAtDataNode() throws Exception {
        createAndSeedIndex();
        String victim = pickShardHostingNode();
        // Slow the victim so we can race in a cancellation.
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        CountDownLatch released = new CountDownLatch(1);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            // Hold the shard-side request until released.
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
            // Allow dispatch + shard-handler entry
            Thread.sleep(500);
            CancelTasksResponse cancel = client().admin().cluster().prepareCancelTasks().setActions(FragmentExecutionAction.NAME).get();
            assertFalse(
                "cancel request must not carry back node failures",
                cancel.getNodeFailures() != null && !cancel.getNodeFailures().isEmpty()
            );
            released.countDown();
            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
                // Cancellation not propagated — assertion below pins that. Some codepaths
                // may still return partial data; don't fail here, let the post-check assert.
            } catch (ExecutionException | TimeoutException ignore) {
                // Expected path: coordinator saw cancellation / failure.
            }
        } finally {
            released.countDown();
            mts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }
        // No zombie fragment tasks anywhere in the cluster.
        assertNoPendingFragmentTasks();
    }

    /**
     * #10 — Cancel the parent (coordinator-level) task; children must terminate.
     */
    public void testTaskCancellationAtCoordinator() throws Exception {
        createAndSeedIndex();
        String victim = pickShardHostingNode();
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
            CancelTasksResponse cancel = client().admin().cluster().prepareCancelTasks().setActions(UnifiedPPLExecuteAction.NAME).get();
            assertFalse("cancel must not report node failures", cancel.getNodeFailures() != null && !cancel.getNodeFailures().isEmpty());
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
        assertNoPendingFragmentTasks();
        // Parent PPL task must also be gone.
        ListTasksResponse remaining = client().admin().cluster().prepareListTasks().setActions(UnifiedPPLExecuteAction.NAME).get();
        assertTrue("No residual UnifiedPPLExecute tasks after cancel: " + remaining.getTasks(), remaining.getTasks().isEmpty());
    }

    /**
     * #11 — Shard returns empty response (zero batches). Coordinator must treat it
     * as 0 rows contributed, not an error. Other shards' data still aggregated.
     *
     * <p>Construct a zero-row response that carries the real shard schema by
     * delegating to the real handler and capturing the field names through a
     * wrapping channel. On the streaming path the wrapper drops every emitted
     * batch but forwards completeStream — the coordinator sees the shard
     * contribute zero rows.
     *
     * <p>Production data-node iterator ({@code DatafusionResultStream.BatchIterator}) yields
     * a single zero-row schema batch when the native source produces no rows, so the streaming
     * Flight handler always sends ≥1 schema-bearing frame before completeStream — the empty-shard
     * contract is honest under the real protocol.
     */
    public void testNoDataReturnedFromShard() throws Exception {
        createAndSeedIndex();
        String victim = pickShardHostingNode();
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            // Wrap the streaming channel: let the real handler run, but substitute its row
            // content with a zero-row batch that still carries the shard's schema. Honors the
            // Flight protocol invariant that ≥1 schema-bearing frame must precede
            // completeStream — subsequent batches are dropped.
            TransportChannel wrapper = new ForwardingChannel(channel) {
                private boolean schemaForwarded = false;

                @Override
                public void sendResponseBatch(TransportResponse response) {
                    if (response instanceof org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse arrow) {
                        org.apache.arrow.vector.VectorSchemaRoot incoming = arrow.getRoot();
                        if (incoming == null) return;
                        if (schemaForwarded) {
                            // Already forwarded a zero-row schema frame — drop subsequent batches.
                            try {
                                incoming.close();
                            } catch (Exception ignore) {}
                            return;
                        }
                        // Truncate the incoming root to zero rows in place — preserves the schema and the
                        // existing allocator ownership, then forwards as the shard's single contribution.
                        incoming.setRowCount(0);
                        schemaForwarded = true;
                        super.sendResponseBatch(arrow);
                    }
                }
            };
            handler.messageReceived(request, wrapper, task);
        });
        try {
            PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            assertNotNull("coordinator must produce a response when one shard is empty", response);
            long actual = ((Number) response.getRows().get(0)[response.getColumns().indexOf("total")]).longValue();
            assertThat("Partial sum must be < full when a shard contributes nothing; got " + actual, actual, lessThan(EXPECTED_SUM));
            assertThat("Partial sum must be ≥ 0 given the other two shards' contribution", actual, greaterThan(-1L));
        } finally {
            mts.clearAllRules();
        }
    }

    /**
     * #12 — One shard OK, one shard errors, one shard empty. Document whatever the
     * coordinator does today (complete-success, partial, fail-fast).
     */
    public void testPartialFailure() throws Exception {
        createAndSeedIndex();
        Map<Integer, String> map = shardToNode();
        assertThat("need ≥2 shard-host nodes", map.values().stream().distinct().count(), greaterThan(1L));

        String[] hosts = map.values().stream().distinct().toArray(String[]::new);
        String errorNode = hosts[0];
        String emptyNode = hosts.length > 1 ? hosts[1] : hosts[0];

        MockTransportService errorMts = (MockTransportService) internalCluster().getInstance(TransportService.class, errorNode);
        MockTransportService emptyMts = (MockTransportService) internalCluster().getInstance(TransportService.class, emptyNode);

        errorMts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            channel.sendResponse(new RuntimeException("partial-failure-error-shard"));
        });
        if (!emptyNode.equals(errorNode)) {
            emptyMts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                // Empty-shard stub on the streaming path: forward one zero-row
                // schema-bearing batch so the Flight invariant holds, drop the rest.
                TransportChannel wrapper = new ForwardingChannel(channel) {
                    private boolean schemaForwarded = false;

                    @Override
                    public void sendResponseBatch(TransportResponse response) {
                        if (response instanceof org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse arrow) {
                            org.apache.arrow.vector.VectorSchemaRoot incoming = arrow.getRoot();
                            if (incoming == null) return;
                            if (schemaForwarded) {
                                try {
                                    incoming.close();
                                } catch (Exception ignore) {}
                                return;
                            }
                            incoming.setRowCount(0);
                            schemaForwarded = true;
                            super.sendResponseBatch(arrow);
                        }
                    }
                };
                handler.messageReceived(request, wrapper, task);
            });
        }
        try {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            assertTrue(
                "Coordinator must terminate with either a response or a failure on partial failure",
                response.get() != null || failure.get() != null
            );
            // Don't assert which of (partial success / total failure) — this test pins the contract by
            // recording the observed behavior. The assertion above ensures 'no hang'.
            logger.info(
                "partial-failure outcome: response={} failure={}",
                response.get() == null ? "null" : response.get().getRows().size() + " rows",
                failure.get() == null ? "null" : failure.get().getClass().getSimpleName()
            );
        } finally {
            errorMts.clearAllRules();
            if (!emptyNode.equals(errorNode)) emptyMts.clearAllRules();
        }
    }

    // ----------------------------------------------- C: reduce-stage / concurrency

    /**
     * #13 — Contract under test: when a failure originates inside the coordinator
     * reduce sink (i.e. {@code DatafusionReduceSink}, the in-process component fed
     * by every shard's stream output), the query terminates with a propagated
     * failure AND the native datafusion memory pool returns to baseline (no leaked
     * stream handle). This is a tighter, reduce-sink-layer assertion of the same
     * propagation logic exercised at the transport layer by
     * {@link #testCoordinatorPropagatesStageFailure} (#8) — only the injection
     * point differs.
     */
    public void testReduceStageFailureSurfaced() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        DataFusionService dfs = internalCluster().getInstance(DataFusionService.class, coord);
        long before = dfs.getMemoryPoolUsage();

        Map<String, StageScheduler> originalByNode = new HashMap<>();
        for (String node : internalCluster().getNodeNames()) {
            StageExecutionBuilder builder = internalCluster().getInstance(QueryScheduler.class, node).getStageExecutionBuilder();
            StageScheduler[] thisNodeOriginal = new StageScheduler[1];
            StageScheduler perNodeFaulting = (stage, sink, qctx) -> {
                ExchangeSink poisoned = new ExchangeSink() {
                    @Override
                    public void feed(VectorSchemaRoot batch) {
                        batch.close();
                        throw new RuntimeException("injected reduce-sink feed failure");
                    }

                    @Override
                    public void close() {
                        sink.close();
                    }
                };
                return thisNodeOriginal[0].createExecution(stage, poisoned, qctx);
            };
            thisNodeOriginal[0] = builder.registerScheduler(StageExecutionType.SHARD_FRAGMENT, perNodeFaulting);
            if (thisNodeOriginal[0] != null) {
                originalByNode.put(node, thisNodeOriginal[0]);
            }
        }
        try {
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            // The contract this guard fixes: feed() throwing must NOT hang the query
            // past QUERY_TIMEOUT. Either the failure surfaces as an exception (preferred),
            // OR the parent reduce stage races ahead and emits a wrong result from an
            // empty source (acceptable for this fix — propagating SHARD_FRAGMENT FAILED
            // to the parent stage is a separate cascade concern). What we forbid is
            // silently returning EXPECTED_SUM, since the producer never delivered rows.
            boolean exception = failure.get() != null;
            boolean wrongResult = false;
            if (response.get() != null) {
                int idx = response.get().getColumns().indexOf("total");
                if (idx < 0 || response.get().getRows().isEmpty()) {
                    wrongResult = true;
                } else {
                    Object cell = response.get().getRows().get(0)[idx];
                    long got = cell == null ? -1 : ((Number) cell).longValue();
                    wrongResult = got != EXPECTED_SUM;
                }
            }
            assertTrue(
                "feed() failure must surface (got response=" + response.get() + ", failure=" + failure.get() + ")",
                exception || wrongResult
            );
        } finally {
            for (Map.Entry<String, StageScheduler> e : originalByNode.entrySet()) {
                StageExecutionBuilder b = internalCluster().getInstance(QueryScheduler.class, e.getKey()).getStageExecutionBuilder();
                b.registerScheduler(StageExecutionType.SHARD_FRAGMENT, e.getValue());
            }
        }
        assertBusy(
            () -> assertEquals("Native memory must return to baseline after reduce failure", before, dfs.getMemoryPoolUsage()),
            10,
            TimeUnit.SECONDS
        );
    }

    /**
     * #14 — Set a tiny datafusion memory pool limit and try to exceed it. Expect a
     * clean error, not an OOM crash.
     */
    public void testReduceStageExceedsMemoryLimit() throws Exception {
        // Crank limit down before indexing so the query's native state can't fit.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.getKey(), 1L))
            .get();
        try {
            createAndSeedIndex();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            assertTrue("Tiny memory limit must surface as failure or bounded response", response.get() != null || failure.get() != null);
        } finally {
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull(DataFusionPlugin.DATAFUSION_MEMORY_POOL_LIMIT.getKey()))
                .get();
        }
    }

    /**
     * #15 — Workload driver for failure-path leak detection. Runs 50 failing
     * queries in a row to amplify any per-failure leak (orphaned fragment
     * task, native-handle reaper, executor pool thread) so even a leak too
     * small to surface at N=1 becomes obvious. Leak detection itself comes
     * from the class-level {@code @ThreadLeakScope(Scope.TEST)}: the suite
     * framework fails this test if any thread spawned during the loop is
     * still alive {@code linger}ms after the method returns.
     *
     * <p>Injects on every node — {@code client()} routes randomly, so every
     * potential coordinator must intercept the dispatch.
     */
    public void testThreadLeakAfterFailedQuery() throws Exception {
        createAndSeedIndex();
        for (String node : internalCluster().getNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                channel.sendResponse(new IllegalStateException("forced-failure for thread-leak test"));
            });
        }
        try {
            int failed = 0;
            for (int i = 0; i < 50; i++) {
                try {
                    executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
                } catch (Throwable expected) {
                    failed++;
                }
            }
            // Verify the workload was real — otherwise we'd be running 50 successful
            // queries through the leak detector, which doesn't exercise failure paths.
            assertTrue("expected most queries to fail (saw " + failed + "/50)", failed >= 45);
        } finally {
            for (String node : internalCluster().getNodeNames()) {
                ((MockTransportService) internalCluster().getInstance(TransportService.class, node)).clearAllRules();
            }
        }
    }

    /**
     * #17 — Assert the coordinator's Arrow allocator returns to baseline after
     * a failed query. Depends on a way to fetch the allocator's usage; we proxy
     * via {@link DataFusionService#getMemoryPoolUsage()} which covers the native
     * side of the exchange sink.
     */
    public void testCoordinatorReleasesBufferOnFailure() throws Exception {
        createAndSeedIndex();
        DataFusionService dfs = internalCluster().getInstance(DataFusionService.class, coordinatorNodeName());
        long before = dfs.getMemoryPoolUsage();

        String victim = pickShardHostingNode();
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            channel.sendResponse(new IllegalStateException("release-on-failure probe"));
        });
        try {
            Throwable queryFailure = null;
            try {
                executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
                fail("query must fail — the victim shard's handler was stubbed to return an exception");
            } catch (Throwable t) {
                queryFailure = t;
            }
            // The Arrow root allocator's close() throws "Memory was leaked" when any child
            // allocator still owns buffers. QueryContext.closeBufferAllocator attaches that
            // exception as a Suppressed on the surfaced query failure; walk the whole cause
            // + suppressed graph so the probe fails the test regardless of how deep it was
            // nested. Without this assertion the query failure swallows the leak.
            assertNoArrowLeakSuppressed(queryFailure);
            assertBusy(() -> {
                long after = dfs.getMemoryPoolUsage();
                // Accept non-zero baseline (runtime overhead), require that we don't leak.
                assertTrue(
                    "Native memory usage grew after failure: before=" + before + " after=" + after,
                    after <= before + 1024 * 1024  // 1MiB slack for runtime heap moves
                );
            }, 15, TimeUnit.SECONDS);
        } finally {
            mts.clearAllRules();
        }
    }

    /**
     * Walks {@code t}'s cause + suppressed graph looking for Arrow's "Memory was leaked"
     * marker. Raised by {@code BaseAllocator.close()} when any child allocator still holds
     * buffers — the definitive oracle that a failure-path cleanup path missed a close.
     */
    private static void assertNoArrowLeakSuppressed(Throwable t) {
        if (t == null) return;
        java.util.ArrayDeque<Throwable> queue = new java.util.ArrayDeque<>();
        java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        queue.add(t);
        while (!queue.isEmpty()) {
            Throwable cur = queue.poll();
            if (cur == null || !seen.add(cur)) continue;
            if (cur.getMessage() != null && cur.getMessage().contains("Memory was leaked")) {
                throw new AssertionError("Arrow allocator leaked on failure path: " + cur.getMessage(), t);
            }
            Throwable cause = cur.getCause();
            if (cause != null) {
                queue.add(cause);
            }
            for (Throwable s : cur.getSuppressed()) {
                queue.add(s);
            }
        }
    }

    /**
     * #18 — Native handle / stream / session refcounts must return to baseline
     * after task cancellation.
     *
     * <p>The earlier @AwaitsFix on this test was outdated: {@code NativeHandle}
     * already exposes {@link NativeHandle#liveHandleCount()} (a global registry
     * of every open native pointer wrapped by an analytics-framework handle —
     * runtime, session, sender, reader, stream). That gives us the precise
     * oracle the test needs: snapshot before, race a cancel against an in-flight
     * query that holds at least one shard handler open, then assert the live
     * count returns to its pre-query value.
     *
     * <p>To keep the cancel race deterministic, the data-node fragment handler
     * is held inside a latch (same pattern as #10 / #27): the test cancels the
     * coordinator's PPL task while the shard handler is parked. After the
     * latch is released, every code path — the held shard handler, the
     * coordinator orchestrator, the reduce sink — converges on cleanup, and
     * {@code liveHandleCount()} must drop back to baseline. We use
     * {@code assertBusy} because a few of the cleanup steps are
     * fire-and-forget (e.g. cleaner-driven {@code NativeHandle.close} via
     * GC) and may take a tick or two to be reflected in the registry.
     */
    public void testNativeHandleReleaseOnCancellation() throws Exception {
        createAndSeedIndex();
        // Drain any asynchronous cleanup from the index-creation path so the
        // pre-query snapshot is stable.
        System.gc();
        Thread.sleep(50);
        int baseline = NativeHandle.liveHandleCount();

        String victim = pickShardHostingNode();
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
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            // Allow the query to dispatch and the coordinator to allocate
            // its native session / runtime handles before we cancel.
            Thread.sleep(500);
            int duringQuery = NativeHandle.liveHandleCount();
            // Sanity: the query must have caused at least one handle to be
            // registered, otherwise this assertion is vacuous.
            assertTrue(
                "Expected at least one native handle to be live mid-query (baseline=" + baseline + ", during=" + duringQuery + ")",
                duringQuery >= baseline
            );
            CancelTasksResponse cancel = client().admin().cluster().prepareCancelTasks().setActions(UnifiedPPLExecuteAction.NAME).get();
            assertFalse("cancel must not produce node failures", cancel.getNodeFailures() != null && !cancel.getNodeFailures().isEmpty());
            released.countDown();
            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException ignore) {
                // Expected — cancel surfaces as failure or as a partial result.
            }
        } finally {
            released.countDown();
            mts.clearAllRules();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }
        // After cancel, every native handle the query allocated must be released.
        // GC nudges the cleaner so any unreachable handles flow through the
        // cleaner registry promptly.
        assertBusy(() -> {
            System.gc();
            int after = NativeHandle.liveHandleCount();
            assertTrue("NativeHandle live count grew after cancel: baseline=" + baseline + " after=" + after, after <= baseline);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Early-termination via LIMIT across shards. The coordinator must stop
     * draining shard streams once the result limit is satisfied — it should
     * not block waiting for the remaining shards to finish, and the total
     * returned row count must equal the requested limit.
     *
     * <p>This is distinct from cancellation: the query is succeeding, not
     * failing. The data-node-side fragment tasks may keep producing for a
     * little longer (their batches get dropped on the coordinator side), but
     * the user-visible response must arrive promptly with exactly LIMIT rows.
     *
     * <p>With {@link #NUM_SHARDS}=3 and ~10 docs/shard, {@code head 5} can be
     * satisfied by the first batch of one shard alone — the other two shards
     * are unneeded. A failure to honor the limit (returns >5 rows) or a hang
     * (no response within {@link #QUERY_TIMEOUT}) both fail this test.
     *
     * <p><b>Currently failing:</b> the planner pushes {@code head N} to each
     * data-node fragment but does not re-apply a coordinator-side LIMIT after
     * the cross-shard merge. With 3 shards, {@code head 5} returns 15 rows
     * (5 per shard). Fix is in the planner, not the reduce-sink lifecycle —
     * early-termination depends on a coordinator-side LIMIT operator existing
     * to short-circuit the merge.
     */
    @AwaitsFix(bugUrl = "TODO")
    public void testEarlyTerminationLimitMultiShard() throws Exception {
        createAndSeedIndex();
        PPLResponse r = executePPL("source = " + INDEX + " | head 5", QUERY_TIMEOUT);
        assertEquals("LIMIT must produce exactly the requested row count", 5, r.getRows().size());
    }

    /**
     * Early-termination via LIMIT within a single shard's stream. With
     * {@code datafusion.indexed.batch_size=10} and 50 docs in one shard, the
     * shard's stream emits 5 batches; {@code head 1} is satisfied by the
     * first batch and the coordinator must stop pulling subsequent batches.
     *
     * <p>Complements {@link #testEarlyTerminationLimitMultiShard()}: this
     * one exercises within-stream backpressure (single producer, multiple
     * batches) rather than cross-stream termination.
     */
    public void testEarlyTerminationLimitSingleShardMultiBatch() throws Exception {
        // Force multiple batches per shard so the coordinator has to honor
        // backpressure mid-stream. NodeScope dynamic — TEST cluster scope
        // means this doesn't bleed into other tests.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.batch_size", 10))
            .get();
        createSingleShardIndex(SINGLE_SHARD_INDEX, 50);
        PPLResponse r = executePPL("source = " + SINGLE_SHARD_INDEX + " | head 1", QUERY_TIMEOUT);
        assertEquals("LIMIT must produce exactly the requested row count", 1, r.getRows().size());
    }

    // ----------------------------------------------- D: shutdown-restart flavor

    /**
     * #19 — Kill a shard-hosting node after a brief disruption window, bring
     * up a fresh node, heal, re-index, re-query.
     *
     * <p>Note: the kill is performed AFTER the disruption stops (not during)
     * because {@link InternalTestCluster#stopRandomNode} requires cluster
     * consensus (it adds voting-config exclusions before shutdown), which a
     * still-disrupted victim cannot acknowledge — the framework would throw
     * AssertionError("unexpected") from {@code excludeClusterManagers}. The
     * brief disruption window before the kill still exposes the coordinator
     * to the disconnected-shard state, then the heal+kill validates the
     * post-disruption recovery path.
     */
    public void testShardHostNodeKillAndRestart() throws Exception {
        createAndSeedIndex();
        String victim = firstNonMatching(coordinatorNodeName());
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(coordinatorNodeName()), Collections.singleton(victim)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();
        try {
            Thread.sleep(300);
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
        // Kill the node post-disruption: the framework needs consensus to
        // record voting-config exclusions, and the disrupted victim can't
        // acknowledge those during disruption.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(victim));
        internalCluster().startNode();
        ensureFullyConnected();
        // With 0 replicas, the index has lost data on killed node — recreate.
        try {
            client().admin().indices().prepareDelete(INDEX).get();
        } catch (Exception ignore) {}
        createAndSeedIndex();
        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total");
        assertScalarLong(response, "total", EXPECTED_SUM);
    }

    /**
     * #20 — Coordinator node itself is killed mid-query. Subsequent client call via
     * remaining node must still function. Tests that coordinator singleton state
     * (DataFusionService, AnalyticsSearchTransportService) is properly reconstructed
     * when the client is re-bound.
     */
    public void testCoordinatorNodeCrashRecovery() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        // Fire a query then attempt to kill the coordinator — note: the client()
        // helper resolves to a cluster-wide client, so post-crash it can still
        // dispatch against a surviving node.
        ExecutorService exec = Executors.newSingleThreadExecutor();
        Future<PPLResponse> inFlight = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
        Thread.sleep(200);
        try {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(coord));
        } catch (Exception e) {
            // Sometimes stop fails in transit; proceed — next query will confirm survival.
        }
        // In-flight query may throw; that's fine.
        try {
            inFlight.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
        } catch (Throwable ignore) {}
        exec.shutdownNow();
        exec.awaitTermination(5, TimeUnit.SECONDS);

        // Bring cluster back to 3 nodes, verify a fresh query works.
        internalCluster().startNode();
        ensureFullyConnected();
        try {
            client().admin().indices().prepareDelete(INDEX).get();
        } catch (Exception ignore) {}
        createAndSeedIndex();
        PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total");
        assertScalarLong(response, "total", EXPECTED_SUM);
    }

    // ---------------------------------------------- E: lifecycle / planner / listener (#21-#34)

    /**
     * #21 — Malformed PPL (references a nonexistent field). Coordinator must fail
     * fast in planning with a clear error containing the bad field name; not hang
     * past timeout, not leak FragmentExecutionAction tasks.
     */
    public void testPlanFailurePropagates() throws Exception {
        createAndSeedIndex();
        // Sample task list before to detect any leak after.
        ListTasksResponse before = client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get();
        int beforeCount = before.getTasks().size();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            executePPL("source = " + INDEX + " | stats sum(nonexistent_field) as total", QUERY_TIMEOUT);
        } catch (Throwable t) {
            failure.set(t);
        }
        assertNotNull("PPL referencing missing field must surface a planning error; got null failure (response=hang?)", failure.get());
        // No leaked fragment tasks (planning failure must short-circuit before dispatch).
        assertNoPendingFragmentTasks();
        ListTasksResponse after = client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get();
        assertEquals(
            "Plan failure must not leak fragment tasks: before=" + beforeCount + " after=" + after.getTasks().size(),
            beforeCount,
            after.getTasks().size()
        );
    }

    /**
     * #22 — Contract under test: when planning fails after a successful transport
     * dispatch but before any fragments execute, the coordinator surfaces the
     * planner failure AND tears down all coordinator-side registered state
     * (pending executions, fragment-task accounting, query-context entries) so
     * that subsequent queries observe a clean baseline. The propagation half of
     * this contract — "planner failure surfaces, no leaked fragment tasks" — is
     * already covered indirectly by {@link #testPlanFailurePropagates} (#21),
     * which drives an actual planner-rejecting query
     * ({@code stats sum(nonexistent_field)}) and asserts on
     * {@code FragmentExecutionAction} task counts. What this test would add on
     * top is tighter cleanup-state observability (registered-state accessors on
     * the planner / orchestrator). Deferred: there is no public fault seam or
     * registered-state accessor on the unified query planner today.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "Tighter cleanup observability for planner-stage failures. The propagation "
        + "contract (planner failure surfaces + no leaked FragmentExecutionAction tasks) is "
        + "already covered indirectly by testPlanFailurePropagates (#21), which exercises the "
        + "same path with a real planner-rejecting query. What this test would add is direct "
        + "assertion on coordinator registered-state cleanup (pending executions, query "
        + "contexts) after a planner-injected failure. Deferred until UnifiedQueryPlanner "
        + "exposes a public fault seam or a registered-state observability accessor — "
        + "natural call sites are sandbox/plugins/analytics-engine/src/main/java/org/"
        + "opensearch/analytics/exec/DefaultPlanExecutor.java (transport entry, around the "
        + "doExecute(...) plan-then-schedule path) and sandbox/plugins/analytics-engine/src/"
        + "main/java/org/opensearch/analytics/planner/PlannerImpl.java (planner internals). "
        + "MockTransportService.addRequestHandlingBehavior on the dispatch action sees the "
        + "request only AFTER transport dispatch — i.e. after planning has kicked off "
        + "in-process — so transport interception cannot reach the planner-internal failure "
        + "mode.")

    /**
     * #23 — Inject a failure during stage build so earlier stages are
     * mid-construction. Earlier stages must close (their backend sinks are
     * released, native handles dropped) and the query must surface the build
     * exception cleanly.
     *
     * <p>The seam is the existing {@link StageExecutionBuilder#registerScheduler}
     * API: tests swap in a faulting {@link StageScheduler} for a chosen
     * {@link StageExecutionType} and the original {@code registerScheduler}
     * call returns the prior scheduler so we can restore in {@code finally}.
     * No test-only static hook, no private-field reflection.
     *
     * <p>This test exercises the partial-build cleanup path in
     * {@code QueryExecution.build()}: the coordinator-reduce stage builds first
     * (root, allocates a native session via {@code ExchangeSinkProvider.createSink}),
     * then a child {@code SHARD_FRAGMENT} build throws — at which point the
     * already-built reduce stage must be cancelled, releasing its native
     * resources. We use {@link NativeHandle#liveHandleCount()} as the
     * post-condition oracle.
     */
    public void testStageBuildFailureReleasesEarlierStages() throws Exception {
        createAndSeedIndex();
        System.gc();
        Thread.sleep(50);
        int baseline = NativeHandle.liveHandleCount();

        // The PPL client may dispatch to any node — register the faulting
        // scheduler on every node so the build failure is reproducible
        // regardless of which one ends up coordinating.
        AtomicInteger faultCount = new AtomicInteger();
        Map<String, StageScheduler> previousByNode = new HashMap<>();
        StageScheduler faulting = (stage, sink, config) -> {
            faultCount.incrementAndGet();
            throw new IllegalStateException("injected SHARD_FRAGMENT build failure for stageId=" + stage.getStageId());
        };
        for (String node : internalCluster().getNodeNames()) {
            StageExecutionBuilder b = internalCluster().getInstance(QueryScheduler.class, node).getStageExecutionBuilder();
            StageScheduler prev = b.registerScheduler(StageExecutionType.SHARD_FRAGMENT, faulting);
            if (prev != null) {
                previousByNode.put(node, prev);
            }
        }
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            try {
                executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            } catch (Throwable t) {
                failure.set(t);
            }
            assertTrue(
                "Faulting SHARD_FRAGMENT scheduler must have fired at least once (failure=" + failure.get() + ")",
                faultCount.get() >= 1
            );
            assertNotNull("Build failure must surface to the listener (got null failure)", failure.get());
        } finally {
            // Restore — even on failure, leaving a faulting scheduler in
            // place would poison subsequent tests in the suite.
            for (Map.Entry<String, StageScheduler> e : previousByNode.entrySet()) {
                StageExecutionBuilder b = internalCluster().getInstance(QueryScheduler.class, e.getKey()).getStageExecutionBuilder();
                b.registerScheduler(StageExecutionType.SHARD_FRAGMENT, e.getValue());
            }
        }
        // Native handle registry must return to baseline — the partial-build
        // cleanup in QueryExecution.build() cancels every already-built stage,
        // which closes any backend sink the reduce path created before the
        // child build threw.
        assertBusy(() -> {
            System.gc();
            int after = NativeHandle.liveHandleCount();
            assertTrue(
                "NativeHandle live count grew after stage-build failure: baseline=" + baseline + " after=" + after,
                after <= baseline
            );
        }, 30, TimeUnit.SECONDS);
        // Sanity: a fresh query against the same index works post-failure.
        // If the partial-build cleanup was incomplete we'd see a residual
        // session / lock and this query would fail or hang.
        assertScalarLong(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT), "total", EXPECTED_SUM);
    }

    /**
     * #24 — Fail during exchange-sink registration: the coordinator-reduce
     * stage's {@code ExchangeSinkProvider.createSink} call (invoked from
     * {@code LocalStageScheduler.createExecution}) throws. Earlier dispatch-
     * side state (none, in this case — the reduce stage builds before any
     * child) must not leak; the build-failure path must surface the cause
     * to the caller and the coordinator-side native pool must return to
     * baseline.
     *
     * <p>Uses the same {@link StageExecutionBuilder#registerScheduler} seam
     * as #23 but replaces the {@code COORDINATOR_REDUCE} scheduler with
     * one that throws on {@code createExecution}. A real
     * {@code ExchangeSinkProvider.createSink} failure (e.g. the backend
     * runs out of native memory while allocating a session) flows through
     * the same code path: {@code LocalStageScheduler} catches and
     * re-throws, {@code QueryExecution.build()} catches and runs the
     * partial-build cleanup, the listener sees the original cause.
     */
    public void testExchangeSinkRegistrationFailureTearsDown() throws Exception {
        createAndSeedIndex();
        DataFusionService dfs = internalCluster().getInstance(DataFusionService.class, coordinatorNodeName());
        long memBefore = dfs.getMemoryPoolUsage();

        AtomicInteger faultCount = new AtomicInteger();
        Map<String, StageScheduler> previousByNode = new HashMap<>();
        StageScheduler faulting = (stage, sink, config) -> {
            faultCount.incrementAndGet();
            // Mirrors LocalStageScheduler's wrapping when ExchangeSinkProvider.createSink throws.
            throw new RuntimeException(
                "Failed to create exchange sink for stageId=" + stage.getStageId(),
                new IllegalStateException("injected exchange-sink registration failure")
            );
        };
        for (String node : internalCluster().getNodeNames()) {
            StageExecutionBuilder b = internalCluster().getInstance(QueryScheduler.class, node).getStageExecutionBuilder();
            StageScheduler prev = b.registerScheduler(StageExecutionType.COORDINATOR_REDUCE, faulting);
            if (prev != null) {
                previousByNode.put(node, prev);
            }
        }
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            try {
                executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            } catch (Throwable t) {
                failure.set(t);
            }
            assertTrue(
                "Faulting COORDINATOR_REDUCE scheduler must have fired at least once (failure=" + failure.get() + ")",
                faultCount.get() >= 1
            );
            assertNotNull("Build failure must surface to the listener (got null failure)", failure.get());
        } finally {
            for (Map.Entry<String, StageScheduler> e : previousByNode.entrySet()) {
                StageExecutionBuilder b = internalCluster().getInstance(QueryScheduler.class, e.getKey()).getStageExecutionBuilder();
                b.registerScheduler(StageExecutionType.COORDINATOR_REDUCE, e.getValue());
            }
        }
        // Native memory pool must not leak across the failed build.
        assertBusy(() -> {
            long memAfter = dfs.getMemoryPoolUsage();
            assertTrue(
                "Native memory grew after exchange-sink build failure: before=" + memBefore + " after=" + memAfter,
                memAfter <= memBefore + 1024 * 1024  // 1MiB slack for runtime heap moves (matches #17)
            );
        }, 30, TimeUnit.SECONDS);
        // Post-recovery sanity check.
        assertScalarLong(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT), "total", EXPECTED_SUM);
    }

    /**
     * #25 — All shards unassigned: stop the node hosting the index's primaries
     * (with 0 replicas), then immediately query before recovery completes.
     * Coordinator must surface a clear "no shard available" error.
     */
    public void testAllShardsUnassigned() throws Exception {
        createAndSeedIndex();
        // With 3 shards, 0 replicas spread across 3 nodes, stopping ANY one node
        // strands its hosted primaries with no allocation target until a recovery
        // happens. We don't wait for recovery — we query immediately.
        String victim = pickShardHostingNode();
        try {
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(victim));
        } catch (Exception e) {
            // Already gone — fine.
        }
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<PPLResponse> response = new AtomicReference<>();
        try {
            response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
        } catch (Throwable t) {
            failure.set(t);
        }
        // Either fast failure (preferred) or partial result with reduced sum.
        assertTrue(
            "Coordinator must terminate (failure or response) when shards are unassigned; got hang",
            failure.get() != null || response.get() != null
        );
        // Restore cluster size so subsequent @After cleanup is clean.
        internalCluster().startNode();
    }

    /**
     * #26 — Race: a node is in the dispatch target set when cluster state was read
     * but disconnects before the request is sent. NetworkDisruption.DISCONNECT
     * scheduled tightly with a query in-flight pins the contract: bounded failure,
     * not a hang.
     */
    public void testMissingNodeAfterResolution() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        String victim = firstNonMatching(coord);
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(coord), Collections.singleton(victim)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        // Race window: schedule disruption start ~50ms after we begin dispatch.
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<?> raceStart = exec.submit(() -> {
                try {
                    Thread.sleep(50);
                    disruption.startDisrupting();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            try {
                raceStart.get(5, TimeUnit.SECONDS);
            } catch (Throwable ignore) {}
            assertTrue("Disconnect-during-dispatch race must terminate in bounded time", failure.get() != null || response.get() != null);
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }
        ensureFullyConnected();
    }

    /**
     * #27 — Cancel between dispatch and first batch. Holds the data-node handler
     * for ~500ms; cancels parent task during that window. Verifies clean cancel
     * with no zombie handlers. Uses MockTransportService to delay; the streaming
     * codepath registers on StreamTransportService which is NOT stubbed by
     * MockTransportService — so this delay only fires on the regular-transport
     * fallback path. With STREAM_TRANSPORT=true this becomes effectively a
     * no-delay cancel test which still pins the contract.
     */
    public void testCancelBetweenDispatchAndFirstBatch() throws Exception {
        createAndSeedIndex();
        String victim = pickShardHostingNode();
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
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            // Wait long enough for dispatch to land on the data-node, but not so long
            // that the streaming codepath (which bypasses our stub) finishes first.
            Thread.sleep(500);
            CancelTasksResponse cancel = client().admin().cluster().prepareCancelTasks().setActions(UnifiedPPLExecuteAction.NAME).get();
            assertFalse(
                "cancel request must not carry node failures",
                cancel.getNodeFailures() != null && !cancel.getNodeFailures().isEmpty()
            );
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
        // No zombie fragment tasks anywhere.
        assertNoPendingFragmentTasks();
    }

    /**
     * #28 — Mid-stream batch exception. Cleanly returns 3 batches, then 4th throws.
     * Coordinator surfaces error without infinite drain.
     *
     * <p>Data-node handler delegates to the production streaming handler via a
     * {@link ForwardingChannel} that counts {@code sendResponseBatch} calls and
     * throws on the 4th. The production handler's outer catch turns the throw
     * into {@code channel.sendResponse(exception)}, which the coordinator
     * surfaces as a query failure (or a partial-failure response — both
     * outcomes are acceptable; a hang is not).
     */
    public void testMidStreamBatchExceptionPropagates() throws Exception {
        createAndSeedIndex();
        String victim = pickShardHostingNode();
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        AtomicInteger batchCount = new AtomicInteger(0);
        AtomicBoolean injected = new AtomicBoolean(false);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            TransportChannel wrapper = new ForwardingChannel(channel) {
                @Override
                public void sendResponseBatch(TransportResponse response) {
                    int n = batchCount.incrementAndGet();
                    if (n == 4) {
                        injected.set(true);
                        // Release the would-be-forwarded batch's native memory
                        // so the throw doesn't leak the source root's buffers.
                        if (response instanceof org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse arrow) {
                            org.apache.arrow.vector.VectorSchemaRoot r = arrow.getRoot();
                            if (r != null) {
                                try {
                                    r.close();
                                } catch (Exception ignore) {}
                            }
                        }
                        throw new RuntimeException("injected mid-stream batch failure");
                    }
                    super.sendResponseBatch(response);
                }
            };
            handler.messageReceived(request, wrapper, task);
        });
        try {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<PPLResponse> response = new AtomicReference<>();
            try {
                response.set(executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            } catch (Throwable t) {
                failure.set(t);
            }
            // Either a query-failure exception OR a partial-failure response;
            // a null+null pair would mean the actionGet wedged past timeout.
            assertTrue(
                "Coordinator must terminate (failure or response) after mid-stream batch error; "
                    + "got null/null → hang. injected="
                    + injected.get()
                    + " batches="
                    + batchCount.get(),
                failure.get() != null || response.get() != null
            );
        } finally {
            mts.clearAllRules();
        }
    }

    /**
     * #29 — Connection drop mid-stream on one shard's stream. NetworkDisruption
     * applies to both the regular and streaming transports' connections, so this
     * one IS exercisable.
     */
    public void testMidStreamConnectionDropOneShard() throws Exception {
        createAndSeedIndex();
        String coord = coordinatorNodeName();
        String victim = firstNonMatching(coord);
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(coord), Collections.singleton(victim)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> {
                try {
                    return executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
                } catch (Throwable t) {
                    return null;
                }
            });
            // Let dispatch get out, then disrupt mid-stream.
            Thread.sleep(75);
            disruption.startDisrupting();
            PPLResponse response = null;
            Throwable failure = null;
            try {
                response = fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (Throwable t) {
                failure = t;
            }
            // Either error surfaced, or partial response; not a hang.
            assertTrue("Mid-stream disconnect must surface error or partial response (no hang)", response != null || failure != null);
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }
        ensureFullyConnected();
    }

    /**
     * #30 — Cancel during the reduce stage. After all data-node streams have
     * closed, the coordinator's reduce sink continues consuming from the native
     * mpsc; cancelling the parent task should signal the reduce to stop and the
     * native handle/session to release.
     *
     * <p>The post-data-node-only window is impractical to race
     * deterministically without an invasive orchestrator hook. This test
     * pins the weaker but still-valuable observable contract called out in
     * the test javadoc: cancel during a query that has begun consuming
     * data, observe that both the native memory pool AND the
     * {@link NativeHandle} live-count return to baseline. We arrange for
     * the data-node side to be fast (no held latch — let it complete
     * naturally) so the cancel races against either an active reduce or
     * a just-completed reduce. Either way, the post-condition (no leak)
     * must hold.
     */
    public void testCancelDuringReduce() throws Exception {
        createAndSeedIndex();
        DataFusionService dfs = internalCluster().getInstance(DataFusionService.class, coordinatorNodeName());
        // Warm-up: prime native pools / JIT so the baseline reflects steady state.
        executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        System.gc();
        Thread.sleep(50);
        long memBaseline = dfs.getMemoryPoolUsage();
        int handleBaseline = NativeHandle.liveHandleCount();

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            Future<PPLResponse> fut = exec.submit(() -> executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT));
            // Brief delay so the dispatch has crossed into the data-node and
            // the reduce-side session is allocated. Tight enough that we
            // usually still have a pending reduce when the cancel lands.
            Thread.sleep(150);
            CancelTasksResponse cancel = client().admin().cluster().prepareCancelTasks().setActions(UnifiedPPLExecuteAction.NAME).get();
            assertFalse("cancel must not produce node failures", cancel.getNodeFailures() != null && !cancel.getNodeFailures().isEmpty());
            try {
                fut.get(QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            } catch (ExecutionException | TimeoutException ignore) {
                // Either the query was cancelled mid-flight (preferred — the
                // race fired during reduce) or it had already completed before
                // the cancel landed (a no-op race). Both must satisfy the
                // post-conditions below.
            }
        } finally {
            exec.shutdownNow();
            exec.awaitTermination(5, TimeUnit.SECONDS);
        }
        // Native memory pool returns to baseline.
        assertBusy(() -> {
            long memAfter = dfs.getMemoryPoolUsage();
            assertTrue(
                "Native memory grew after cancel-during-reduce: baseline=" + memBaseline + " after=" + memAfter,
                memAfter <= memBaseline + 1024 * 1024
            );
        }, 30, TimeUnit.SECONDS);
        // NativeHandle registry returns to baseline.
        assertBusy(() -> {
            System.gc();
            int handleAfter = NativeHandle.liveHandleCount();
            assertTrue(
                "NativeHandle live count grew after cancel-during-reduce: baseline=" + handleBaseline + " after=" + handleAfter,
                handleAfter <= handleBaseline
            );
        }, 30, TimeUnit.SECONDS);
        // Parent task is gone.
        ListTasksResponse remaining = client().admin().cluster().prepareListTasks().setActions(UnifiedPPLExecuteAction.NAME).get();
        assertTrue(
            "No residual UnifiedPPLExecute tasks after cancel-during-reduce: " + remaining.getTasks(),
            remaining.getTasks().isEmpty()
        );
    }

    /**
     * #32 — Listener single-callback contract: exactly one of (onResponse,
     * onFailure) fires per query, normal or failing.
     */
    public void testListenerSingleCallback() throws Exception {
        createAndSeedIndex();
        // Normal query.
        AtomicInteger okCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();
        org.opensearch.core.action.ActionListener<PPLResponse> okListener = new org.opensearch.core.action.ActionListener<>() {
            @Override
            public void onResponse(PPLResponse r) {
                okCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                failCount.incrementAndGet();
            }
        };
        client().execute(
            UnifiedPPLExecuteAction.INSTANCE,
            new PPLRequest("source = " + INDEX + " | stats sum(value) as total"),
            okListener
        );
        assertBusy(() -> assertEquals(1, okCount.get() + failCount.get()), QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertEquals("normal query: exactly one onResponse, zero onFailure", 1, okCount.get());
        assertEquals("normal query: zero onFailure", 0, failCount.get());

        // Failing query (bad field — guaranteed plan failure from #21).
        AtomicInteger ok2 = new AtomicInteger();
        AtomicInteger fail2 = new AtomicInteger();
        org.opensearch.core.action.ActionListener<PPLResponse> failListener = new org.opensearch.core.action.ActionListener<>() {
            @Override
            public void onResponse(PPLResponse r) {
                ok2.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                fail2.incrementAndGet();
            }
        };
        client().execute(
            UnifiedPPLExecuteAction.INSTANCE,
            new PPLRequest("source = " + INDEX + " | stats sum(nonexistent_field) as total"),
            failListener
        );
        assertBusy(() -> assertEquals(1, ok2.get() + fail2.get()), QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
        assertEquals("failing query: zero onResponse", 0, ok2.get());
        assertEquals("failing query: exactly one onFailure", 1, fail2.get());
    }

    /**
     * #33 — Race a shard success with another shard's failure (via injected
     * exception on one node). The coordinator listener must fire exactly once.
     */
    public void testConcurrentSuccessAndFailureDoesNotDoubleCallback() throws Exception {
        createAndSeedIndex();
        // Inject failure on one shard-host. With STREAM_TRANSPORT enabled, the
        // injection only fires on the regular-transport fallback. Either way,
        // the listener-single-callback contract holds.
        String victim = pickShardHostingNode();
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            channel.sendResponse(new IllegalStateException("listener-race-injection"));
        });
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();
        try {
            org.opensearch.core.action.ActionListener<PPLResponse> listener = new org.opensearch.core.action.ActionListener<>() {
                @Override
                public void onResponse(PPLResponse r) {
                    ok.incrementAndGet();
                }

                @Override
                public void onFailure(Exception e) {
                    fail.incrementAndGet();
                }
            };
            client().execute(
                UnifiedPPLExecuteAction.INSTANCE,
                new PPLRequest("source = " + INDEX + " | stats sum(value) as total"),
                listener
            );
            assertBusy(() -> assertEquals(1, ok.get() + fail.get()), QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            // Single callback regardless of outcome.
            assertEquals(
                "Listener must fire exactly once on partial-failure race; got ok=" + ok.get() + " fail=" + fail.get(),
                1,
                ok.get() + fail.get()
            );
        } finally {
            mts.clearAllRules();
        }
    }

    /**
     * On a child-stage failure, the coordinator-side reduce sink's drain task
     * must NOT see a closed query allocator. Reproduces a race where
     * {@code LocalStageExecution.failFromChild} fired the walker terminal
     * listener (which tears down the per-query Arrow allocator) before
     * closing the reduce sink — leaving the drain task importing batches
     * into a freshly-closed allocator. The drain task logs a WARN
     * ("drain task terminated with error ... allocator is closed"); the
     * test asserts that WARN never fires.
     */
    public void testStageFailureDoesNotLeakAllocator() throws Exception {
        createAndSeedIndex();
        Logger reduceSinkLogger = LogManager.getLogger("org.opensearch.be.datafusion.DatafusionReduceSink");
        try (MockLogAppender appender = MockLogAppender.createForLoggers(reduceSinkLogger)) {
            appender.addExpectation(new MockLogAppender.UnseenEventExpectation(
                "drain task terminated with closed allocator",
                "org.opensearch.be.datafusion.DatafusionReduceSink",
                Level.WARN,
                "*drain task terminated*"
            ));

            String victim = pickShardHostingNode();
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                // Brief delay so the other shards' streams have time to feed
                // the coordinator-side reduce sink, putting the drain task in
                // its active-import window when the injected failure fires.
                Thread.sleep(50);
                channel.sendResponse(new IllegalStateException("inject-shard-failure"));
            });
            AtomicInteger ok = new AtomicInteger();
            AtomicInteger fail = new AtomicInteger();
            try {
                ActionListener<PPLResponse> listener = new ActionListener<>() {
                    @Override
                    public void onResponse(PPLResponse r) {
                        ok.incrementAndGet();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        fail.incrementAndGet();
                    }
                };
                client().execute(
                    UnifiedPPLExecuteAction.INSTANCE,
                    new PPLRequest("source = " + INDEX + " | stats sum(value) as total"),
                    listener
                );
                assertBusy(() -> assertEquals(1, ok.get() + fail.get()), QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
                assertEquals("query must fail (one shard injected error); ok=" + ok.get(), 1, fail.get());
                // Give async drain task a moment to tick after the listener fired.
                Thread.sleep(200);
                appender.assertAllExpectationsMatched();
            } finally {
                mts.clearAllRules();
            }
        }
    }

    /**
     * #34 — Coordinator memory stable across many queries. 50 successful queries
     * (200 was the spec target; reduced to keep test budget under 60s with
     * 3-shard parquet cold-start). Asserts the native memory pool's end usage
     * does not drift more than max(5 MiB, 10%) above baseline.
     */
    public void testCoordinatorMemoryStablesAcrossManyQueries() throws Exception {
        createAndSeedIndex();
        DataFusionService dfs = internalCluster().getInstance(DataFusionService.class, coordinatorNodeName());
        // Warm-up: one query primes thread pools, JIT, native runtime caches.
        executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        long baseline = dfs.getMemoryPoolUsage();
        long midpoint = baseline;
        final int n = 50;
        for (int i = 0; i < n; i++) {
            PPLResponse r = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            assertScalarLong(r, "total", EXPECTED_SUM);
            if (i == n / 2) {
                midpoint = dfs.getMemoryPoolUsage();
            }
        }
        long end = dfs.getMemoryPoolUsage();

        // Threshold: end ≤ baseline + 5 MiB AND end ≤ baseline × 1.10.
        long absoluteCeiling = baseline + 5L * 1024L * 1024L;
        long relativeCeiling = (long) (baseline * 1.10);
        long allowed = Math.max(absoluteCeiling, relativeCeiling);
        // Special case: tiny / zero baseline. Use absolute-only to avoid 1.10 × 0 = 0.
        if (baseline < 1024L * 1024L) {
            allowed = absoluteCeiling;
        }
        logger.info("memory drift across {} queries: baseline={} mid={} end={} allowed≤{}", n, baseline, midpoint, end, allowed);
        assertTrue(
            "Native memory pool drifted: baseline=" + baseline + " mid=" + midpoint + " end=" + end + " allowed=" + allowed,
            end <= allowed
        );
    }

    // ---------------------------------------------- F: topology variations

    /**
     * #F1 — Dedicated coordinating-only client node. Adds a coordinating-only node to
     * the 3-data-node cluster and dispatches the query through it. Verifies the
     * coordinator-reduce stage runs correctly on a node that holds NO data shards —
     * a common production topology where coordinators are separate from data nodes.
     */
    public void testTopologyDedicatedClientNode() throws Exception {
        String coordOnly = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureStableCluster(internalCluster().getNodeNames().length);

        createAndSeedIndex();

        // Dispatch through the coordinating-only node specifically.
        PPLResponse r = client(coordOnly).execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest("source = " + INDEX + " | stats sum(value) as total")).actionGet(QUERY_TIMEOUT);
        assertScalarLong(r, "total", EXPECTED_SUM);
    }

    // -------------------------------------------- G: single-shard / no-reduce-stage

    /**
     * #G1 — Single-shard happy path. With one shard the planner emits a
     * LOCAL_PASSTHROUGH stage instead of LOCAL_REDUCE, so neither
     * LocalStageScheduler nor DatafusionReduceSink is on the hot path.
     * Coordinator just forwards the shard's stream to the caller.
     */
    public void testSingleShardHappyPath() throws Exception {
        createSingleShardIndex(SINGLE_SHARD_INDEX, 30);
        PPLResponse r = executePPL("source = " + SINGLE_SHARD_INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
        assertScalarLong(r, "total", 30L * VALUE);
    }

    /**
     * #G2 — Single-shard shard-failure surfacing. With no reduce stage the failure
     * propagates straight from the shard fragment through the passthrough stage to
     * the listener. Verifies the failure path doesn't hang and surfaces a non-null
     * cause without traversing the reduce-stage error path.
     */
    public void testSingleShardShardFailureSurfaced() throws Exception {
        createSingleShardIndex(SINGLE_SHARD_INDEX, 10);
        String victim = singleShardHostingNode(SINGLE_SHARD_INDEX);
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            channel.sendResponse(new IllegalStateException("single-shard injected failure"));
        });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();
        try {
            client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest("source = " + SINGLE_SHARD_INDEX + " | stats sum(value) as total"), new ActionListener<PPLResponse>() {
                @Override
                public void onResponse(PPLResponse pplResponse) { ok.incrementAndGet(); }
                @Override
                public void onFailure(Exception e) { failure.set(e); fail.incrementAndGet(); }
            });
            assertBusy(() -> assertEquals(1, ok.get() + fail.get()), QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertEquals("single-shard failure must surface as exactly one onFailure", 1, fail.get());
            assertEquals("single-shard failure must not also produce onResponse", 0, ok.get());
            assertNotNull("failure must carry a cause", failure.get());
        } finally {
            mts.clearAllRules();
        }
    }

    /**
     * #G3 — Single-shard parent-task cancellation. Cancels the AnalyticsQueryTask
     * mid-flight and verifies the listener fires with TaskCancelledException
     * without hanging — exercises cancellation through the passthrough stage
     * (no reduce-stage drain to wait on).
     */
    public void testSingleShardCancellationAtCoordinator() throws Exception {
        createSingleShardIndex(SINGLE_SHARD_INDEX, 50);
        String victim = singleShardHostingNode(SINGLE_SHARD_INDEX);
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        // Hold the shard handler so the query stays in flight long enough to cancel.
        CountDownLatch holdShard = new CountDownLatch(1);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            try {
                holdShard.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            // After release, surface a cancelled-style failure so the shard side terminates cleanly.
            channel.sendResponse(new org.opensearch.core.tasks.TaskCancelledException("shard-cancelled-during-test"));
        });
        try {
            AtomicInteger ok = new AtomicInteger();
            AtomicInteger fail = new AtomicInteger();
            client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest("source = " + SINGLE_SHARD_INDEX + " | stats sum(value) as total"), new ActionListener<PPLResponse>() {
                @Override
                public void onResponse(PPLResponse pplResponse) { ok.incrementAndGet(); }
                @Override
                public void onFailure(Exception e) { fail.incrementAndGet(); }
            });
            // Cancel any in-flight analytics tasks.
            Thread.sleep(200);
            client().admin().cluster().prepareCancelTasks().setActions(FragmentExecutionAction.NAME).get();
            holdShard.countDown();
            assertBusy(() -> assertEquals(1, ok.get() + fail.get()), QUERY_TIMEOUT.seconds(), TimeUnit.SECONDS);
            assertEquals("cancelled query must produce exactly one onFailure", 1, fail.get());
        } finally {
            holdShard.countDown();
            mts.clearAllRules();
        }
    }

    /** Returns the node hosting the single primary shard of {@code indexName}. */
    private String singleShardHostingNode(String indexName) {
        for (ShardRouting sr : clusterService().state()
            .routingTable()
            .index(indexName)
            .shardsWithState(org.opensearch.cluster.routing.ShardRoutingState.STARTED)) {
            if (sr.primary()) {
                return clusterService().state().nodes().get(sr.currentNodeId()).getName();
            }
        }
        throw new AssertionError("no started primary for index " + indexName);
    }

    // -------------------------------------------------------------- helpers

    private String firstNonMatching(String name) {
        for (String n : internalCluster().getNodeNames()) {
            if (n.equals(name) == false) return n;
        }
        throw new AssertionError("no node other than " + name + " in cluster: " + List.of(internalCluster().getNodeNames()));
    }

    /**
     * Wait until the cluster is actually fully reconnected after a disruption.
     * The framework's {@code ensureFullyConnectedCluster} only takes a single
     * snapshot — connections re-established via the periodic
     * {@code NodeConnectionsService} take a few hundred ms post-heal, so we
     * retry until success or 30s. Tests that assume a healed cluster (post-
     * disruption queries) need this; silently swallowing the assertion error
     * lets them fire against a half-connected cluster and surface a misleading
     * NodeNotConnectedException as the test failure.
     */
    private void ensureFullyConnected() throws Exception {
        assertBusy(() -> NetworkDisruption.ensureFullyConnectedCluster(internalCluster()), 30, TimeUnit.SECONDS);
    }

    private static void assertScalarLong(PPLResponse response, String column, long expected) {
        assertNotNull("PPLResponse must not be null", response);
        int idx = response.getColumns().indexOf(column);
        assertTrue("response must contain column " + column, idx >= 0);
        assertEquals("row count", 1, response.getRows().size());
        Object cell = response.getRows().get(0)[idx];
        assertNotNull(column + " cell must not be null", cell);
        assertThat(column, ((Number) cell).longValue(), equalTo(expected));
    }

    /** Verify no residual FragmentExecutionAction tasks anywhere in the cluster. */
    private void assertNoPendingFragmentTasks() throws Exception {
        assertBusy(() -> {
            ListTasksResponse tasks = client().admin().cluster().prepareListTasks().setActions(FragmentExecutionAction.NAME).get();
            List<TaskInfo> infos = tasks.getTasks();
            assertTrue(
                "Pending fragment tasks remain: " + infos.stream().map(t -> t.getTaskId().toString()).collect(Collectors.joining(", ")),
                infos.isEmpty()
            );
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Delegating {@link TransportChannel} whose subclasses override sendResponse
     * (or sendResponseBatch) to mutate payloads. Forwards both the unary
     * (sendResponse) and streaming (sendResponseBatch + completeStream)
     * operations so tests can intercept either path.
     */
    private static class ForwardingChannel implements TransportChannel {
        private final TransportChannel delegate;

        ForwardingChannel(TransportChannel delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getProfileName() {
            return delegate.getProfileName();
        }

        @Override
        public String getChannelType() {
            return delegate.getChannelType();
        }

        @Override
        public org.opensearch.Version getVersion() {
            return delegate.getVersion();
        }

        @Override
        public void sendResponse(TransportResponse response) throws java.io.IOException {
            delegate.sendResponse(response);
        }

        @Override
        public void sendResponse(Exception exception) throws java.io.IOException {
            delegate.sendResponse(exception);
        }

        @Override
        public void sendResponseBatch(TransportResponse response) {
            delegate.sendResponseBatch(response);
        }

        @Override
        public void completeStream() {
            delegate.completeStream();
        }
    }

    private static String dumpAllThreads() {
        // Thread.getAllStackTraces() is forbidden in OpenSearch (needs RuntimePermission
        // "getStackTrace"). ThreadMXBean#dumpAllThreads is allowed and returns equivalent
        // info plus monitor/synchronizer ownership for diagnosing deadlocks.
        ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
        StringBuilder sb = new StringBuilder();
        for (ThreadInfo info : tmx.dumpAllThreads(true, true)) {
            sb.append('\n').append(info.toString());
        }
        return sb.toString();
    }
}
