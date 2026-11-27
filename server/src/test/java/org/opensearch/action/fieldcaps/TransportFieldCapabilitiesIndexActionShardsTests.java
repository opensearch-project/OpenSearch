/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.fieldcaps;

import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.WeightedRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;

public class TransportFieldCapabilitiesIndexActionShardsTests extends OpenSearchTestCase {

    private static final String INDEX = "test";

    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;
    private TransportFieldCapabilitiesIndexAction action;

    @BeforeClass
    public static void startThreadPool() {
        threadPool = new TestThreadPool(TransportFieldCapabilitiesIndexActionShardsTests.class.getSimpleName());
    }

    @AfterClass
    public static void stopThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        action = new TransportFieldCapabilitiesIndexAction(
            clusterService,
            transportService,
            mock(IndicesService.class),
            mock(SearchService.class),
            threadPool,
            new ActionFilters(new HashSet<>()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    public void testDoesNotMatchOnceEveryShardSaysSo() {
        setState(clusterService, ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica(INDEX, 2));
        PlainActionFuture<FieldCapabilitiesIndexResponse> future = execute();

        answerEach(shard -> false);

        assertFalse(future.actionGet().canMatch());
    }

    public void testStopsAtTheFirstShardThatCanMatch() {
        setState(clusterService, ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica(INDEX, 2));
        PlainActionFuture<FieldCapabilitiesIndexResponse> future = execute();

        CapturingTransport.CapturedRequest[] requests = transport.getCapturedRequestsAndClear();
        assertEquals(1, requests.length);
        transport.handleResponse(requests[0].requestId, new FieldCapabilitiesIndexResponse(INDEX, Collections.emptyMap(), true));

        assertTrue(future.actionGet().canMatch());
        assertEquals(0, transport.getCapturedRequestsAndClear().length);
    }

    public void testFailsWhenAShardHasNoCopy() {
        setState(clusterService, stateWithUnassignedShard(2, 1));
        PlainActionFuture<FieldCapabilitiesIndexResponse> future = execute();

        answerEach(shard -> false);

        NoShardAvailableActionException e = expectThrows(NoShardAvailableActionException.class, future::actionGet);
        assertEquals("No shard available for index [test]", e.getMessage());
    }

    public void testIgnoresTheFailureOfACopyWhoseShardAnswered() {
        setState(clusterService, stateWithUnassignedShard(2, 1));
        PlainActionFuture<FieldCapabilitiesIndexResponse> future = execute();

        CapturingTransport.CapturedRequest[] requests = transport.getCapturedRequestsAndClear();
        assertEquals(1, requests.length);
        transport.handleRemoteError(requests[0].requestId, new CircuitBreakingException("tripped", CircuitBreaker.Durability.TRANSIENT));
        answerEach(shard -> false);

        NoShardAvailableActionException e = expectThrows(NoShardAvailableActionException.class, future::actionGet);
        assertEquals("No shard available for index [test]", e.getMessage());
    }

    public void testReportsWhyAShardCouldNotAnswer() {
        setState(clusterService, ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica(INDEX, 2));
        PlainActionFuture<FieldCapabilitiesIndexResponse> future = execute();

        CapturingTransport.CapturedRequest[] requests;
        while ((requests = transport.getCapturedRequestsAndClear()).length > 0) {
            assertEquals(1, requests.length);
            if (((FieldCapabilitiesIndexRequest) requests[0].request).shardId().id() == 0) {
                transport.handleRemoteError(
                    requests[0].requestId,
                    new CircuitBreakingException("tripped", CircuitBreaker.Durability.TRANSIENT)
                );
            } else {
                transport.handleResponse(requests[0].requestId, new FieldCapabilitiesIndexResponse(INDEX, Collections.emptyMap(), false));
            }
        }

        expectThrows(CircuitBreakingException.class, future::actionGet);
    }

    public void testSkippingAWeighedAwayCopyDoesNotSkipItsShard() {
        setState(clusterService, weighAwayNode1(ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica(INDEX, 2)));
        PlainActionFuture<FieldCapabilitiesIndexResponse> future = execute();

        // Shard 0's preferred copy fails with a 429, and its other copy is weighed away, so it is skipped.
        CapturingTransport.CapturedRequest[] requests = transport.getCapturedRequestsAndClear();
        assertEquals(1, requests.length);
        assertEquals(0, ((FieldCapabilitiesIndexRequest) requests[0].request).shardId().id());
        transport.handleRemoteError(requests[0].requestId, new CircuitBreakingException("tripped", CircuitBreaker.Durability.TRANSIENT));

        requests = transport.getCapturedRequestsAndClear();
        assertEquals("shard 1 must still be asked", 1, requests.length);
        assertEquals(1, ((FieldCapabilitiesIndexRequest) requests[0].request).shardId().id());
        transport.handleResponse(requests[0].requestId, new FieldCapabilitiesIndexResponse(INDEX, Collections.emptyMap(), true));

        assertTrue(future.actionGet().canMatch());
    }

    private PlainActionFuture<FieldCapabilitiesIndexResponse> execute() {
        PlainActionFuture<FieldCapabilitiesIndexResponse> future = new PlainActionFuture<>();
        action.execute(
            new FieldCapabilitiesIndexRequest(
                new String[] { "*" },
                INDEX,
                OriginalIndices.NONE,
                QueryBuilders.rangeQuery("timestamp").gte("now-1d"),
                System.currentTimeMillis()
            ),
            future
        );
        return future;
    }

    /** Answers each shard request in turn, the first time a shard is asked. */
    private void answerEach(IntPredicate canMatch) {
        Set<Integer> answered = new HashSet<>();
        CapturingTransport.CapturedRequest[] requests;
        while ((requests = transport.getCapturedRequestsAndClear()).length > 0) {
            assertEquals(1, requests.length);
            int shard = ((FieldCapabilitiesIndexRequest) requests[0].request).shardId().id();
            assertTrue("shard " + shard + " asked again after answering", answered.add(shard));
            transport.handleResponse(
                requests[0].requestId,
                new FieldCapabilitiesIndexResponse(INDEX, Collections.emptyMap(), canMatch.test(shard))
            );
        }
    }

    /** node_1, which holds every replica, is in a zone weighted to zero. */
    private static ClusterState weighAwayNode1(ClusterState state) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes());
        for (DiscoveryNode node : state.nodes()) {
            String zone = node.getId().equals("node_1") ? "b" : "a";
            nodes.remove(node.getId());
            nodes.add(
                new DiscoveryNode(node.getName(), node.getId(), node.getAddress(), Map.of("zone", zone), node.getRoles(), node.getVersion())
            );
        }
        WeightedRouting weights = new WeightedRouting("zone", Map.of("a", 1.0, "b", 0.0));
        return ClusterState.builder(state)
            .nodes(nodes)
            .metadata(Metadata.builder(state.metadata()).putCustom(WeightedRoutingMetadata.TYPE, new WeightedRoutingMetadata(weights, 0)))
            .build();
    }

    /** Every shard has a started copy on two nodes, except {@code unassigned}, which has none. */
    private static ClusterState stateWithUnassignedShard(int shards, int unassigned) {
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndOneReplica(INDEX, shards);
        IndexRoutingTable.Builder routing = IndexRoutingTable.builder(state.metadata().index(INDEX).getIndex());
        for (IndexShardRoutingTable shard : state.routingTable().index(INDEX)) {
            if (shard.shardId().id() == unassigned) {
                routing.addIndexShard(
                    new IndexShardRoutingTable.Builder(shard.shardId()).addShard(
                        TestShardRouting.newShardRouting(shard.shardId(), null, true, ShardRoutingState.UNASSIGNED)
                    ).build()
                );
            } else {
                routing.addIndexShard(shard);
            }
        }
        return ClusterState.builder(state).routingTable(RoutingTable.builder().add(routing).build()).build();
    }
}
