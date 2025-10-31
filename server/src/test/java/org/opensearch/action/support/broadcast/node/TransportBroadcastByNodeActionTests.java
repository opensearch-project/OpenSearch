/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support.broadcast.node;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.OptionallyResolvedIndices;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TestTransportChannel;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.object.HasToString.hasToString;
import static org.mockito.Mockito.mock;

public class TransportBroadcastByNodeActionTests extends OpenSearchTestCase {

    private static final String TEST_INDEX = "test-index";
    private static final String TEST_CLUSTER = "test-cluster";
    private static ThreadPool THREAD_POOL;

    private ClusterService clusterService;
    private CapturingTransport transport;

    private TestTransportBroadcastByNodeAction action;

    public static class Request extends BroadcastRequest<Request> {

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(String... indices) {
            super(indices);
        }
    }

    public static class Response extends BroadcastResponse {
        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(int totalShards, int successfulShards, int failedShards, List<DefaultShardOperationFailedException> shardFailures) {
            super(totalShards, successfulShards, failedShards, shardFailures);
        }
    }

    class TestTransportBroadcastByNodeAction extends TransportBroadcastByNodeAction<
        Request,
        Response,
        TransportBroadcastByNodeAction.EmptyResult> {
        private final Map<ShardRouting, Object> shards = new HashMap<>();
        private final CounterMetric nodeOperationCount = new CounterMetric();

        TestTransportBroadcastByNodeAction(
            TransportService transportService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Writeable.Reader<Request> request,
            String executor
        ) {
            super(
                "indices:admin/test",
                TransportBroadcastByNodeActionTests.this.clusterService,
                transportService,
                actionFilters,
                indexNameExpressionResolver,
                request,
                executor
            );
        }

        @Override
        protected EmptyResult readShardResult(StreamInput in) throws IOException {
            return EmptyResult.readEmptyResultFrom(in);
        }

        @Override
        protected Response newResponse(
            Request request,
            int totalShards,
            int successfulShards,
            int failedShards,
            List<EmptyResult> emptyResults,
            List<DefaultShardOperationFailedException> shardFailures,
            ClusterState clusterState
        ) {
            return new Response(totalShards, successfulShards, failedShards, shardFailures);
        }

        @Override
        protected Request readRequestFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        @Override
        protected EmptyResult shardOperation(Request request, ShardRouting shardRouting) {
            if (rarely()) {
                shards.put(shardRouting, Boolean.TRUE);
                return EmptyResult.INSTANCE;
            } else {
                OpenSearchException e = new OpenSearchException("operation failed");
                shards.put(shardRouting, e);
                throw e;
            }
        }

        @Override
        protected ShardsIterator shards(ClusterState clusterState, Request request, String[] concreteIndices) {
            return clusterState.routingTable().allShards(new String[] { TEST_INDEX });
        }

        @Override
        protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected ClusterBlockException checkRequestBlock(ClusterState state, Request request, String[] concreteIndices) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
        }

        @Override
        protected void nodeOperation(
            List<TransportBroadcastByNodeAction.EmptyResult> results,
            List<BroadcastShardOperationFailedException> accumulatedExceptions
        ) {
            nodeOperationCount.inc();
        }

        public long getNodeOperationCount() {
            return nodeOperationCount.count();
        }

        public Map<ShardRouting, Object> getResults() {
            return shards;
        }
    }

    class MyResolver extends IndexNameExpressionResolver {
        MyResolver() {
            super(new ThreadContext(Settings.EMPTY));
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportBroadcastByNodeActionTests.class.getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        TransportService transportService = transport.createTransportService(
            clusterService.getSettings(),
            THREAD_POOL,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        setClusterState(clusterService, TEST_INDEX);
        action = new TestTransportBroadcastByNodeAction(
            transportService,
            new ActionFilters(new HashSet<>()),
            new MyResolver(),
            Request::new,
            ThreadPool.Names.SAME
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    void setClusterState(ClusterService clusterService, String index) {
        int numberOfNodes = randomIntBetween(3, 5);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(new Index(index, "_na_"));

        int shardIndex = -1;
        int totalIndexShards = 0;
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = newNode(i);
            discoBuilder = discoBuilder.add(node);
            int numberOfShards = randomIntBetween(1, 10);
            totalIndexShards += numberOfShards;
            for (int j = 0; j < numberOfShards; j++) {
                final ShardId shardId = new ShardId(index, "_na_", ++shardIndex);
                ShardRouting shard = TestShardRouting.newShardRouting(
                    index,
                    shardId.getId(),
                    node.getId(),
                    true,
                    ShardRoutingState.STARTED
                );
                IndexShardRoutingTable.Builder indexShard = new IndexShardRoutingTable.Builder(shardId);
                indexShard.addShard(shard);
                indexRoutingTable.addIndexShard(indexShard.build());
            }
        }
        discoBuilder.localNodeId(newNode(0).getId());
        discoBuilder.clusterManagerNodeId(newNode(numberOfNodes - 1).getId());
        ClusterState.Builder stateBuilder = ClusterState.builder(new ClusterName(TEST_CLUSTER));
        stateBuilder.nodes(discoBuilder);
        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder(index)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfReplicas(0)
            .numberOfShards(totalIndexShards);

        stateBuilder.metadata(Metadata.builder().put(indexMetadata));
        stateBuilder.routingTable(RoutingTable.builder().add(indexRoutingTable.build()).build());
        ClusterState clusterState = stateBuilder.build();
        setState(clusterService, clusterState);
    }

    static DiscoveryNode newNode(int nodeId) {
        return new DiscoveryNode("node_" + nodeId, buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    public void testGlobalBlock() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlocks.Builder block = ClusterBlocks.builder()
            .addGlobalBlock(new ClusterBlock(1, "test-block", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL));
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        try {
            action.new AsyncAction(null, request, listener).start();
            fail("expected ClusterBlockException");
        } catch (ClusterBlockException expected) {
            assertEquals("blocked by: [SERVICE_UNAVAILABLE/1/test-block];", expected.getMessage());
        }
    }

    public void testRequestBlock() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        ClusterBlocks.Builder block = ClusterBlocks.builder()
            .addIndexBlock(
                TEST_INDEX,
                new ClusterBlock(1, "test-block", false, true, false, RestStatus.SERVICE_UNAVAILABLE, ClusterBlockLevel.ALL)
            );
        setState(clusterService, ClusterState.builder(clusterService.state()).blocks(block));
        try {
            action.new AsyncAction(null, request, listener).start();
            fail("expected ClusterBlockException");
        } catch (ClusterBlockException expected) {
            assertEquals("index [" + TEST_INDEX + "] blocked by: [SERVICE_UNAVAILABLE/1/test-block];", expected.getMessage());
        }
    }

    public void testOneRequestIsSentToEachNodeHoldingAShard() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<String> set = new HashSet<>();
        for (ShardRouting shard : shardIt) {
            set.add(shard.currentNodeId());
        }

        // check a request was sent to the right number of nodes
        assertEquals(set.size(), capturedRequests.size());

        // check requests were sent to the right nodes
        assertEquals(set, capturedRequests.keySet());
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            // check one request was sent to each node
            assertEquals(1, entry.getValue().size());
        }
    }

    // simulate the cluster-manager being removed from the cluster but before a new cluster-manager is elected
    // as such, the shards assigned to the cluster-manager will still show up in the cluster state as assigned to a node but
    // that node will not be in the local cluster state on any node that has detected the cluster-manager as failing
    // in this case, such a shard should be treated as unassigned
    public void testRequestsAreNotSentToFailedClusterManager() {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        DiscoveryNode clusterManagerNode = clusterService.state().nodes().getClusterManagerNode();
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterService.state().getNodes());
        builder.remove(clusterManagerNode.getId());

        setState(clusterService, ClusterState.builder(clusterService.state()).nodes(builder));

        action.new AsyncAction(null, request, listener).start();

        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        // the cluster manager should not be in the list of nodes that requests were sent to
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<String> set = new HashSet<>();
        for (ShardRouting shard : shardIt) {
            if (!shard.currentNodeId().equals(clusterManagerNode.getId())) {
                set.add(shard.currentNodeId());
            }
        }

        // check a request was sent to the right number of nodes
        assertEquals(set.size(), capturedRequests.size());

        // check requests were sent to the right nodes
        assertEquals(set, capturedRequests.keySet());
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            // check one request was sent to each non-cluster-manager node
            assertEquals(1, entry.getValue().size());
        }
    }

    public void testOperationExecution() throws Exception {
        ShardsIterator shardIt = clusterService.state().routingTable().allShards(new String[] { TEST_INDEX });
        Set<ShardRouting> shards = new HashSet<>();
        String nodeId = shardIt.iterator().next().currentNodeId();
        for (ShardRouting shard : shardIt) {
            if (nodeId.equals(shard.currentNodeId())) {
                shards.add(shard);
            }
        }
        final TransportBroadcastByNodeAction.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();

        final PlainActionFuture<TransportResponse> future = PlainActionFuture.newFuture();
        TestTransportChannel channel = new TestTransportChannel(future);

        handler.messageReceived(action.new NodeRequest(nodeId, new Request(), new ArrayList<>(shards)), channel, null);

        // check the operation was executed only on the expected shards
        assertEquals(shards, action.getResults().keySet());

        TransportResponse response = future.actionGet();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;

        // check the operation was executed on the correct node
        assertEquals("node id", nodeId, nodeResponse.getNodeId());

        int successfulShards = 0;
        int failedShards = 0;
        for (Object result : action.getResults().values()) {
            if (!(result instanceof OpenSearchException)) {
                successfulShards++;
            } else {
                failedShards++;
            }
        }

        // check the operation results
        assertEquals("successful shards", successfulShards, nodeResponse.getSuccessfulShards());
        assertEquals("total shards", action.getResults().size(), nodeResponse.getTotalShards());
        assertEquals("failed shards", failedShards, nodeResponse.getExceptions().size());
        List<BroadcastShardOperationFailedException> exceptions = nodeResponse.getExceptions();
        for (BroadcastShardOperationFailedException exception : exceptions) {
            assertThat(exception.getMessage(), is("operation indices:admin/test failed"));
            assertThat(exception, hasToString(containsString("operation failed")));
        }
        assertEquals(1, action.getNodeOperationCount());
    }

    public void testNodeLevelHookForMultiNode() throws Exception {
        // Manually send the request from coordinator --> target nodes, action.doExecute() does not do that in this test case setup

        Request request = new Request(new String[] { TEST_INDEX });
        final PlainActionFuture<Response> listener = PlainActionFuture.newFuture();
        action.doExecute(mock(Task.class), request, ActionListener.wrap(listener::onResponse, listener::onFailure));

        // Get captured NodeRequest from coordinator for each target node
        Map<String, List<CapturingTransport.CapturedRequest>> captured = transport.getCapturedRequestsByTargetNodeAndClear();
        assertFalse("expected at least one target node", captured.isEmpty());

        final TransportBroadcastByNodeAction<?, ?, ?>.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();

        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> e : captured.entrySet()) {
            CapturingTransport.CapturedRequest cr = e.getValue().get(0);
            assertEquals(action.transportNodeBroadcastAction, cr.action);
            TransportBroadcastByNodeAction.NodeRequest nodeReq = (TransportBroadcastByNodeAction.NodeRequest) cr.request;

            final PlainActionFuture<TransportResponse> future = PlainActionFuture.newFuture();
            TestTransportChannel channel = new TestTransportChannel(future);
            handler.messageReceived(nodeReq, channel, null);
        }
        assertEquals("nodeOperation should run once per target node", captured.size(), action.getNodeOperationCount());
    }

    public void testResultAggregation() throws ExecutionException, InterruptedException {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();

        // simulate removing the cluster-manager
        final boolean simulateFailedClusterManagerNode = rarely();
        DiscoveryNode failedClusterManagerNode = null;
        if (simulateFailedClusterManagerNode) {
            failedClusterManagerNode = clusterService.state().nodes().getClusterManagerNode();
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterService.state().getNodes());
            builder.remove(failedClusterManagerNode.getId());
            builder.clusterManagerNodeId(null);

            setState(clusterService, ClusterState.builder(clusterService.state()).nodes(builder));
        }

        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        ShardsIterator shardIt = clusterService.state().getRoutingTable().allShards(new String[] { TEST_INDEX });
        Map<String, List<ShardRouting>> map = new HashMap<>();
        for (ShardRouting shard : shardIt) {
            if (!map.containsKey(shard.currentNodeId())) {
                map.put(shard.currentNodeId(), new ArrayList<>());
            }
            map.get(shard.currentNodeId()).add(shard);
        }

        int totalShards = 0;
        int totalSuccessfulShards = 0;
        int totalFailedShards = 0;
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            List<BroadcastShardOperationFailedException> exceptions = new ArrayList<>();
            long requestId = entry.getValue().get(0).requestId;
            if (rarely()) {
                // simulate node failure
                totalShards += map.get(entry.getKey()).size();
                totalFailedShards += map.get(entry.getKey()).size();
                transport.handleRemoteError(requestId, new Exception());
            } else {
                List<ShardRouting> shards = map.get(entry.getKey());
                List<TransportBroadcastByNodeAction.EmptyResult> shardResults = new ArrayList<>();
                for (ShardRouting shard : shards) {
                    totalShards++;
                    if (rarely()) {
                        // simulate operation failure
                        totalFailedShards++;
                        exceptions.add(new BroadcastShardOperationFailedException(shard.shardId(), "operation indices:admin/test failed"));
                    } else {
                        shardResults.add(TransportBroadcastByNodeAction.EmptyResult.INSTANCE);
                    }
                }
                totalSuccessfulShards += shardResults.size();
                TransportBroadcastByNodeAction.NodeResponse nodeResponse = action.new NodeResponse(
                    entry.getKey(), shards.size(), shardResults, exceptions
                );
                transport.handleResponse(requestId, nodeResponse);
            }
        }
        if (simulateFailedClusterManagerNode) {
            totalShards += map.get(failedClusterManagerNode.getId()).size();
        }

        Response response = listener.get();
        assertEquals("total shards", totalShards, response.getTotalShards());
        assertEquals("successful shards", totalSuccessfulShards, response.getSuccessfulShards());
        assertEquals("failed shards", totalFailedShards, response.getFailedShards());
        assertEquals("accumulated exceptions", totalFailedShards, response.getShardFailures().length);
    }

    public void testResultWithTimeouts() throws ExecutionException, InterruptedException {
        Request request = new Request(new String[] { TEST_INDEX });
        PlainActionFuture<Response> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();

        ShardsIterator shardIt = clusterService.state().getRoutingTable().allShards(new String[] { TEST_INDEX });
        Map<String, List<ShardRouting>> map = new HashMap<>();
        for (ShardRouting shard : shardIt) {
            if (!map.containsKey(shard.currentNodeId())) {
                map.put(shard.currentNodeId(), new ArrayList<>());
            }
            map.get(shard.currentNodeId()).add(shard);
        }

        int totalShards = 0;
        int totalSuccessfulShards = 0;
        int totalFailedShards = 0;
        String failedNodeId = "node_" + randomIntBetween(0, capturedRequests.size());
        for (Map.Entry<String, List<CapturingTransport.CapturedRequest>> entry : capturedRequests.entrySet()) {
            List<BroadcastShardOperationFailedException> exceptions = new ArrayList<>();
            long requestId = entry.getValue().get(0).requestId;
            if (entry.getKey().equals(failedNodeId)) {
                // simulate node timeout
                totalShards += map.get(entry.getKey()).size();
                totalFailedShards += map.get(entry.getKey()).size();
                transport.handleError(
                    requestId,
                    new ReceiveTimeoutTransportException(
                        clusterService.state().getRoutingNodes().node(entry.getKey()).node(),
                        "indices:admin/test",
                        "time_out_simulated"
                    )
                );
            } else {
                List<ShardRouting> shards = map.get(entry.getKey());
                List<TransportBroadcastByNodeAction.EmptyResult> shardResults = new ArrayList<>();
                for (ShardRouting shard : shards) {
                    totalShards++;
                    if (rarely()) {
                        // simulate operation failure
                        totalFailedShards++;
                        exceptions.add(new BroadcastShardOperationFailedException(shard.shardId(), "operation indices:admin/test failed"));
                    } else {
                        shardResults.add(TransportBroadcastByNodeAction.EmptyResult.INSTANCE);
                    }
                }
                totalSuccessfulShards += shardResults.size();
                TransportBroadcastByNodeAction.NodeResponse nodeResponse = action.new NodeResponse(
                    entry.getKey(), shards.size(), shardResults, exceptions
                );
                transport.handleResponse(requestId, nodeResponse);
            }
        }

        Response response = listener.get();
        assertEquals("total shards", totalShards, response.getTotalShards());
        assertEquals("successful shards", totalSuccessfulShards, response.getSuccessfulShards());
        assertEquals("failed shards", totalFailedShards, response.getFailedShards());
        assertEquals("accumulated exceptions", totalFailedShards, response.getShardFailures().length);
    }

    public void testResolveIndices() {
        Request request = new Request(TEST_INDEX);
        OptionallyResolvedIndices resolvedIndices = action.resolveIndices(request);
        assertEquals(ResolvedIndices.of(TEST_INDEX), resolvedIndices);
    }
}
