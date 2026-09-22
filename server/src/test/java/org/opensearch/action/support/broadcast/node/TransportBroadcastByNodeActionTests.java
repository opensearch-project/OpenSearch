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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
        private final Map<ShardRouting, Object> shards = new ConcurrentHashMap<>();
        private final CounterMetric nodeOperationCount = new CounterMetric();
        // When true, the node-level operation uses the async dispatch path (shardOperationAsync).
        // Defaults to false so existing tests exercise the synchronous path unchanged.
        private volatile boolean asyncMode = false;
        private final AtomicInteger asyncShardCounter = new AtomicInteger(0);
        // When true, each async shard operation completes its listener twice (a buggy double-completion)
        // to verify the framework's notifyOnce guard keeps per-shard accounting correct.
        private volatile boolean duplicateCompletion = false;
        // When true, shardOperationAsync throws synchronously before invoking the listener.
        // Verifies the framework's per-shard try/catch funnels the throw into the listener so the
        // node response is still produced (no transport-timeout hang).
        private volatile boolean syncThrow = false;
        // Pool used for async per-shard dispatch. A test can point this at an unknown name to exercise
        // the framework's thread-pool-rejection branch in onAsyncShardOperation.
        private volatile String asyncPoolName = ThreadPool.Names.GENERIC;

        void enableAsyncMode() {
            this.asyncMode = true;
        }

        void enableDuplicateCompletion() {
            this.duplicateCompletion = true;
        }

        void enableSyncThrow() {
            this.syncThrow = true;
        }

        void useAsyncPool(String poolName) {
            this.asyncPoolName = poolName;
        }

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
        protected boolean isAsyncShardOperation() {
            return asyncMode;
        }

        @Override
        protected String asyncShardOperationThreadPool() {
            return asyncPoolName;
        }

        /**
         * Async variant used when {@link #asyncMode} is on. Completes deterministically:
         * the first shard dispatched fails, the rest succeed — exercising both the success
         * and the failure (BroadcastShardOperationFailedException) aggregation branches.
         * Results are recorded so the test can derive the expected counts.
         */
        @Override
        protected void shardOperationAsync(Request request, ShardRouting shardRouting, ActionListener<EmptyResult> listener) {
            if (asyncMode == false) {
                super.shardOperationAsync(request, shardRouting, listener);
                return;
            }
            if (syncThrow) {
                // Throw synchronously before invoking the listener. The framework's per-shard try/catch
                // must funnel this into the listener so the node response is still produced.
                throw new OpenSearchException("synchronous failure in shardOperationAsync");
            }
            if (duplicateCompletion) {
                // Intentionally complete the same shard's listener twice. A correct framework must
                // absorb the second completion (notifyOnce) so node-level accounting is not corrupted.
                OpenSearchException e = new OpenSearchException("operation failed");
                shards.put(shardRouting, e);
                listener.onFailure(e);
                listener.onFailure(e);
                return;
            }
            if (asyncShardCounter.getAndIncrement() == 0) {
                OpenSearchException e = new OpenSearchException("operation failed");
                shards.put(shardRouting, e);
                listener.onFailure(e);
            } else {
                shards.put(shardRouting, Boolean.TRUE);
                listener.onResponse(EmptyResult.INSTANCE);
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

    /**
     * Drives the node-level operation through the async dispatch path (isAsyncShardOperation=true).
     * Verifies that every shard is operated on via shardOperationAsync, results and failures are
     * aggregated, the response is sent only after all shards complete, and nodeOperation runs once.
     */
    public void testOperationExecution_AsyncShardOperation() throws Exception {
        action.enableAsyncMode();

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

        // Async path dispatches to the GENERIC pool; the response is only sent after all shards complete.
        TransportResponse response = future.actionGet();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;

        assertEquals("node id", nodeId, nodeResponse.getNodeId());
        // Every shard on the node was operated on via the async path.
        assertEquals(shards, action.getResults().keySet());

        int successfulShards = 0;
        int failedShards = 0;
        for (Object result : action.getResults().values()) {
            if (!(result instanceof OpenSearchException)) {
                successfulShards++;
            } else {
                failedShards++;
            }
        }

        assertEquals("total shards", shards.size(), nodeResponse.getTotalShards());
        assertEquals("successful shards", successfulShards, nodeResponse.getSuccessfulShards());
        assertEquals("failed shards", failedShards, nodeResponse.getExceptions().size());
        // The async failure path wraps the cause in a BroadcastShardOperationFailedException.
        List<BroadcastShardOperationFailedException> exceptions = nodeResponse.getExceptions();
        for (BroadcastShardOperationFailedException exception : exceptions) {
            assertThat(exception.getMessage(), is("operation indices:admin/test failed"));
            assertThat(exception, hasToString(containsString("operation failed")));
        }
        // nodeOperation aggregation hook runs exactly once, after all async shards complete.
        assertEquals(1, action.getNodeOperationCount());
    }

    /**
     * Deterministic async aggregation: exactly one shard fails and one succeeds, guaranteeing both
     * the onResponse and onFailure branches of onAsyncShardOperation execute regardless of the
     * (random) cluster shape. The shard list is supplied directly to the NodeRequest.
     */
    public void testAsyncShardOperation_AggregatesSuccessAndFailure() throws Exception {
        action.enableAsyncMode();

        String nodeId = "test-node";
        // Two primary shards on the same node. The async action fails exactly the first shard to
        // execute (counter == 0) and succeeds the rest, so the outcome is 1 failure + 1 success.
        ShardRouting shard0 = TestShardRouting.newShardRouting(TEST_INDEX, 0, nodeId, true, ShardRoutingState.STARTED);
        ShardRouting shard1 = TestShardRouting.newShardRouting(TEST_INDEX, 1, nodeId, true, ShardRoutingState.STARTED);

        final TransportBroadcastByNodeAction.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();
        final PlainActionFuture<TransportResponse> future = PlainActionFuture.newFuture();
        TestTransportChannel channel = new TestTransportChannel(future);

        List<ShardRouting> shards = new ArrayList<>();
        shards.add(shard0);
        shards.add(shard1);
        handler.messageReceived(action.new NodeRequest(nodeId, new Request(), shards), channel, null);

        TransportResponse response = future.actionGet();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;

        assertEquals("total shards", 2, nodeResponse.getTotalShards());
        assertEquals("one shard succeeded", 1, nodeResponse.getSuccessfulShards());
        List<BroadcastShardOperationFailedException> exceptions = nodeResponse.getExceptions();
        assertEquals("one shard failed", 1, exceptions.size());
        assertThat(exceptions.get(0).getMessage(), is("operation indices:admin/test failed"));
        assertEquals(1, action.getNodeOperationCount());
    }

    /**
     * A buggy async shard operation that completes its listener more than once (e.g. a racing
     * drain-callback and timeout) must not corrupt node-level accounting. The framework wraps each
     * shard listener in {@link org.opensearch.core.action.ActionListener#notifyOnce}, so each shard
     * is counted exactly once and the node response is produced from complete per-shard results
     * rather than being sent early on the duplicate completion.
     */
    public void testAsyncShardOperation_DoubleCompletion_CountedOncePerShard() throws Exception {
        action.enableAsyncMode();
        action.enableDuplicateCompletion();

        String nodeId = "test-node";
        ShardRouting shard0 = TestShardRouting.newShardRouting(TEST_INDEX, 0, nodeId, true, ShardRoutingState.STARTED);
        ShardRouting shard1 = TestShardRouting.newShardRouting(TEST_INDEX, 1, nodeId, true, ShardRoutingState.STARTED);

        final TransportBroadcastByNodeAction.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();
        final PlainActionFuture<TransportResponse> future = PlainActionFuture.newFuture();
        TestTransportChannel channel = new TestTransportChannel(future);

        List<ShardRouting> shards = new ArrayList<>();
        shards.add(shard0);
        shards.add(shard1);
        handler.messageReceived(action.new NodeRequest(nodeId, new Request(), shards), channel, null);

        TransportResponse response = future.actionGet();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;

        // Despite each shard completing twice, accounting reflects exactly 2 shards, each failed once.
        assertEquals("total shards", 2, nodeResponse.getTotalShards());
        assertEquals("no shard succeeded", 0, nodeResponse.getSuccessfulShards());
        assertEquals("each shard recorded exactly one failure", 2, nodeResponse.getExceptions().size());
        // nodeOperation aggregation hook runs exactly once.
        assertEquals(1, action.getNodeOperationCount());
    }

    /**
     * Pointing the async pool at an unknown executor name makes {@code threadPool.executor(...).execute(...)}
     * throw, which the framework must catch and convert into a per-shard failure. The node response is
     * still produced (no leak) and the rejection cause surfaces via the failure exception.
     */
    public void testAsyncShardOperation_ThreadPoolRejection_RecordedAsFailure() throws Exception {
        action.enableAsyncMode();
        action.useAsyncPool("no_such_pool"); // unknown executor → executor(...).execute(...) throws → rejection branch

        String nodeId = "test-node";
        ShardRouting shard = TestShardRouting.newShardRouting(TEST_INDEX, 0, nodeId, true, ShardRoutingState.STARTED);

        final TransportBroadcastByNodeAction.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();
        final PlainActionFuture<TransportResponse> future = PlainActionFuture.newFuture();
        TestTransportChannel channel = new TestTransportChannel(future);

        List<ShardRouting> shards = new ArrayList<>();
        shards.add(shard);
        handler.messageReceived(action.new NodeRequest(nodeId, new Request(), shards), channel, null);

        TransportResponse response = future.actionGet();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;

        assertEquals("total shards", 1, nodeResponse.getTotalShards());
        assertEquals("no shard succeeded", 0, nodeResponse.getSuccessfulShards());
        List<BroadcastShardOperationFailedException> exceptions = nodeResponse.getExceptions();
        assertEquals("the rejected shard is recorded as a failure", 1, exceptions.size());
        assertNotNull("failure must carry the rejection cause", exceptions.get(0).getCause());
        assertThat(exceptions.get(0).getCause().getMessage(), containsString("rejected by thread pool"));
        // nodeOperation aggregation hook still runs exactly once even on a fully-rejected node.
        assertEquals(1, action.getNodeOperationCount());
    }

    /**
     * If {@code shardOperationAsync} throws synchronously on the pool worker before invoking the
     * listener, the framework's per-shard try/catch must funnel the throw into the (notifyOnce-wrapped)
     * listener. Otherwise the throw would escape onto the pool's uncaught-exception handler, the
     * listener would never fire, and the coordinator would only get a response after its transport
     * timeout — a hang we don't want.
     */
    public void testAsyncShardOperation_SyncThrowFromShardOperationAsync_RecordedAsFailure() throws Exception {
        action.enableAsyncMode();
        action.enableSyncThrow();

        String nodeId = "test-node";
        ShardRouting shard0 = TestShardRouting.newShardRouting(TEST_INDEX, 0, nodeId, true, ShardRoutingState.STARTED);
        ShardRouting shard1 = TestShardRouting.newShardRouting(TEST_INDEX, 1, nodeId, true, ShardRoutingState.STARTED);

        final TransportBroadcastByNodeAction.BroadcastByNodeTransportRequestHandler handler =
            action.new BroadcastByNodeTransportRequestHandler();
        final PlainActionFuture<TransportResponse> future = PlainActionFuture.newFuture();
        TestTransportChannel channel = new TestTransportChannel(future);

        List<ShardRouting> shards = new ArrayList<>();
        shards.add(shard0);
        shards.add(shard1);
        handler.messageReceived(action.new NodeRequest(nodeId, new Request(), shards), channel, null);

        // The node response must arrive (not hang) and report both shards as failed.
        TransportResponse response = future.actionGet();
        assertTrue(response instanceof TransportBroadcastByNodeAction.NodeResponse);
        TransportBroadcastByNodeAction.NodeResponse nodeResponse = (TransportBroadcastByNodeAction.NodeResponse) response;

        assertEquals("total shards", 2, nodeResponse.getTotalShards());
        assertEquals("no shard succeeded — both threw synchronously", 0, nodeResponse.getSuccessfulShards());
        List<BroadcastShardOperationFailedException> exceptions = nodeResponse.getExceptions();
        assertEquals("each shard recorded exactly one failure", 2, exceptions.size());
        for (BroadcastShardOperationFailedException ex : exceptions) {
            assertNotNull("failure must carry the sync-throw cause", ex.getCause());
            assertThat(ex.getCause().getMessage(), containsString("synchronous failure in shardOperationAsync"));
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

    // ── Async shardOperationAsync tests ──────────────────────────────────────

    /**
     * Verifies that isAsyncShardOperation defaults to false.
     */
    public void testIsAsyncShardOperation_DefaultFalse() {
        assertFalse("Default isAsyncShardOperation should be false", action.isAsyncShardOperation());
    }

    /**
     * Verifies that the default shardOperationAsync throws UnsupportedOperationException
     * since subclasses must override it when using async mode.
     */
    public void testShardOperationAsync_DefaultThrowsUnsupported() {
        Request request = new Request(TEST_INDEX);
        ShardId shardId = new ShardId(new Index(TEST_INDEX, "_na_"), 0);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED);

        PlainActionFuture<TransportBroadcastByNodeAction.EmptyResult> future = new PlainActionFuture<>();
        expectThrows(UnsupportedOperationException.class, () -> action.shardOperationAsync(request, shardRouting, future));
    }
}
