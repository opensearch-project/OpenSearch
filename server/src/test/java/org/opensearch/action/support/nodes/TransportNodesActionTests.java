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

package org.opensearch.action.support.nodes;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeActionTests;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.NodeService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;

public class TransportNodesActionTests extends OpenSearchTestCase {

    protected static ThreadPool THREAD_POOL;
    protected ClusterService clusterService;
    protected CapturingTransport transport;
    protected TransportService transportService;
    protected NodeService nodeService;
    protected IndicesService indicesService;

    public void testRequestIsSentToEachNode() throws Exception {
        TransportNodesAction action = getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        int numNodes = clusterService.state().getNodes().getSize();
        // check a request was sent to the right number of nodes
        assertEquals(numNodes, capturedRequests.size());
    }

    public void testNodesSelectors() {
        TransportNodesAction action = getTestTransportNodesAction();
        int numSelectors = randomIntBetween(1, 5);
        Set<String> nodeSelectors = new HashSet<>();
        for (int i = 0; i < numSelectors; i++) {
            nodeSelectors.add(randomFrom(NodeSelector.values()).selector);
        }
        int numNodeIds = randomIntBetween(0, 3);
        String[] nodeIds = clusterService.state().nodes().getNodes().keySet().toArray(new String[0]);
        for (int i = 0; i < numNodeIds; i++) {
            String nodeId = randomFrom(nodeIds);
            nodeSelectors.add(nodeId);
        }
        String[] finalNodesIds = nodeSelectors.toArray(new String[0]);
        TestNodesRequest request = new TestNodesRequest(finalNodesIds);
        action.new AsyncAction(null, request, new PlainActionFuture<>()).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        assertEquals(clusterService.state().nodes().resolveNodes(finalNodesIds).length, capturedRequests.size());
    }

    public void testNewResponseNullArray() {
        TransportNodesAction action = getTestTransportNodesAction();
        expectThrows(NullPointerException.class, () -> action.newResponse(new TestNodesRequest(), null));
    }

    public void testNewResponse() {
        TestTransportNodesAction action = getTestTransportNodesAction();
        TestNodesRequest request = new TestNodesRequest();
        List<TestNodeResponse> expectedNodeResponses = mockList(TestNodeResponse::new, randomIntBetween(0, 2));
        expectedNodeResponses.add(new TestNodeResponse());
        List<BaseNodeResponse> nodeResponses = new ArrayList<>(expectedNodeResponses);
        // This should be ignored:
        nodeResponses.add(new OtherNodeResponse());
        List<FailedNodeException> failures = mockList(
            () -> new FailedNodeException(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                new IllegalStateException(randomAlphaOfLength(8))
            ),
            randomIntBetween(0, 2)
        );

        List<Object> allResponses = new ArrayList<>(expectedNodeResponses);
        allResponses.addAll(failures);

        Collections.shuffle(allResponses, random());

        AtomicReferenceArray<?> atomicArray = new AtomicReferenceArray<>(allResponses.toArray());

        TestNodesResponse response = action.newResponse(request, atomicArray);

        assertSame(request, response.request);
        // note: I shuffled the overall list, so it's not possible to guarantee that it's in the right order
        assertTrue(expectedNodeResponses.containsAll(response.getNodes()));
        assertTrue(failures.containsAll(response.failures()));
    }

    public void testCustomResolving() throws Exception {
        TransportNodesAction action = getDataNodesOnlyTransportNodesAction(transportService);
        TestNodesRequest request = new TestNodesRequest(randomBoolean() ? null : generateRandomStringArray(10, 5, false, true));
        PlainActionFuture<TestNodesResponse> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        // check requests were only sent to data nodes
        for (String nodeTarget : capturedRequests.keySet()) {
            assertTrue(clusterService.state().nodes().get(nodeTarget).isDataNode());
        }
        assertEquals(clusterService.state().nodes().getDataNodes().size(), capturedRequests.size());
    }

    public void testTransportNodesActionWithDiscoveryNodesIncluded() {
        String[] nodeIds = clusterService.state().nodes().getNodes().keySet().toArray(new String[0]);
        TestNodesRequest request = new TestNodesRequest(true, nodeIds);
        getTestTransportNodesAction().new AsyncAction(null, request, new PlainActionFuture<>()).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        List<TestNodeRequest> capturedTransportNodeRequestList = capturedRequests.values()
            .stream()
            .flatMap(Collection::stream)
            .map(capturedRequest -> (TestNodeRequest) capturedRequest.request)
            .collect(Collectors.toList());
        assertEquals(nodeIds.length, capturedTransportNodeRequestList.size());
        capturedTransportNodeRequestList.forEach(
            capturedRequest -> assertEquals(nodeIds.length, capturedRequest.testNodesRequest.concreteNodes().length)
        );
    }

    public void testTransportNodesActionWithDiscoveryNodesReset() {
        String[] nodeIds = clusterService.state().nodes().getNodes().keySet().toArray(new String[0]);
        TestNodesRequest request = new TestNodesRequest(false, nodeIds);
        getTestTransportNodesAction().new AsyncAction(null, request, new PlainActionFuture<>()).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        List<TestNodeRequest> capturedTransportNodeRequestList = capturedRequests.values()
            .stream()
            .flatMap(Collection::stream)
            .map(capturedRequest -> (TestNodeRequest) capturedRequest.request)
            .collect(Collectors.toList());
        assertEquals(nodeIds.length, capturedTransportNodeRequestList.size());
        capturedTransportNodeRequestList.forEach(capturedRequest -> assertNull(capturedRequest.testNodesRequest.concreteNodes()));
    }

    private <T> List<T> mockList(Supplier<T> supplier, int size) {
        List<T> failures = new ArrayList<>(size);
        for (int i = 0; i < size; ++i) {
            failures.add(supplier.get());
        }
        return failures;
    }

    private enum NodeSelector {
        LOCAL("_local"),
        ELECTED_MASTER("_master"),
        // TODO: Remove this element after removing DiscoveryNodeRole.MASTER_ROLE
        MASTER_ELIGIBLE("master:true"),
        CLUSTER_MANAGER_ELIGIBLE("cluster_manager:true"),
        DATA("data:true"),
        CUSTOM_ATTRIBUTE("attr:value");

        private final String selector;

        NodeSelector(String selector) {
            this.selector = selector;
        }
    }

    @BeforeClass
    public static void startThreadPool() {
        THREAD_POOL = new TestThreadPool(TransportBroadcastByNodeActionTests.class.getSimpleName());
    }

    @AfterClass
    public static void destroyThreadPool() {
        ThreadPool.terminate(THREAD_POOL, 30, TimeUnit.SECONDS);
        // since static must set to null to be eligible for collection
        THREAD_POOL = null;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(THREAD_POOL);
        transportService = transport.createTransportService(
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
        int numNodes = randomIntBetween(3, 10);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES));
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAlphaOfLengthBetween(3, 5));
            }
            final DiscoveryNode node = newNode(i, attributes, roles);
            discoBuilder = discoBuilder.add(node);
            discoveryNodes.add(node);
        }
        discoBuilder.localNodeId(randomFrom(discoveryNodes).getId());
        discoBuilder.clusterManagerNodeId(randomFrom(discoveryNodes).getId());
        ClusterState.Builder stateBuilder = ClusterState.builder(clusterService.getClusterName());
        stateBuilder.nodes(discoBuilder);
        ClusterState clusterState = stateBuilder.build();
        setState(clusterService, clusterState);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transport.close();
    }

    public TestTransportNodesAction getTestTransportNodesAction() {
        return new TestTransportNodesAction(
            THREAD_POOL,
            clusterService,
            transportService,
            new ActionFilters(Collections.emptySet()),
            TestNodesRequest::new,
            TestNodeRequest::new,
            ThreadPool.Names.SAME
        );
    }

    public DataNodesOnlyTransportNodesAction getDataNodesOnlyTransportNodesAction(TransportService transportService) {
        return new DataNodesOnlyTransportNodesAction(
            THREAD_POOL,
            clusterService,
            transportService,
            new ActionFilters(Collections.emptySet()),
            TestNodesRequest::new,
            TestNodeRequest::new,
            ThreadPool.Names.SAME
        );
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        String node = "node_" + nodeId;
        return new DiscoveryNode(node, node, buildNewFakeTransportAddress(), attributes, roles, Version.CURRENT);
    }

    private static class TestTransportNodesAction extends TransportNodesAction<
        TestNodesRequest,
        TestNodesResponse,
        TestNodeRequest,
        TestNodeResponse> {

        TestTransportNodesAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<TestNodesRequest> request,
            Writeable.Reader<TestNodeRequest> nodeRequest,
            String nodeExecutor
        ) {
            super(
                "indices:admin/test",
                threadPool,
                clusterService,
                transportService,
                actionFilters,
                request,
                nodeRequest,
                nodeExecutor,
                TestNodeResponse.class
            );
        }

        @Override
        protected TestNodesResponse newResponse(
            TestNodesRequest request,
            List<TestNodeResponse> responses,
            List<FailedNodeException> failures
        ) {
            return new TestNodesResponse(clusterService.getClusterName(), request, responses, failures);
        }

        @Override
        protected TestNodeRequest newNodeRequest(TestNodesRequest request) {
            return new TestNodeRequest(request);
        }

        @Override
        protected TestNodeResponse newNodeResponse(StreamInput in) throws IOException {
            return new TestNodeResponse(in);
        }

        @Override
        protected TestNodeResponse nodeOperation(TestNodeRequest request) {
            return new TestNodeResponse();
        }

    }

    private static class DataNodesOnlyTransportNodesAction extends TestTransportNodesAction {

        DataNodesOnlyTransportNodesAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            ActionFilters actionFilters,
            Writeable.Reader<TestNodesRequest> request,
            Writeable.Reader<TestNodeRequest> nodeRequest,
            String nodeExecutor
        ) {
            super(threadPool, clusterService, transportService, actionFilters, request, nodeRequest, nodeExecutor);
        }

        @Override
        protected void resolveRequest(TestNodesRequest request, ClusterState clusterState) {
            request.setConcreteNodes(clusterState.nodes().getDataNodes().values().toArray(new DiscoveryNode[0]));
        }
    }

    private static class TestNodesRequest extends BaseNodesRequest<TestNodesRequest> {
        TestNodesRequest(StreamInput in) throws IOException {
            super(in);
        }

        TestNodesRequest(String... nodesIds) {
            super(nodesIds);
        }

        TestNodesRequest(boolean includeDiscoveryNodes, String... nodesIds) {
            super(includeDiscoveryNodes, nodesIds);
        }
    }

    private static class TestNodesResponse extends BaseNodesResponse<TestNodeResponse> {

        private final TestNodesRequest request;

        TestNodesResponse(
            ClusterName clusterName,
            TestNodesRequest request,
            List<TestNodeResponse> nodeResponses,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodeResponses, failures);
            this.request = request;
        }

        @Override
        protected List<TestNodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(TestNodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<TestNodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }
    }

    private static class TestNodeRequest extends TransportRequest {

        protected TestNodesRequest testNodesRequest;

        TestNodeRequest() {}

        TestNodeRequest(TestNodesRequest testNodesRequest) {
            this.testNodesRequest = testNodesRequest;
        }

        TestNodeRequest(StreamInput in) throws IOException {
            super(in);
            testNodesRequest = new TestNodesRequest(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            testNodesRequest.writeTo(out);
        }
    }

    private static class TestNodeResponse extends BaseNodeResponse {
        TestNodeResponse() {
            super(mock(DiscoveryNode.class));
        }

        protected TestNodeResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    private static class OtherNodeResponse extends BaseNodeResponse {
        OtherNodeResponse() {
            super(mock(DiscoveryNode.class));
        }

        protected OtherNodeResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

}
