/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TransportPruneFileCacheAction} using TransportNodesAction pattern.
 * Tests enhanced multi-node architecture and warm node intelligence.
 */
public class TransportPruneFileCacheActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private FileCache fileCache;
    private TransportPruneFileCacheAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        threadPool = new TestThreadPool("test");
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            null
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        actionFilters = new ActionFilters(Collections.emptySet());
        fileCache = mock(FileCache.class);

        setupClusterWithWarmNodes();

        action = new TransportPruneFileCacheAction(threadPool, clusterService, transportService, actionFilters, fileCache);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
        transport.close();
        clusterService.close();
    }

    private void setupClusterWithWarmNodes() {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();

        DiscoveryNode warmNode1 = new DiscoveryNode(
            "warm-node-1",
            "warm-node-1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.WARM_ROLE),
            Version.CURRENT
        );
        DiscoveryNode warmNode2 = new DiscoveryNode(
            "warm-node-2",
            "warm-node-2",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.WARM_ROLE),
            Version.CURRENT
        );

        DiscoveryNode dataNode1 = new DiscoveryNode(
            "data-node-1",
            "data-node-1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        nodesBuilder.add(warmNode1).add(warmNode2).add(dataNode1);
        nodesBuilder.localNodeId(warmNode1.getId());
        nodesBuilder.clusterManagerNodeId(warmNode1.getId());

        ClusterState clusterState = ClusterState.builder(clusterService.getClusterName()).nodes(nodesBuilder).build();

        setState(clusterService, clusterState);
    }

    public void testRequestPreparationAndTargeting() {
        PruneFileCacheRequest request = new PruneFileCacheRequest();

        TransportPruneFileCacheAction.NodeRequest nodeRequest = action.newNodeRequest(request);
        assertNotNull("Node request should not be null", nodeRequest);
        assertEquals("Node request should wrap original request", request, nodeRequest.getRequest());
    }

    public void testNodeOperation() {
        when(fileCache.capacity()).thenReturn(10737418240L);
        when(fileCache.prune()).thenReturn(1048576L);

        PruneFileCacheRequest globalRequest = new PruneFileCacheRequest();
        TransportPruneFileCacheAction.NodeRequest nodeRequest = new TransportPruneFileCacheAction.NodeRequest(globalRequest);

        NodePruneFileCacheResponse response = action.nodeOperation(nodeRequest);

        assertNotNull("Response should not be null", response);
        assertEquals("Pruned bytes should match", 1048576L, response.getPrunedBytes());
        assertEquals("Capacity should match", 10737418240L, response.getCacheCapacity());

        verify(fileCache).prune();
        verify(fileCache).capacity();
    }

    public void testNullFileCache() {

        CapturingTransport nullTransport = new CapturingTransport();
        TransportService nullTransportService = nullTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            null
        );
        nullTransportService.start();
        nullTransportService.acceptIncomingRequests();

        try {
            TransportPruneFileCacheAction nullCacheAction = new TransportPruneFileCacheAction(
                threadPool,
                clusterService,
                nullTransportService,
                actionFilters,
                null
            );

            PruneFileCacheRequest globalRequest = new PruneFileCacheRequest();
            TransportPruneFileCacheAction.NodeRequest nodeRequest = new TransportPruneFileCacheAction.NodeRequest(globalRequest);

            NodePruneFileCacheResponse response = nullCacheAction.nodeOperation(nodeRequest);

            assertEquals("Pruned bytes should be 0 for null cache", 0L, response.getPrunedBytes());
            assertEquals("Capacity should be 0 for null cache", 0L, response.getCacheCapacity());
        } finally {
            nullTransportService.close();
            nullTransport.close();
        }
    }

    public void testWarmNodeResolution() {

        PruneFileCacheRequest defaultRequest = new PruneFileCacheRequest();

        try {
            action.resolveRequest(defaultRequest, clusterService.state());

            assertEquals("Should resolve to 2 warm nodes", 2, defaultRequest.concreteNodes().length);
            assertTrue(
                "Should include warm-node-1",
                Arrays.stream(defaultRequest.concreteNodes()).anyMatch(node -> "warm-node-1".equals(node.getId()))
            );
            assertTrue(
                "Should include warm-node-2",
                Arrays.stream(defaultRequest.concreteNodes()).anyMatch(node -> "warm-node-2".equals(node.getId()))
            );
        } catch (IllegalArgumentException e) {
            fail("Default request should not fail: " + e.getMessage());
        }
    }

    public void testSpecificNodeTargeting() {

        PruneFileCacheRequest specificRequest = new PruneFileCacheRequest("warm-node-1");

        action.resolveRequest(specificRequest, clusterService.state());

        assertEquals("Should resolve to 1 warm node", 1, specificRequest.concreteNodes().length);
        assertEquals("Should target warm-node-1", "warm-node-1", specificRequest.concreteNodes()[0].getId());
    }

    public void testInvalidNodeTargeting() {

        PruneFileCacheRequest invalidRequest = new PruneFileCacheRequest("data-node-1");

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> action.resolveRequest(invalidRequest, clusterService.state())
        );

        assertTrue(
            "Error message should mention warm nodes",
            exception.getMessage().contains("FileCache operations can only target warm nodes")
        );
    }

    public void testFileCacheException() {
        when(fileCache.prune()).thenThrow(new RuntimeException("Cache corruption"));

        PruneFileCacheRequest globalRequest = new PruneFileCacheRequest();
        TransportPruneFileCacheAction.NodeRequest nodeRequest = new TransportPruneFileCacheAction.NodeRequest(globalRequest);

        RuntimeException exception = expectThrows(RuntimeException.class, () -> action.nodeOperation(nodeRequest));

        assertTrue("Exception should contain node ID", exception.getMessage().contains("node"));
        assertTrue("Exception should mention failure", exception.getMessage().contains("failed"));
    }

    public void testResponseAggregation() {
        PruneFileCacheRequest request = new PruneFileCacheRequest();

        List<NodePruneFileCacheResponse> responses = Arrays.asList(
            new NodePruneFileCacheResponse(clusterService.state().nodes().get("warm-node-1"), 1048576L, 10737418240L),
            new NodePruneFileCacheResponse(clusterService.state().nodes().get("warm-node-2"), 2097152L, 10737418240L)
        );

        List<FailedNodeException> failures = Collections.emptyList();

        PruneFileCacheResponse aggregatedResponse = action.newResponse(request, responses, failures);

        assertNotNull("Aggregated response should not be null", aggregatedResponse);
        assertEquals("Should have 2 successful nodes", 2, aggregatedResponse.getNodes().size());
        assertEquals("Should have no failures", 0, aggregatedResponse.failures().size());
        assertEquals("Total pruned bytes should be sum", 3145728L, aggregatedResponse.getTotalPrunedBytes());
        assertTrue("Should be completely successful", aggregatedResponse.isCompletelySuccessful());
    }
}
