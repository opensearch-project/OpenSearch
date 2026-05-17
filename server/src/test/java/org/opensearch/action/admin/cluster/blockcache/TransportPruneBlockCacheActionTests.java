/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.blockcache;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.plugins.BlockCache;
import org.opensearch.plugins.BlockCacheRegistry;
import org.opensearch.plugins.BlockCacheConstants;
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
import java.util.Optional;
import java.util.Set;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportPruneBlockCacheActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private CapturingTransport transport;
    private TransportService transportService;
    private ActionFilters actionFilters;
    private BlockCacheRegistry registry;
    private BlockCache blockCache;
    private TransportPruneBlockCacheAction action;

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
        blockCache = mock(BlockCache.class);
        registry = mock(BlockCacheRegistry.class);
        when(registry.get(BlockCacheConstants.DISK_CACHE)).thenReturn(Optional.of(blockCache));

        setupClusterWithWarmNodes();
        action = new TransportPruneBlockCacheAction(threadPool, clusterService, transportService, actionFilters, registry);
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
            "warm-node-1", "warm-node-1", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Set.of(DiscoveryNodeRole.WARM_ROLE), Version.CURRENT
        );
        DiscoveryNode warmNode2 = new DiscoveryNode(
            "warm-node-2", "warm-node-2", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Set.of(DiscoveryNodeRole.WARM_ROLE), Version.CURRENT
        );
        DiscoveryNode dataNode = new DiscoveryNode(
            "data-node-1", "data-node-1", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Set.of(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT
        );
        nodesBuilder.add(warmNode1).add(warmNode2).add(dataNode);
        nodesBuilder.localNodeId(warmNode1.getId()).clusterManagerNodeId(warmNode1.getId());
        setState(clusterService, ClusterState.builder(clusterService.getClusterName()).nodes(nodesBuilder).build());
    }

    public void testNodeOperation() {
        PruneBlockCacheRequest request = new PruneBlockCacheRequest(BlockCacheConstants.DISK_CACHE);
        NodePruneBlockCacheResponse response = action.nodeOperation(new TransportPruneBlockCacheAction.NodeRequest(request));

        assertTrue("cleared should be true", response.isCleared());
        verify(blockCache).clear();
    }

    public void testNodeOperationUnknownCache() {
        when(registry.get("unknown")).thenReturn(Optional.empty());
        PruneBlockCacheRequest request = new PruneBlockCacheRequest("unknown");

        expectThrows(IllegalArgumentException.class, () -> action.nodeOperation(new TransportPruneBlockCacheAction.NodeRequest(request)));
    }

    public void testNullRegistry() {
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
            TransportPruneBlockCacheAction nullRegistryAction = new TransportPruneBlockCacheAction(
                threadPool, clusterService, nullTransportService, actionFilters, null
            );
            PruneBlockCacheRequest request = new PruneBlockCacheRequest(BlockCacheConstants.DISK_CACHE);
            NodePruneBlockCacheResponse response = nullRegistryAction.nodeOperation(
                new TransportPruneBlockCacheAction.NodeRequest(request)
            );
            assertFalse("cleared should be false when registry is null", response.isCleared());
        } finally {
            nullTransportService.close();
            nullTransport.close();
        }
    }

    public void testWarmNodeResolution() {
        PruneBlockCacheRequest request = new PruneBlockCacheRequest(BlockCacheConstants.DISK_CACHE);
        action.resolveRequest(request, clusterService.state());

        assertEquals(2, request.concreteNodes().length);
        assertTrue(Arrays.stream(request.concreteNodes()).anyMatch(n -> "warm-node-1".equals(n.getId())));
        assertTrue(Arrays.stream(request.concreteNodes()).anyMatch(n -> "warm-node-2".equals(n.getId())));
    }

    public void testSpecificNodeTargeting() {
        PruneBlockCacheRequest request = new PruneBlockCacheRequest(BlockCacheConstants.DISK_CACHE, "warm-node-1");
        action.resolveRequest(request, clusterService.state());

        assertEquals(1, request.concreteNodes().length);
        assertEquals("warm-node-1", request.concreteNodes()[0].getId());
    }

    public void testNonWarmNodeTargetingThrows() {
        PruneBlockCacheRequest request = new PruneBlockCacheRequest(BlockCacheConstants.DISK_CACHE, "data-node-1");
        expectThrows(IllegalArgumentException.class, () -> action.resolveRequest(request, clusterService.state()));
    }

    public void testResponseAggregation() {
        List<NodePruneBlockCacheResponse> responses = Arrays.asList(
            new NodePruneBlockCacheResponse(clusterService.state().nodes().get("warm-node-1"), true),
            new NodePruneBlockCacheResponse(clusterService.state().nodes().get("warm-node-2"), true)
        );
        PruneBlockCacheResponse response = action.newResponse(
            new PruneBlockCacheRequest(BlockCacheConstants.DISK_CACHE), responses, Collections.emptyList()
        );

        assertEquals(2, response.getNodes().size());
        assertEquals(0, response.failures().size());
    }

    public void testResponseAggregationWithFailure() {
        List<NodePruneBlockCacheResponse> responses = Collections.singletonList(
            new NodePruneBlockCacheResponse(clusterService.state().nodes().get("warm-node-1"), true)
        );
        List<FailedNodeException> failures = Collections.singletonList(
            new FailedNodeException("warm-node-2", "simulated failure", new RuntimeException())
        );
        PruneBlockCacheResponse response = action.newResponse(
            new PruneBlockCacheRequest(BlockCacheConstants.DISK_CACHE), responses, failures
        );

        assertEquals(1, response.getNodes().size());
        assertEquals(1, response.failures().size());
    }
}
