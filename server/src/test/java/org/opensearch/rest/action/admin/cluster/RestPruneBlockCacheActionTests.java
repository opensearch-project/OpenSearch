/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.blockcache.NodePruneBlockCacheResponse;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheAction;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheRequest;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.plugins.BlockCacheConstants;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestPruneBlockCacheActionTests extends RestActionTestCase {

    private RestPruneBlockCacheAction action;

    @Before
    public void setUpAction() {
        action = new RestPruneBlockCacheAction();
        controller().registerHandler(action);
    }

    private PruneBlockCacheResponse mockResponse() {
        DiscoveryNode node = mock(DiscoveryNode.class);
        when(node.getId()).thenReturn("node1");
        when(node.getName()).thenReturn("test-node");
        when(node.getHostName()).thenReturn("localhost");
        when(node.getHostAddress()).thenReturn("127.0.0.1");
        when(node.getAddress()).thenReturn(buildNewFakeTransportAddress());
        return new PruneBlockCacheResponse(
            new ClusterName("test"),
            Collections.singletonList(new NodePruneBlockCacheResponse(node, true)),
            Collections.emptyList()
        );
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
        assertEquals("/_blockcache/prune", action.routes().get(0).getPath());
    }

    public void testGetName() {
        assertEquals("prune_blockcache_action", action.getName());
    }

    public void testValidCacheParam() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneBlockCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneBlockCacheRequest.class));
            assertEquals(BlockCacheConstants.DISK_CACHE, ((PruneBlockCacheRequest) request).getCacheName());
            return mockResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("cache", BlockCacheConstants.DISK_CACHE);
        dispatchRequest(new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.POST)
            .withPath("/_blockcache/prune")
            .withParams(params)
            .build());
    }

    public void testMissingCacheParamThrows() {
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(
            new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.POST)
                .withPath("/_blockcache/prune")
                .build(),
            verifyingClient
        ));
    }

    public void testUnknownCacheParamThrows() {
        Map<String, String> params = new HashMap<>();
        params.put("cache", "unknown");
        expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(
            new FakeRestRequest.Builder(xContentRegistry())
                .withMethod(RestRequest.Method.POST)
                .withPath("/_blockcache/prune")
                .withParams(params)
                .build(),
            verifyingClient
        ));
    }

    public void testCannotTripCircuitBreaker() {
        assertFalse(action.canTripCircuitBreaker());
    }

    public void testNodeTargeting() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(PruneBlockCacheRequest.class));
            PruneBlockCacheRequest pruneRequest = (PruneBlockCacheRequest) request;
            assertArrayEquals(new String[] { "node1", "node2" }, pruneRequest.nodesIds());
            return mockResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("cache", BlockCacheConstants.DISK_CACHE);
        params.put("nodes", "node1,node2");
        dispatchRequest(new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(RestRequest.Method.POST)
            .withPath("/_blockcache/prune")
            .withParams(params)
            .build());
    }
}
