/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.filecache.NodePruneFileCacheResponse;
import org.opensearch.action.admin.cluster.filecache.PruneFileCacheAction;
import org.opensearch.action.admin.cluster.filecache.PruneFileCacheRequest;
import org.opensearch.action.admin.cluster.filecache.PruneFileCacheResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the enhanced {@link RestPruneCacheAction} class.
 * Covers route registration, parameter parsing, and multi-node response handling.
 */
public class RestPruneCacheActionTests extends RestActionTestCase {

    private RestPruneCacheAction action;

    @Before
    public void setUpAction() {
        action = new RestPruneCacheAction();
        controller().registerHandler(action);
    }

    private PruneFileCacheResponse createMockMultiNodeResponse() {
        DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("node1");
        when(mockNode.getName()).thenReturn("test-node");
        when(mockNode.getHostName()).thenReturn("localhost");
        when(mockNode.getHostAddress()).thenReturn("127.0.0.1");
        when(mockNode.getAddress()).thenReturn(buildNewFakeTransportAddress());

        List<NodePruneFileCacheResponse> nodeResponses = Arrays.asList(new NodePruneFileCacheResponse(mockNode, 1024L, 10737418240L));

        return new PruneFileCacheResponse(new ClusterName("test-cluster"), nodeResponses, Collections.emptyList());
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
        assertEquals("/_filecache/prune", action.routes().get(0).getPath());
    }

    /**
     * Verifies that the action has the correct registered name.
     */
    public void testGetName() {
        assertEquals("prune_filecache_action", action.getName());
    }

    /**
     * Tests basic request preparation without parameters (all defaults).
     */
    public void testPrepareRequestDefaults() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;
            assertNull("Default should target all nodes", pruneFileCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests enhanced parameter handling.
     */
    public void testEnhancedParameterHandling() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;
            assertArrayEquals("Node targeting should work", new String[] { "node1", "node2" }, pruneFileCacheRequest.nodesIds());
            assertEquals("Timeout should be set", 30000, pruneFileCacheRequest.timeout().getMillis());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "node1,node2");
        params.put("timeout", "30s");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests single node parameter handling.
     */
    public void testSingleNodeParameter() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;
            assertArrayEquals("Single node should be targeted", new String[] { "node1" }, pruneFileCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("node", "node1");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests nodes parameter takes priority over node parameter.
     */
    public void testParameterPriority() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;

            assertArrayEquals("Nodes parameter should take priority", new String[] { "node1", "node2" }, pruneFileCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "node1,node2");
        params.put("node", "node3");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests warm node targeting syntax.
     */
    public void testWarmNodeTargetingSyntax() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;
            assertArrayEquals("Should target warm nodes", new String[] { "warm:true" }, pruneFileCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "warm:true");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests parameter cleaning and validation.
     */
    public void testParameterCleaning() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;
            // Should clean whitespace and filter empty strings
            assertArrayEquals("Should clean parameters", new String[] { "node1", "node2" }, pruneFileCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", " node1 ,  node2  , ,   ");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests that the action correctly reports it cannot trip circuit breaker.
     */
    public void testCanTripCircuitBreaker() {
        assertFalse("Prune cache action should not trip circuit breaker", action.canTripCircuitBreaker());
    }

    /**
     * Tests comprehensive parameter combinations.
     */
    public void testComprehensiveParameterCombinations() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;
            assertArrayEquals(
                "Node targeting should work",
                new String[] { "warm-node-1", "warm-node-2" },
                pruneFileCacheRequest.nodesIds()
            );

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "warm-node-1,warm-node-2");
        params.put("timeout", "5m");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests empty parameter handling edge cases.
     */
    public void testEmptyParameterHandling() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;
            assertNull("Empty nodes parameter should result in null", pruneFileCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        // Test empty parameters
        Map<String, String> params = new HashMap<>();
        params.put("nodes", "");
        params.put("node", "");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests large node list parameter handling.
     */
    public void testLargeNodeListHandling() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneFileCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneFileCacheRequest.class));

            PruneFileCacheRequest pruneFileCacheRequest = (PruneFileCacheRequest) request;

            assertEquals("Should handle large node list", 10, pruneFileCacheRequest.nodesIds().length);

            return createMockMultiNodeResponse();
        });

        // Test large node list
        StringBuilder largeNodeList = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            if (i > 0) largeNodeList.append(",");
            largeNodeList.append("node").append(i);
        }

        Map<String, String> params = new HashMap<>();
        params.put("nodes", largeNodeList.toString());

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }
}
