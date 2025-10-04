/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.cache.NodePruneCacheResponse;
import org.opensearch.action.admin.cluster.cache.PruneCacheAction;
import org.opensearch.action.admin.cluster.cache.PruneCacheRequest;
import org.opensearch.action.admin.cluster.cache.PruneCacheResponse;
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

    private PruneCacheResponse createMockMultiNodeResponse() {
        DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn("node1");
        when(mockNode.getName()).thenReturn("test-node");
        when(mockNode.getHostName()).thenReturn("localhost");
        when(mockNode.getHostAddress()).thenReturn("127.0.0.1");
        when(mockNode.getAddress()).thenReturn(buildNewFakeTransportAddress());

        List<NodePruneCacheResponse> nodeResponses = Arrays.asList(new NodePruneCacheResponse(mockNode, 1024L, 10737418240L));

        return new PruneCacheResponse(new ClusterName("test-cluster"), nodeResponses, Collections.emptyList());
    }

    public void testRoutes() {
        assertEquals(1, action.routes().size());
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
        assertEquals("/_cache/filecache/prune", action.routes().get(0).getPath());
    }

    /**
     * Verifies that the action has the correct registered name.
     */
    public void testGetName() {
        assertEquals("prune_cache_action", action.getName());
    }

    /**
     * Tests basic request preparation without parameters (all defaults).
     */
    public void testPrepareRequestDefaults() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            assertNull("Default should target all nodes", pruneCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests enhanced parameter handling.
     */
    public void testEnhancedParameterHandling() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            assertArrayEquals("Node targeting should work", new String[] { "node1", "node2" }, pruneCacheRequest.nodesIds());
            assertEquals("Timeout should be set", 30000, pruneCacheRequest.timeout().getMillis());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "node1,node2");
        params.put("timeout", "30s");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests single node parameter handling.
     */
    public void testSingleNodeParameter() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            assertArrayEquals("Single node should be targeted", new String[] { "node1" }, pruneCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("node", "node1");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests nodes parameter takes priority over node parameter.
     */
    public void testParameterPriority() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;

            assertArrayEquals("Nodes parameter should take priority", new String[] { "node1", "node2" }, pruneCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "node1,node2");
        params.put("node", "node3");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests warm node targeting syntax.
     */
    public void testWarmNodeTargetingSyntax() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            assertArrayEquals("Should target warm nodes", new String[] { "warm:true" }, pruneCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "warm:true");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests parameter cleaning and validation.
     */
    public void testParameterCleaning() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            // Should clean whitespace and filter empty strings
            assertArrayEquals("Should clean parameters", new String[] { "node1", "node2" }, pruneCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", " node1 ,  node2  , ,   ");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
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
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            assertArrayEquals("Node targeting should work", new String[] { "warm-node-1", "warm-node-2" }, pruneCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        Map<String, String> params = new HashMap<>();
        params.put("nodes", "warm-node-1,warm-node-2");
        params.put("timeout", "5m");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests empty parameter handling edge cases.
     */
    public void testEmptyParameterHandling() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            assertNull("Empty nodes parameter should result in null", pruneCacheRequest.nodesIds());

            return createMockMultiNodeResponse();
        });

        // Test empty parameters
        Map<String, String> params = new HashMap<>();
        params.put("nodes", "");
        params.put("node", "");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }

    /**
     * Tests large node list parameter handling.
     */
    public void testLargeNodeListHandling() throws Exception {
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));

            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;

            assertEquals("Should handle large node list", 10, pruneCacheRequest.nodesIds().length);

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
            .withPath("/_cache/filecache/prune")
            .withParams(params)
            .build();

        dispatchRequest(request);
    }
}
