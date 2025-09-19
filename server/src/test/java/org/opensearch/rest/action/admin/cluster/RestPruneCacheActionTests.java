/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.cache.PruneCacheAction;
import org.opensearch.action.admin.cluster.cache.PruneCacheRequest;
import org.opensearch.action.admin.cluster.cache.PruneCacheResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for the {@link RestPruneCacheAction} class.
 * This test suite verifies the route registration, action name, and basic functionality.
 */
public class RestPruneCacheActionTests extends RestActionTestCase {

    private RestPruneCacheAction action;

    @Before
    public void setUpAction() {
        action = new RestPruneCacheAction();
        controller().registerHandler(action);
    }

    /**
     * Verifies that the action is correctly registered with the expected HTTP method and path.
     */
    public void testRoutes() {
        // Assert that the action registers exactly one route
        assertEquals(1, action.routes().size());
        // Assert that the method is POST and the path is correct
        assertEquals(RestRequest.Method.POST, action.routes().get(0).getMethod());
        assertEquals("/_cache/remote/prune", action.routes().get(0).getPath());
    }

    /**
     * Verifies that the action has the correct registered name.
     */
    public void testGetName() {
        assertEquals("prune_cache_action", action.getName());
    }

    /**
     * Tests basic request preparation without parameters.
     */
    public void testPrepareRequest() throws Exception {
        // Set up the verifier to return a mock response
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));
            return new PruneCacheResponse(true, 0);
        });

        // Create a fake REST request
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/remote/prune")
            .build();

        // This should not throw an exception
        dispatchRequest(request);
    }

    /**
     * Tests timeout parameter handling in the REST request.
     */
    public void testTimeoutParameterHandling() throws Exception {
        // Set up the verifier to return a mock response and verify timeout
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertEquals(PruneCacheAction.INSTANCE, actionType);
            assertThat(request, instanceOf(PruneCacheRequest.class));
            PruneCacheRequest pruneCacheRequest = (PruneCacheRequest) request;
            assertEquals(30000, pruneCacheRequest.timeout().getMillis());
            return new PruneCacheResponse(true, 1024);
        });

        // Create a fake REST request with timeout parameter
        Map<String, String> params = new HashMap<>();
        params.put("timeout", "30s");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_cache/remote/prune")
            .withParams(params)
            .build();

        // This should not throw an exception
        dispatchRequest(request);
    }

    /**
     * Tests that the action correctly reports it cannot trip circuit breaker.
     */
    public void testCanTripCircuitBreaker() {
        assertFalse("Prune cache action should not trip circuit breaker", action.canTripCircuitBreaker());
    }
}
