/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Unit tests for {@link RestClearCacheAction}.
 */
public class RestClearCacheActionTests extends OpenSearchTestCase {

    private RestClearCacheAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestClearCacheAction();
    }

    public void testSinglePostRoute() {
        List<Route> routes = action.routes();
        assertEquals(1, routes.size());
        assertEquals(POST, routes.get(0).getMethod());
        assertEquals("/_plugins/_analytics_backend_datafusion/cache/_clear", routes.get(0).getPath());
    }

    public void testName() {
        assertEquals("datafusion_clear_cache_action", action.getName());
    }

    public void testDoesNotTripCircuitBreaker() {
        assertFalse(action.canTripCircuitBreaker());
    }

    public void testNoParamsSendsClearAllRequest() throws Exception {
        ClearCacheNodesRequest req = captureRequest(Map.of());
        assertFalse(req.isFooter());
        assertFalse(req.isColumn());
        assertFalse(req.isOffset());
        assertFalse(req.isStatistics());
        assertTrue("no params → isClearAll()", req.isClearAll());
    }

    public void testFooterParamSetsFooterFlag() throws Exception {
        ClearCacheNodesRequest req = captureRequest(Map.of("footer", "true"));
        assertTrue(req.isFooter());
        assertFalse(req.isColumn());
        assertFalse(req.isOffset());
        assertFalse(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testColumnParamSetsColumnFlag() throws Exception {
        ClearCacheNodesRequest req = captureRequest(Map.of("column", "true"));
        assertFalse(req.isFooter());
        assertTrue(req.isColumn());
        assertFalse(req.isOffset());
        assertFalse(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testOffsetParamSetsOffsetFlag() throws Exception {
        ClearCacheNodesRequest req = captureRequest(Map.of("offset", "true"));
        assertFalse(req.isFooter());
        assertFalse(req.isColumn());
        assertTrue(req.isOffset());
        assertFalse(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testStatisticsParamSetsStatisticsFlag() throws Exception {
        ClearCacheNodesRequest req = captureRequest(Map.of("statistics", "true"));
        assertFalse(req.isFooter());
        assertFalse(req.isColumn());
        assertFalse(req.isOffset());
        assertTrue(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testMultipleParamsSetsMultipleFlags() throws Exception {
        ClearCacheNodesRequest req = captureRequest(Map.of("column", "true", "offset", "true"));
        assertFalse(req.isFooter());
        assertTrue(req.isColumn());
        assertTrue(req.isOffset());
        assertFalse(req.isStatistics());
        assertFalse(req.isClearAll());
    }

    public void testFalseParamDoesNotSetFlag() throws Exception {
        ClearCacheNodesRequest req = captureRequest(Map.of("footer", "false", "column", "true"));
        assertFalse(req.isFooter());
        assertTrue(req.isColumn());
        assertFalse(req.isClearAll());
    }

    @SuppressWarnings("unchecked")
    private ClearCacheNodesRequest captureRequest(Map<String, String> params) throws Exception {
        AtomicReference<ClearCacheNodesRequest> captured = new AtomicReference<>();
        try (NodeClient client = new NoOpNodeClient(getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> actionType,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof ClearCacheNodesRequest) {
                    captured.set((ClearCacheNodesRequest) request);
                }
            }
        }) {
            FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/cache/_clear"
            ).withParams(new HashMap<>(params)).build();
            FakeRestChannel channel = new FakeRestChannel(restRequest, false, 1);
            action.handleRequest(restRequest, channel, client);
        }
        assertNotNull("request must have been captured", captured.get());
        return captured.get();
    }
}
