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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestHandler.Route;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link RestDataFusionStatsAction}.
 *
 * <p>Verifies route registration, path parameter parsing ({@code nodeId} and
 * {@code stat}), and HTTP 400 for invalid stat names.
 *
 * @opensearch.internal
 */
public class RestDataFusionStatsActionTests extends OpenSearchTestCase {

    private RestDataFusionStatsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestDataFusionStatsAction();
    }

    // ---- Test: 4 canonical routes registered with correct paths and methods ----

    public void testRoutesRegistered() {
        List<Route> routes = action.routes();
        assertEquals("Expected 4 canonical routes", 4, routes.size());

        Set<String> expectedPaths = Set.of(
            "/_plugins/_analytics_backend_datafusion/{nodeId}/stats/{stat}",
            "/_plugins/_analytics_backend_datafusion/{nodeId}/stats",
            "/_plugins/_analytics_backend_datafusion/stats/{stat}",
            "/_plugins/_analytics_backend_datafusion/stats"
        );

        Set<String> actualPaths = routes.stream().map(Route::getPath).collect(Collectors.toSet());
        assertEquals(expectedPaths, actualPaths);

        // All routes should be GET
        for (Route route : routes) {
            assertEquals(GET, route.getMethod());
        }
    }

    // ---- Test: nodeId path parameter parsing ----

    public void testNodeIdParsing() throws Exception {
        AtomicReference<DataFusionStatsNodesRequest> capturedRequest = new AtomicReference<>();

        try (NodeClient client = buildCapturingClient(capturedRequest)) {
            // Single node ID
            Map<String, String> params = new HashMap<>();
            params.put("nodeId", "node1");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/node1/stats"
            ).withParams(params).build();

            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            action.handleRequest(request, channel, client);

            assertNotNull(capturedRequest.get());
            assertArrayEquals(new String[] { "node1" }, capturedRequest.get().nodesIds());
        }

        // Multiple comma-separated node IDs
        capturedRequest.set(null);
        try (NodeClient client = buildCapturingClient(capturedRequest)) {
            Map<String, String> params = new HashMap<>();
            params.put("nodeId", "node1,node2,node3");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/node1,node2,node3/stats"
            ).withParams(params).build();

            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            action.handleRequest(request, channel, client);

            assertNotNull(capturedRequest.get());
            assertArrayEquals(new String[] { "node1", "node2", "node3" }, capturedRequest.get().nodesIds());
        }

        // _local special value
        capturedRequest.set(null);
        try (NodeClient client = buildCapturingClient(capturedRequest)) {
            Map<String, String> params = new HashMap<>();
            params.put("nodeId", "_local");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/_local/stats"
            ).withParams(params).build();

            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            action.handleRequest(request, channel, client);

            assertNotNull(capturedRequest.get());
            assertArrayEquals(new String[] { "_local" }, capturedRequest.get().nodesIds());
        }
    }

    // ---- Test: stat path parameter parsing ----

    public void testStatParsing() throws Exception {
        AtomicReference<DataFusionStatsNodesRequest> capturedRequest = new AtomicReference<>();

        // Single stat
        try (NodeClient client = buildCapturingClient(capturedRequest)) {
            Map<String, String> params = new HashMap<>();
            params.put("stat", "io_runtime");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/stats/io_runtime"
            ).withParams(params).build();

            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            action.handleRequest(request, channel, client);

            assertNotNull(capturedRequest.get());
            assertEquals(Set.of("io_runtime"), capturedRequest.get().getStatsToRetrieve());
        }

        // Multiple comma-separated stats
        capturedRequest.set(null);
        try (NodeClient client = buildCapturingClient(capturedRequest)) {
            Map<String, String> params = new HashMap<>();
            params.put("stat", "io_runtime,cpu_runtime,datanode_gate");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/stats/io_runtime,cpu_runtime,datanode_gate"
            ).withParams(params).build();

            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            action.handleRequest(request, channel, client);

            assertNotNull(capturedRequest.get());
            assertEquals(Set.of("io_runtime", "cpu_runtime", "datanode_gate"), capturedRequest.get().getStatsToRetrieve());
        }
    }

    // ---- Test: invalid stat name returns HTTP 400 ----

    public void testInvalidStatReturns400() throws Exception {
        try (NodeClient client = new NoOpNodeClient(getTestName())) {
            Map<String, String> params = new HashMap<>();
            params.put("stat", "invalid_stat_name");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/stats/invalid_stat_name"
            ).withParams(params).build();

            FakeRestChannel channel = new FakeRestChannel(request, true, 1);
            action.handleRequest(request, channel, client);

            RestResponse response = channel.capturedResponse();
            assertNotNull("Expected a response to be sent", response);
            assertEquals(RestStatus.BAD_REQUEST, response.status());

            String responseBody = response.content().utf8ToString();
            assertThat(responseBody, containsString("Invalid stat sections"));
            assertThat(responseBody, containsString("invalid_stat_name"));
            assertThat(responseBody, containsString("io_runtime"));
            assertThat(responseBody, containsString("cpu_runtime"));
            assertThat(responseBody, containsString("coordinator_reduce"));
            assertThat(responseBody, containsString("query_execution"));
            assertThat(responseBody, containsString("stream_next"));
            assertThat(responseBody, containsString("plan_setup"));
            assertThat(responseBody, containsString("datanode_gate"));
            assertThat(responseBody, containsString("coordinator_gate"));
        }
    }

    // ---- Test: no nodeId defaults to all nodes (empty array) ----

    public void testNoNodeIdDefaultsToAllNodes() throws Exception {
        AtomicReference<DataFusionStatsNodesRequest> capturedRequest = new AtomicReference<>();

        try (NodeClient client = buildCapturingClient(capturedRequest)) {
            Map<String, String> params = new HashMap<>();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_plugins/_analytics_backend_datafusion/stats")
                .withParams(params)
                .build();

            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            action.handleRequest(request, channel, client);

            assertNotNull("Request should have been captured", capturedRequest.get());
            assertEquals("Empty nodeIds means all nodes", 0, capturedRequest.get().nodesIds().length);
        }
    }

    // ---- Test: no stat defaults to all stats (empty set) ----

    public void testNoStatDefaultsToAllStats() throws Exception {
        AtomicReference<DataFusionStatsNodesRequest> capturedRequest = new AtomicReference<>();

        try (NodeClient client = buildCapturingClient(capturedRequest)) {
            Map<String, String> params = new HashMap<>();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_plugins/_analytics_backend_datafusion/stats")
                .withParams(params)
                .build();

            FakeRestChannel channel = new FakeRestChannel(request, false, 1);
            action.handleRequest(request, channel, client);

            assertNotNull("Request should have been captured", capturedRequest.get());
            assertTrue("Empty statsToRetrieve means all stats", capturedRequest.get().getStatsToRetrieve().isEmpty());
        }
    }

    // ---- Test: mixed valid and invalid stat returns 400 for the invalid one ----

    public void testMixedValidAndInvalidStatReturns400() throws Exception {
        try (NodeClient client = new NoOpNodeClient(getTestName())) {
            Map<String, String> params = new HashMap<>();
            params.put("stat", "io_runtime,bogus_stat");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(
                "/_plugins/_analytics_backend_datafusion/stats/io_runtime,bogus_stat"
            ).withParams(params).build();

            FakeRestChannel channel = new FakeRestChannel(request, true, 1);
            action.handleRequest(request, channel, client);

            RestResponse response = channel.capturedResponse();
            assertNotNull("Expected a response to be sent", response);
            assertEquals(RestStatus.BAD_REQUEST, response.status());

            String responseBody = response.content().utf8ToString();
            assertThat(responseBody, containsString("Invalid stat sections"));
            assertThat(responseBody, containsString("bogus_stat"));
        }
    }

    // ---- Test: handler name ----

    public void testGetName() {
        assertEquals("datafusion_stats_action", action.getName());
    }

    public void testSpillIsAcceptedAsValidStatName() {
        // The REST handler exposes VALID_STATS for error messages; assert "disk_spill" is now in it.
        assertTrue(
            "VALID_STATS should include 'disk_spill' but was: " + RestDataFusionStatsAction.VALID_STATS,
            RestDataFusionStatsAction.VALID_STATS.contains("disk_spill")
        );
    }

    // ---- Helper: build a NodeClient that captures the DataFusionStatsNodesRequest ----

    @SuppressWarnings("unchecked")
    private NodeClient buildCapturingClient(AtomicReference<DataFusionStatsNodesRequest> capturedRequest) {
        return new NoOpNodeClient(getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof DataFusionStatsNodesRequest) {
                    capturedRequest.set((DataFusionStatsNodesRequest) request);
                }
                // Don't call listener - we just want to capture the request
            }
        };
    }
}
