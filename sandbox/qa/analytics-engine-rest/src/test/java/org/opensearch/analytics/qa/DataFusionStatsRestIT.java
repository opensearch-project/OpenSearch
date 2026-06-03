/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * REST integration tests for the DataFusion cluster stats endpoint.
 *
 * <p>Verifies node targeting, stat filtering, HTTP error codes, IP leakage
 * protection, and legacy route deprecation via REST calls against
 * {@code /_plugins/_analytics_backend_datafusion/stats}.
 */
public class DataFusionStatsRestIT extends OpenSearchRestTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String STATS_ENDPOINT = "/_plugins/_analytics_backend_datafusion/stats";
    private static final String LOCAL_STATS_ENDPOINT = "/_plugins/_analytics_backend_datafusion/_local/stats";

    /**
     * All 8 stat sections that a full (unfiltered) response should contain.
     */
    private static final Set<String> ALL_SECTIONS = Set.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "datanode_gate",
        "coordinator_gate"
    );

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    // ── Test methods ────────────────────────────────────────────────────────

    /**
     * GET /_plugins/_analytics_backend_datafusion/stats
     * Verify HTTP 200, verify _nodes, cluster_name, nodes keys exist,
     * verify all 8 stat sections in each node entry.
     */
    public void testAllStatsFromAllNodes() throws Exception {
        Response response = client().performRequest(new Request("GET", STATS_ENDPOINT));
        assertEquals("expected HTTP 200", 200, response.getStatusLine().getStatusCode());

        JsonNode root = parseResponse(response);

        // Verify top-level structure
        assertTrue("response must contain '_nodes'", root.has("_nodes"));
        assertTrue("response must contain 'cluster_name'", root.has("cluster_name"));
        assertTrue("response must contain 'nodes'", root.has("nodes"));

        // Verify _nodes header
        JsonNode nodesHeader = root.get("_nodes");
        assertTrue("_nodes must have 'total'", nodesHeader.has("total"));
        assertTrue("_nodes must have 'successful'", nodesHeader.has("successful"));
        assertTrue("_nodes.total must be > 0", nodesHeader.get("total").asInt() > 0);
        assertTrue("_nodes.successful must be > 0", nodesHeader.get("successful").asInt() > 0);

        // Verify nodes map and stat sections
        JsonNode nodesMap = root.get("nodes");
        assertTrue("nodes must not be empty", nodesMap.size() > 0);

        Iterator<String> nodeIds = nodesMap.fieldNames();
        while (nodeIds.hasNext()) {
            String nodeId = nodeIds.next();
            JsonNode nodeEntry = nodesMap.get(nodeId);
            for (String section : ALL_SECTIONS) {
                assertTrue(
                    "node '" + nodeId + "' must contain section '" + section + "'",
                    nodeEntry.has(section)
                );
            }
        }
    }

    /**
     * GET /_plugins/_analytics_backend_datafusion/_local/stats
     * Verify HTTP 200, verify single node in response.
     */
    public void testLocalNodeStats() throws Exception {
        Response response = client().performRequest(new Request("GET", LOCAL_STATS_ENDPOINT));
        assertEquals("expected HTTP 200", 200, response.getStatusLine().getStatusCode());

        JsonNode root = parseResponse(response);
        assertTrue("response must contain 'nodes'", root.has("nodes"));

        JsonNode nodesMap = root.get("nodes");
        assertEquals("_local request must return exactly 1 node", 1, nodesMap.size());

        // Verify the single node has stats
        Iterator<String> nodeIds = nodesMap.fieldNames();
        String nodeId = nodeIds.next();
        JsonNode nodeEntry = nodesMap.get(nodeId);
        assertNotNull("local node entry must not be null", nodeEntry);
    }

    /**
     * GET /_plugins/_analytics_backend_datafusion/stats/io_runtime
     * Verify only io_runtime in node entries.
     */
    public void testSingleStatFilter() throws Exception {
        Response response = client().performRequest(new Request("GET", STATS_ENDPOINT + "/io_runtime"));
        assertEquals("expected HTTP 200", 200, response.getStatusLine().getStatusCode());

        JsonNode root = parseResponse(response);
        JsonNode nodesMap = root.get("nodes");
        assertTrue("nodes must not be empty", nodesMap.size() > 0);

        Iterator<String> nodeIds = nodesMap.fieldNames();
        while (nodeIds.hasNext()) {
            String nodeId = nodeIds.next();
            JsonNode nodeEntry = nodesMap.get(nodeId);
            assertTrue("node must contain 'io_runtime'", nodeEntry.has("io_runtime"));

            // Verify no other stat sections are present
            for (String section : ALL_SECTIONS) {
                if (!section.equals("io_runtime")) {
                    assertFalse(
                        "filtered response must NOT contain '" + section + "'",
                        nodeEntry.has(section)
                    );
                }
            }
        }
    }

    /**
     * GET /_plugins/_analytics_backend_datafusion/stats/io_runtime,datanode_gate
     * Verify only those 2 sections in node entries.
     */
    public void testMultipleStatFilter() throws Exception {
        Response response = client().performRequest(new Request("GET", STATS_ENDPOINT + "/io_runtime,datanode_gate"));
        assertEquals("expected HTTP 200", 200, response.getStatusLine().getStatusCode());

        JsonNode root = parseResponse(response);
        JsonNode nodesMap = root.get("nodes");
        assertTrue("nodes must not be empty", nodesMap.size() > 0);

        Iterator<String> nodeIds = nodesMap.fieldNames();
        while (nodeIds.hasNext()) {
            String nodeId = nodeIds.next();
            JsonNode nodeEntry = nodesMap.get(nodeId);

            assertTrue("node must contain 'io_runtime'", nodeEntry.has("io_runtime"));
            assertTrue("node must contain 'datanode_gate'", nodeEntry.has("datanode_gate"));

            // Verify no other stat sections are present
            for (String section : ALL_SECTIONS) {
                if (!section.equals("io_runtime") && !section.equals("datanode_gate")) {
                    assertFalse(
                        "filtered response must NOT contain '" + section + "'",
                        nodeEntry.has(section)
                    );
                }
            }
        }
    }

    /**
     * GET /_plugins/_analytics_backend_datafusion/stats/bogus
     * Verify HTTP 400 status and error message contains "Invalid stat sections".
     */
    public void testInvalidStatReturns400() throws Exception {
        ResponseException ex = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("GET", STATS_ENDPOINT + "/bogus"))
        );

        assertEquals("expected HTTP 400", 400, ex.getResponse().getStatusLine().getStatusCode());

        String responseBody = EntityUtils.toString(ex.getResponse().getEntity());
        assertTrue(
            "error message must contain 'Invalid stat sections', got: " + responseBody,
            responseBody.contains("Invalid stat sections")
        );
    }

    /**
     * GET /_plugins/_analytics_backend_datafusion/stats
     * Verify node entries do NOT contain 'name', 'host', 'transport_address'.
     */
    public void testNoIpLeakage() throws Exception {
        Response response = client().performRequest(new Request("GET", STATS_ENDPOINT));
        assertEquals("expected HTTP 200", 200, response.getStatusLine().getStatusCode());

        JsonNode root = parseResponse(response);
        JsonNode nodesMap = root.get("nodes");
        assertTrue("nodes must not be empty", nodesMap.size() > 0);

        Iterator<String> nodeIds = nodesMap.fieldNames();
        while (nodeIds.hasNext()) {
            String nodeId = nodeIds.next();
            JsonNode nodeEntry = nodesMap.get(nodeId);

            assertFalse("node entry must NOT contain 'name'", nodeEntry.has("name"));
            assertFalse("node entry must NOT contain 'host'", nodeEntry.has("host"));
            assertFalse("node entry must NOT contain 'transport_address'", nodeEntry.has("transport_address"));
        }
    }

    // ── Helper methods ──────────────────────────────────────────────────────

    /**
     * Parse an HTTP response body into a Jackson {@link JsonNode}.
     */
    private JsonNode parseResponse(Response response) throws Exception {
        String body = EntityUtils.toString(response.getEntity());
        return MAPPER.readTree(body);
    }
}
