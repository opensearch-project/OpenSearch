/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for NodesResponse rendering correct structure.
 *
 * <p>Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
 *
 * <p>For any set of successful {@link DataFusionStatsNodeResponse} instances and
 * {@link FailedNodeException} failures, the rendered {@link DataFusionStatsNodesResponse}
 * JSON SHALL contain: a {@code _nodes} object with {@code total} equal to successes + failures,
 * {@code successful} equal to the number of successful responses, and {@code failed} equal to
 * the number of failures; a {@code cluster_name} string; and a {@code nodes} object where each
 * key is a node ID and each value contains {@code name}, {@code host}, and
 * {@code transport_address} fields from the node's {@link DiscoveryNode}.
 *
 * <p><b>Validates: Requirements 1.4, 1.5, 2.1, 2.2</b>
 */
public class NodesResponseStructurePropertyTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // ---- Generators ----

    /** Generates a non-negative long suitable for metrics. */
    private Arbitrary<Long> nonNegLong() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
    }

    @Provide
    Arbitrary<RuntimeMetrics> runtimeMetrics() {
        return nonNegLong().list()
            .ofSize(9)
            .map(l -> new RuntimeMetrics(l.get(0), l.get(1), l.get(2), l.get(3), l.get(4), l.get(5), l.get(6), l.get(7), l.get(8)));
    }

    @Provide
    Arbitrary<TaskMonitorStats> taskMonitorStats() {
        return Combinators.combine(nonNegLong(), nonNegLong(), nonNegLong()).as(TaskMonitorStats::new);
    }

    /** Generates a DataFusionStats instance with all sections populated. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStats() {
        return Combinators.combine(runtimeMetrics(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats())
            .as((io, cr, qe, sn, ps) -> {
                Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
                monitors.put("coordinator_reduce", cr);
                monitors.put("query_execution", qe);
                monitors.put("stream_next", sn);
                monitors.put("plan_setup", ps);
                return new DataFusionStats(
                    new NativeExecutorsStats(io, null, monitors),
                    new PartitionGateStats("datanode_gate", 12, 3, 100, 50, 0, 12),
                    new PartitionGateStats("coordinator_gate", 8, 1, 200, 75, 0, 8)
                );
            });
    }

    /**
     * Generates a unique node ID string (alphanumeric, 5-15 chars).
     */
    @Provide
    Arbitrary<String> nodeId() {
        return Arbitraries.strings().alpha().numeric().ofMinLength(5).ofMaxLength(15);
    }

    /**
     * Generates a node name string.
     */
    @Provide
    Arbitrary<String> nodeName() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(20).map(s -> "node-" + s);
    }

    /**
     * Generates a valid IPv4 address as a string (using 10.x.x.x range).
     */
    @Provide
    Arbitrary<String> hostAddress() {
        return Arbitraries.integers()
            .between(1, 254)
            .list()
            .ofSize(3)
            .map(octets -> "10." + octets.get(0) + "." + octets.get(1) + "." + octets.get(2));
    }

    /**
     * Generates a port number.
     */
    @Provide
    Arbitrary<Integer> port() {
        return Arbitraries.integers().between(9200, 9400);
    }

    /**
     * Generates a cluster name string.
     */
    @Provide
    Arbitrary<String> clusterName() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(20).map(s -> "cluster-" + s);
    }

    /**
     * Generates a list of DataFusionStatsNodeResponse instances (0 to 10 nodes).
     * Each node has a unique ID, name, host, and transport address.
     */
    @Provide
    Arbitrary<List<DataFusionStatsNodeResponse>> nodeResponses() {
        return Arbitraries.integers().between(0, 10).flatMap(count -> {
            if (count == 0) {
                return Arbitraries.just(Collections.emptyList());
            }
            return Combinators.combine(
                nodeId().list().ofSize(count).uniqueElements(),
                nodeName().list().ofSize(count),
                hostAddress().list().ofSize(count),
                port().list().ofSize(count),
                dataFusionStats().list().ofSize(count)
            ).as((ids, names, hosts, ports, statsList) -> {
                List<DataFusionStatsNodeResponse> responses = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    try {
                        DiscoveryNode node = new DiscoveryNode(
                            names.get(i),
                            ids.get(i),
                            new TransportAddress(InetAddress.getByName(hosts.get(i)), ports.get(i)),
                            Collections.emptyMap(),
                            Collections.emptySet(),
                            Version.CURRENT
                        );
                        responses.add(new DataFusionStatsNodeResponse(node, statsList.get(i)));
                    } catch (UnknownHostException e) {
                        throw new RuntimeException(e);
                    }
                }
                return responses;
            });
        });
    }

    /**
     * Generates a list of FailedNodeException instances (0 to 5 failures).
     */
    @Provide
    Arbitrary<List<FailedNodeException>> failures() {
        return Arbitraries.integers().between(0, 5).flatMap(count -> {
            if (count == 0) {
                return Arbitraries.just(Collections.emptyList());
            }
            return nodeId().list().ofSize(count).uniqueElements().map(ids -> {
                List<FailedNodeException> failureList = new ArrayList<>();
                for (String id : ids) {
                    failureList.add(new FailedNodeException(id, "node failure", new RuntimeException("test error")));
                }
                return failureList;
            });
        });
    }

    // ---- Property 1: NodesResponse renders correct structure ----

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that {@code _nodes.total} = successes + failures,
     * {@code _nodes.successful} = successes count, and
     * {@code _nodes.failed} = failures count.
     *
     * <p><b>Validates: Requirements 1.4, 1.5, 2.1, 2.2</b>
     */
    @Property(tries = 150)
    void nodesHeaderCountsAreCorrect(
        @ForAll("nodeResponses") List<DataFusionStatsNodeResponse> successResponses,
        @ForAll("failures") List<FailedNodeException> failureList,
        @ForAll("clusterName") String clusterNameStr
    ) throws IOException {
        ClusterName cluster = new ClusterName(clusterNameStr);
        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(cluster, successResponses, failureList);

        JsonNode root = renderAndParse(response);

        // Verify _nodes object
        JsonNode nodesHeader = root.get("_nodes");
        assertNotNull(nodesHeader, "_nodes object must be present");

        int expectedTotal = successResponses.size() + failureList.size();
        assertEquals(expectedTotal, nodesHeader.get("total").asInt(), "_nodes.total must equal successes + failures");
        assertEquals(
            successResponses.size(),
            nodesHeader.get("successful").asInt(),
            "_nodes.successful must equal number of successful responses"
        );
        assertEquals(failureList.size(), nodesHeader.get("failed").asInt(), "_nodes.failed must equal number of failures");
    }

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that {@code cluster_name} is present and matches the input.
     *
     * <p><b>Validates: Requirements 1.5</b>
     */
    @Property(tries = 150)
    void clusterNameIsPresentAndCorrect(
        @ForAll("nodeResponses") List<DataFusionStatsNodeResponse> successResponses,
        @ForAll("failures") List<FailedNodeException> failureList,
        @ForAll("clusterName") String clusterNameStr
    ) throws IOException {
        ClusterName cluster = new ClusterName(clusterNameStr);
        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(cluster, successResponses, failureList);

        JsonNode root = renderAndParse(response);

        // Verify cluster_name
        assertTrue(root.has("cluster_name"), "cluster_name field must be present");
        assertEquals(clusterNameStr, root.get("cluster_name").asText(), "cluster_name must match the input cluster name");
    }

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that the {@code nodes} object has exactly as many entries as
     * successful responses, and each entry is keyed by the node's ID.
     *
     * <p><b>Validates: Requirements 2.1</b>
     */
    @Property(tries = 150)
    void nodesObjectHasCorrectEntries(
        @ForAll("nodeResponses") List<DataFusionStatsNodeResponse> successResponses,
        @ForAll("failures") List<FailedNodeException> failureList,
        @ForAll("clusterName") String clusterNameStr
    ) throws IOException {
        ClusterName cluster = new ClusterName(clusterNameStr);
        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(cluster, successResponses, failureList);

        JsonNode root = renderAndParse(response);

        // Verify nodes object
        JsonNode nodesObj = root.get("nodes");
        assertNotNull(nodesObj, "nodes object must be present");
        assertEquals(successResponses.size(), nodesObj.size(), "nodes object must have exactly as many entries as successful responses");

        // Verify each node entry is keyed by node ID
        for (DataFusionStatsNodeResponse nodeResp : successResponses) {
            String expectedId = nodeResp.getNode().getId();
            assertTrue(nodesObj.has(expectedId), "nodes object must contain entry for node ID: " + expectedId);
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 1: NodesResponse renders correct structure
     *
     * <p>Verifies that each node entry in {@code nodes} contains {@code name},
     * Verifies that node entries do NOT contain {@code name}, {@code host},
     * or {@code transport_address} fields (KNN pattern — no IP exposure).
     *
     * <p><b>Validates: Security — no private IP leakage</b>
     */
    @Property(tries = 150)
    void eachNodeEntryDoesNotContainMetadataFields(
        @ForAll("nodeResponses") List<DataFusionStatsNodeResponse> successResponses,
        @ForAll("failures") List<FailedNodeException> failureList,
        @ForAll("clusterName") String clusterNameStr
    ) throws IOException {
        ClusterName cluster = new ClusterName(clusterNameStr);
        DataFusionStatsNodesResponse response = new DataFusionStatsNodesResponse(cluster, successResponses, failureList);

        JsonNode root = renderAndParse(response);
        JsonNode nodesObj = root.get("nodes");

        for (DataFusionStatsNodeResponse nodeResp : successResponses) {
            String nodeId = nodeResp.getNode().getId();
            JsonNode nodeEntry = nodesObj.get(nodeId);
            assertNotNull(nodeEntry, "Node entry must exist for ID: " + nodeId);

            // Verify metadata fields are NOT present
            assertFalse(nodeEntry.has("name"), "Node entry must NOT contain 'name' field for node: " + nodeId);
            assertFalse(nodeEntry.has("host"), "Node entry must NOT contain 'host' field for node: " + nodeId);
            assertFalse(nodeEntry.has("transport_address"), "Node entry must NOT contain 'transport_address' field for node: " + nodeId);
        }
    }

    // ---- Helper methods ----

    /**
     * Renders the response to JSON and parses it into a Jackson JsonNode.
     */
    private JsonNode renderAndParse(DataFusionStatsNodesResponse response) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return MAPPER.readTree(builder.toString());
    }
}
