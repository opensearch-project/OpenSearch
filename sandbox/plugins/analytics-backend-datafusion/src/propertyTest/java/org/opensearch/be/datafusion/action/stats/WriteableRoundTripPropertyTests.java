/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.Version;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Property-based tests for Writeable round-trip serialization of transport objects.
 *
 * <p>Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
 *
 * <p>For any {@link DataFusionStatsNodesRequest}, {@link DataFusionStatsNodeRequest},
 * {@link DataFusionStatsNodeResponse}, or {@link DataFusionStatsNodesResponse},
 * serializing to a {@code StreamOutput} and deserializing from the resulting
 * {@code StreamInput} produces an object equal to the original.
 *
 * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
 */
public class WriteableRoundTripPropertyTests {

    /** Valid stat section names for generating filter sets. */
    private static final List<String> ALL_SECTIONS = List.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "datanode_gate",
        "coordinator_gate"
    );

    // ═══════════════════════════════════════════════════════════════════
    // Generators
    // ═══════════════════════════════════════════════════════════════════

    @Provide
    Arbitrary<RuntimeMetrics> runtimeMetrics() {
        return Arbitraries.longs()
            .between(0, Long.MAX_VALUE / 2)
            .list()
            .ofSize(9)
            .map(l -> new RuntimeMetrics(l.get(0), l.get(1), l.get(2), l.get(3), l.get(4), l.get(5), l.get(6), l.get(7), l.get(8)));
    }

    @Provide
    Arbitrary<TaskMonitorStats> taskMonitorStats() {
        Arbitrary<Long> nonNeg = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNeg, nonNeg, nonNeg).as(TaskMonitorStats::new);
    }

    @Provide
    Arbitrary<PartitionGateStats> partitionGateStats() {
        Arbitrary<String> names = Arbitraries.of("datanode_gate", "coordinator_gate");
        Arbitrary<Long> nonNeg = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(names, nonNeg, nonNeg, nonNeg, nonNeg).as(PartitionGateStats::new);
    }

    @Provide
    Arbitrary<DataFusionStats> dataFusionStats() {
        return Arbitraries.oneOf(dataFusionStatsWithCpu(), dataFusionStatsWithoutCpu());
    }

    private Arbitrary<DataFusionStats> dataFusionStatsWithCpu() {
        return Combinators.combine(
            runtimeMetrics(),
            runtimeMetrics(),
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats()
        ).as((io, cpu, cr, qe, sn, ps) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("coordinator_reduce", cr);
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("plan_setup", ps);
            return new DataFusionStats(
                new NativeExecutorsStats(io, cpu, monitors),
                new PartitionGateStats("datanode_gate", 12, 3, 100, 50),
                new PartitionGateStats("coordinator_gate", 8, 1, 200, 75),
                null
            );
        });
    }

    private Arbitrary<DataFusionStats> dataFusionStatsWithoutCpu() {
        return Combinators.combine(runtimeMetrics(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats())
            .as((io, cr, qe, sn, ps) -> {
                Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
                monitors.put("coordinator_reduce", cr);
                monitors.put("query_execution", qe);
                monitors.put("stream_next", sn);
                monitors.put("plan_setup", ps);
                return new DataFusionStats(
                    new NativeExecutorsStats(io, null, monitors),
                    new PartitionGateStats("datanode_gate", 12, 3, 100, 50),
                    new PartitionGateStats("coordinator_gate", 8, 1, 200, 75),
                    null
                );
            });
    }

    /** Generates a set of stat section names (possibly empty). */
    @Provide
    Arbitrary<Set<String>> statsToRetrieve() {
        return Arbitraries.of(ALL_SECTIONS).set().ofMinSize(0).ofMaxSize(8);
    }

    /** Generates an array of node IDs (possibly empty). */
    @Provide
    Arbitrary<String[]> nodeIds() {
        return Arbitraries.strings()
            .alpha()
            .ofMinLength(3)
            .ofMaxLength(10)
            .list()
            .ofMinSize(0)
            .ofMaxSize(5)
            .map(list -> list.toArray(new String[0]));
    }

    /** Generates a DiscoveryNode with a stable loopback address. */
    @Provide
    Arbitrary<DiscoveryNode> discoveryNode() {
        Arbitrary<String> nodeId = Arbitraries.strings().alpha().ofMinLength(5).ofMaxLength(12);
        Arbitrary<String> nodeName = Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(10);
        Arbitrary<Integer> port = Arbitraries.integers().between(1024, 65535);
        return Combinators.combine(nodeId, nodeName, port).as((id, name, p) -> {
            try {
                return new DiscoveryNode(
                    name,
                    id,
                    new TransportAddress(InetAddress.getByName("127.0.0.1"), p),
                    Collections.emptyMap(),
                    Collections.emptySet(),
                    Version.CURRENT
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodesRequest
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodesRequest, serialize → deserialize produces an
     * object with equal nodesIds and statsToRetrieve.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    @Property(tries = 100)
    void nodesRequestRoundTrip(@ForAll("nodeIds") String[] nodeIds, @ForAll("statsToRetrieve") Set<String> statsFilter) throws IOException {
        DataFusionStatsNodesRequest original = new DataFusionStatsNodesRequest(nodeIds, statsFilter);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        DataFusionStatsNodesRequest deserialized = new DataFusionStatsNodesRequest(in);

        // Verify nodesIds
        assertEquals(
            new HashSet<>(Arrays.asList(original.nodesIds())),
            new HashSet<>(Arrays.asList(deserialized.nodesIds())),
            "nodesIds must survive round-trip"
        );

        // Verify statsToRetrieve
        assertEquals(original.getStatsToRetrieve(), deserialized.getStatsToRetrieve(), "statsToRetrieve must survive round-trip");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodeRequest
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodeRequest, serialize → deserialize produces an
     * object with equal statsToRetrieve.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    @Property(tries = 100)
    void nodeRequestRoundTrip(@ForAll("statsToRetrieve") Set<String> statsFilter) throws IOException {
        // Create a NodesRequest first, then derive the NodeRequest from it
        DataFusionStatsNodesRequest nodesRequest = new DataFusionStatsNodesRequest(new String[0], statsFilter);
        DataFusionStatsNodeRequest original = new DataFusionStatsNodeRequest(nodesRequest);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        DataFusionStatsNodeRequest deserialized = new DataFusionStatsNodeRequest(in);

        // Verify statsToRetrieve
        assertEquals(original.getStatsToRetrieve(), deserialized.getStatsToRetrieve(), "statsToRetrieve must survive round-trip");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodeResponse
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodeResponse, serialize → deserialize produces an
     * equal object (node + stats).
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    @Property(tries = 100)
    void nodeResponseRoundTrip(@ForAll("discoveryNode") DiscoveryNode node, @ForAll("dataFusionStats") DataFusionStats stats)
        throws IOException {
        DataFusionStatsNodeResponse original = new DataFusionStatsNodeResponse(node, stats);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        DataFusionStatsNodeResponse deserialized = new DataFusionStatsNodeResponse(in);

        // Verify equality (DataFusionStatsNodeResponse has equals/hashCode)
        assertEquals(original, deserialized, "NodeResponse must survive round-trip");
        assertEquals(original.hashCode(), deserialized.hashCode(), "hashCode must be consistent after round-trip");

        // Verify individual components
        assertEquals(original.getNode(), deserialized.getNode(), "DiscoveryNode must survive round-trip");
        assertEquals(original.getStats(), deserialized.getStats(), "DataFusionStats must survive round-trip");
    }

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For a DataFusionStatsNodeResponse with null stats, serialize → deserialize
     * preserves the null stats.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    @Property(tries = 100)
    void nodeResponseRoundTripWithNullStats(@ForAll("discoveryNode") DiscoveryNode node) throws IOException {
        DataFusionStatsNodeResponse original = new DataFusionStatsNodeResponse(node, null);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        DataFusionStatsNodeResponse deserialized = new DataFusionStatsNodeResponse(in);

        // Verify equality
        assertEquals(original, deserialized, "NodeResponse with null stats must survive round-trip");
        assertEquals(original.getNode(), deserialized.getNode(), "DiscoveryNode must survive round-trip");
        assertEquals(null, deserialized.getStats(), "Null stats must remain null after round-trip");
    }

    // ═══════════════════════════════════════════════════════════════════
    // Property 5: Writeable round-trip for DataFusionStatsNodesResponse
    // ═══════════════════════════════════════════════════════════════════

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For any DataFusionStatsNodesResponse, serialize → deserialize produces an
     * object with equal node responses (compared via rendered XContent since
     * BaseNodesResponse does not implement equals).
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    @Property(tries = 100)
    void nodesResponseRoundTrip(
        @ForAll("discoveryNode") DiscoveryNode node1,
        @ForAll("discoveryNode") DiscoveryNode node2,
        @ForAll("dataFusionStats") DataFusionStats stats1,
        @ForAll("dataFusionStats") DataFusionStats stats2
    ) throws IOException {
        List<DataFusionStatsNodeResponse> nodeResponses = new ArrayList<>();
        nodeResponses.add(new DataFusionStatsNodeResponse(node1, stats1));
        nodeResponses.add(new DataFusionStatsNodeResponse(node2, stats2));

        DataFusionStatsNodesResponse original = new DataFusionStatsNodesResponse(
            new ClusterName("test-cluster"),
            nodeResponses,
            Collections.emptyList()
        );

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        DataFusionStatsNodesResponse deserialized = new DataFusionStatsNodesResponse(in);

        // Verify cluster name
        assertNotNull(deserialized.getClusterName(), "ClusterName must not be null after round-trip");
        assertEquals(original.getClusterName(), deserialized.getClusterName(), "ClusterName must survive round-trip");

        // Verify node responses count
        assertEquals(original.getNodes().size(), deserialized.getNodes().size(), "Number of node responses must survive round-trip");

        // Verify each node response
        for (int i = 0; i < original.getNodes().size(); i++) {
            DataFusionStatsNodeResponse origNode = original.getNodes().get(i);
            DataFusionStatsNodeResponse deserNode = deserialized.getNodes().get(i);
            assertEquals(origNode, deserNode, "Node response at index " + i + " must survive round-trip");
        }

        // Verify failures count (empty in this test)
        assertEquals(original.failures().size(), deserialized.failures().size(), "Failures list size must survive round-trip");
    }

    /**
     * Feature: datafusion-cluster-stats, Property 5: Writeable round-trip for transport objects
     *
     * <p>For a DataFusionStatsNodesResponse with empty node list, serialize → deserialize
     * preserves the empty state.
     *
     * <p><b>Validates: Requirements 5.4, 5.5, 5.6</b>
     */
    @Property(tries = 100)
    void nodesResponseRoundTripEmpty() throws IOException {
        DataFusionStatsNodesResponse original = new DataFusionStatsNodesResponse(
            new ClusterName("empty-cluster"),
            Collections.emptyList(),
            Collections.emptyList()
        );

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        DataFusionStatsNodesResponse deserialized = new DataFusionStatsNodesResponse(in);

        // Verify
        assertEquals(original.getClusterName(), deserialized.getClusterName(), "ClusterName must survive round-trip");
        assertEquals(0, deserialized.getNodes().size(), "Empty node list must survive round-trip");
        assertEquals(0, deserialized.failures().size(), "Empty failures list must survive round-trip");
    }
}
