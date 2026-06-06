/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.Version;
import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
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

/**
 * Property-based tests for per-node stats equivalence.
 *
 * <p>Feature: datafusion-cluster-stats, Property 2: Per-node stats equivalence
 *
 * <p>For any {@link DataFusionStats} instance, when wrapped in a
 * {@link DataFusionStatsNodeResponse} and rendered via
 * {@link DataFusionStatsNodesResponse#toXContent}, the stats portion of the per-node
 * JSON is equivalent to rendering the same {@link DataFusionStats} directly via
 * {@code DataFusionStats.toXContent}.
 *
 * <p><b>Validates: Requirements 2.4</b>
 */
public class PerNodeStatsEquivalencePropertyTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Metadata fields that were previously added by DataFusionStatsNodesResponse (now removed). */
    private static final Set<String> NODE_METADATA_FIELDS = Set.of();

    // ---- Object generators ----

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

    /** DataFusionStats with all sections populated (CPU runtime present). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsFullCpuPresent() {
        return Combinators.combine(runtimeMetrics(), runtimeMetrics().map(rt -> {
            if (rt.workersCount == 0) {
                return new RuntimeMetrics(
                    1,
                    rt.totalPollsCount,
                    rt.totalBusyDurationMs,
                    rt.totalOverflowCount,
                    rt.globalQueueDepth,
                    rt.blockingQueueDepth,
                    rt.numAliveTasks,
                    rt.spawnedTasksCount,
                    rt.totalLocalQueueDepth
                );
            }
            return rt;
        }), taskMonitorStats(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats()).as((io, cpu, cr, qe, sn, ps) -> {
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

    /** DataFusionStats with CPU runtime absent. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsFullCpuAbsent() {
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

    /** Combined DataFusionStats generator (CPU present or absent). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStats() {
        return Arbitraries.oneOf(dataFusionStatsFullCpuPresent(), dataFusionStatsFullCpuAbsent());
    }

    // ---- Property 2: Per-node stats equivalence ----

    /**
     * Feature: datafusion-cluster-stats, Property 2: Per-node stats equivalence
     *
     * <p>For any DataFusionStats instance, the stats portion rendered via
     * DataFusionStatsNodesResponse is equivalent to rendering via
     * DataFusionStats.toXContent directly.
     *
     * <p><b>Validates: Requirements 2.4</b>
     */
    @Property(tries = 150)
    @SuppressWarnings("unchecked")
    void perNodeStatsMatchDirectRendering(@ForAll("dataFusionStats") DataFusionStats stats) throws Exception {
        // Step 1: Render DataFusionStats directly
        Map<String, Object> directMap = renderStatsDirect(stats);

        // Step 2: Wrap in a DataFusionStatsNodeResponse + DataFusionStatsNodesResponse and render
        Map<String, Object> wrappedMap = renderStatsViaNodesResponse(stats);

        // Step 3: Compare — they should be identical
        assertEquals(
            directMap,
            wrappedMap,
            "Stats rendered directly via DataFusionStats.toXContent must equal "
                + "the stats portion extracted from DataFusionStatsNodesResponse per-node entry"
        );
    }

    // ---- Helper methods ----

    /**
     * Renders DataFusionStats directly: {@code builder.startObject(); stats.toXContent(builder, params); builder.endObject();}
     * then parses to a Map.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> renderStatsDirect(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);
    }

    /**
     * Wraps the stats in a DataFusionStatsNodeResponse with a DiscoveryNode,
     * puts it in a DataFusionStatsNodesResponse, renders to JSON, extracts the
     * per-node entry from the "nodes" object, and removes the metadata fields
     * (name, host, transport_address) to isolate just the stats portion.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> renderStatsViaNodesResponse(DataFusionStats stats) throws Exception {
        // Create a DiscoveryNode for wrapping
        DiscoveryNode node = new DiscoveryNode(
            "test-node",
            "test-node-id",
            new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );

        // Wrap in node response
        DataFusionStatsNodeResponse nodeResponse = new DataFusionStatsNodeResponse(node, stats);

        // Wrap in nodes response
        DataFusionStatsNodesResponse nodesResponse = new DataFusionStatsNodesResponse(
            new ClusterName("test-cluster"),
            List.of(nodeResponse),
            Collections.emptyList()
        );

        // Render the full response to JSON
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        nodesResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> fullMap = XContentHelper.convertToMap(MediaTypeRegistry.JSON.xContent(), builder.toString(), false);

        // Extract the per-node entry
        Map<String, Object> nodesObj = (Map<String, Object>) fullMap.get("nodes");
        Map<String, Object> nodeEntry = (Map<String, Object>) nodesObj.get("test-node-id");

        // Remove metadata fields to isolate just the stats
        LinkedHashMap<String, Object> statsOnly = new LinkedHashMap<>(nodeEntry);
        for (String metaField : NODE_METADATA_FIELDS) {
            statsOnly.remove(metaField);
        }

        return statsOnly;
    }
}
