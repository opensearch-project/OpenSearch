/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Property-based tests for NodeStats serialization round-trip with native metrics.
 *
 * NodeStats holds a single {@code @Nullable NativeExecutorsStats nativeExecutorsStats}
 * field serialized via {@code writeOptionalWriteable} / {@code readOptionalWriteable}.
 * The {@link NativeExecutorsStats} wrapper delegates serialization for the underlying
 * {@link DataFusionPluginStats} POJO.
 *
 * Validates: Requirements 9
 */
public class NodeStatsNativeMetricRoundTripTests {

    // --- Arbitraries ---

    @Provide
    Arbitrary<DataFusionPluginStats.RuntimeValues> runtimeValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(posLong, posLong, posLong, posLong, posLong, posLong)
            .as((a, b, c, d, e, f) -> {
                long[] data = new long[] { a, b, c, d, e, f };
                return new DataFusionPluginStats.RuntimeValues(data, 0);
            });
    }

    @Provide
    Arbitrary<DataFusionPluginStats.TaskMonitorValues> taskMonitorValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(posLong, posLong, posLong)
            .as((a, b, c) -> {
                long[] data = new long[] { a, b, c };
                return new DataFusionPluginStats.TaskMonitorValues(data, 0);
            });
    }

    @Provide
    Arbitrary<DataFusionPluginStats> dataFusionPluginStats() {
        Arbitrary<DataFusionPluginStats.RuntimeValues> rv = runtimeValues();
        Arbitrary<DataFusionPluginStats.TaskMonitorValues> tm = taskMonitorValues();
        return Combinators.combine(
            rv.injectNull(0.3),    // ioRuntime (30% chance null)
            rv.injectNull(0.3),    // cpuRuntime (30% chance null)
            tm, tm, tm, tm, tm     // 5 task monitors (always non-null)
        ).as(DataFusionPluginStats::new);
    }

    // --- Property: null DataFusionPluginStats round-trip ---

    /**
     * Property: NodeStats Writeable round-trip with null NativeExecutorsStats
     *
     * For any NodeStats with null nativeExecutorsStats, serializing via
     * writeTo(StreamOutput) and deserializing via new NodeStats(StreamInput)
     * SHALL produce a NodeStats with null getNativeExecutorsStats().
     *
     * **Validates: Requirements 9**
     */
    @Property(tries = 10)
    void writeableRoundTripPreservesNullPluginStats() throws IOException {
        NodeStats original = createNodeStats(null);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NodeStats deserialized = new NodeStats(in);

        assertNull(deserialized.getNativeExecutorsStats(),
            "nativeExecutorsStats should be null after round-trip when not set");
    }

    // --- Property: DataFusionPluginStats Writeable round-trip through NodeStats ---

    /**
     * Property: NodeStats Writeable round-trip with NativeExecutorsStats
     *
     * For any valid NodeStats with a non-null NativeExecutorsStats wrapping a
     * DataFusionPluginStats, serializing and deserializing via StreamOutput/StreamInput
     * (plain Writeable, no NamedWriteableRegistry) SHALL produce a NodeStats whose
     * NativeExecutorsStats contains an equal DataFusionPluginStats field.
     *
     * This exercises the writeOptionalWriteable / readOptionalWriteable code path
     * — same pattern as OsStats, JvmStats.
     *
     * **Validates: Requirements 9**
     */
    @Property(tries = 100)
    void writeableRoundTripPreservesPluginStats(
            @ForAll("dataFusionPluginStats") DataFusionPluginStats pluginStats) throws IOException {
        NativeExecutorsStats nativeExecutors = new NativeExecutorsStats(pluginStats);
        NodeStats original = createNodeStats(nativeExecutors);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize with plain StreamInput (no NamedWriteableAwareStreamInput needed)
        StreamInput in = out.bytes().streamInput();
        NodeStats deserialized = new NodeStats(in);

        // Verify the NativeExecutorsStats field survived the round-trip
        assertNotNull(deserialized.getNativeExecutorsStats(),
            "nativeExecutorsStats should be non-null after round-trip");
        assertEquals(pluginStats, deserialized.getNativeExecutorsStats().getDataFusionPluginStats(),
            "DataFusionPluginStats should be equal after Writeable round-trip through NodeStats");
    }

    // --- Helper ---

    private NodeStats createNodeStats(@Nullable NativeExecutorsStats nativeExecutorsStats) {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        return new NodeStats(
            node,
            System.currentTimeMillis(),
            null, // indices
            null, // os
            null, // process
            null, // jvm
            null, // threadPool
            null, // fs
            null, // transport
            null, // http
            null, // breaker
            null, // scriptStats
            null, // discoveryStats
            null, // ingestStats
            null, // adaptiveSelectionStats
            null, // resourceUsageStats
            null, // scriptCacheStats
            null, // indexingPressureStats
            null, // shardIndexingPressureStats
            null, // searchBackpressureStats
            null, // clusterManagerThrottlingStats
            null, // weightedRoutingStats
            null, // fileCacheStats
            null, // taskCancellationStats
            null, // searchPipelineStats
            null, // segmentReplicationRejectionStats
            null, // repositoriesStats
            null, // admissionControlStats
            null, // nodeCacheStats
            null, // remoteStoreNodeStats
            nativeExecutorsStats  // nativeExecutorsStats
        );
    }
}
