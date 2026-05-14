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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.nativebridge.spi.NativeMemoryStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

/**
 * Property-based tests for version-gated serialization of {@link NativeMemoryStats}
 * within {@link NodeStats}.
 *
 * Verifies that when NodeStats containing a non-null NativeMemoryStats is serialized
 * to a stream with a version older than V_3_7_0, the deserialized NodeStats has
 * nativeMemoryStats == null. Conversely, when serialized to V_3_7_0 or later, the
 * NativeMemoryStats is preserved.
 */
public class NativeMemoryStatsVersionGateTests extends OpenSearchTestCase {

    /**
     * Property 3: Version-gated serialization preserves null for old versions.
     *
     * For any NodeStats containing a non-null NativeMemoryStats, serializing to a stream
     * with version older than the native-memory support version (V_3_7_0) and then
     * deserializing SHALL yield a NodeStats with nativeMemoryStats == null.
     *
     * Validates: Requirements 3.4, 3.5
     */
    public void testVersionGatedSerializationOmitsNativeMemoryStatsForOldVersions() throws IOException {
        for (int i = 0; i < 100; i++) {
            long allocatedBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long residentBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            NativeMemoryStats nativeMemoryStats = new NativeMemoryStats(allocatedBytes, residentBytes);

            NodeStats nodeStats = createNodeStatsWithNativeMemory(nativeMemoryStats);

            // Serialize with a version older than V_3_7_0
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(Version.V_2_18_0);
                nodeStats.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(Version.V_2_18_0);
                    NodeStats deserialized = new NodeStats(in);

                    assertNull(
                        "nativeMemoryStats should be null when deserialized from version < V_3_7_0, "
                            + "iteration "
                            + i
                            + " with values ["
                            + allocatedBytes
                            + ", "
                            + residentBytes
                            + "]",
                        deserialized.getNativeMemoryStats()
                    );
                }
            }
        }
    }

    /**
     * Positive case: Version-gated serialization preserves NativeMemoryStats for V_3_7_0+.
     *
     * For any NodeStats containing a non-null NativeMemoryStats, serializing to a stream
     * with version V_3_7_0 or later and then deserializing SHALL yield a NodeStats with
     * nativeMemoryStats containing the original values.
     *
     * Validates: Requirements 3.4, 3.5
     */
    public void testVersionGatedSerializationPreservesNativeMemoryStatsForCurrentVersion() throws IOException {
        for (int i = 0; i < 100; i++) {
            long allocatedBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long residentBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            NativeMemoryStats nativeMemoryStats = new NativeMemoryStats(allocatedBytes, residentBytes);

            NodeStats nodeStats = createNodeStatsWithNativeMemory(nativeMemoryStats);

            // Serialize with V_3_7_0 (the version that introduced native memory support)
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(Version.V_3_7_0);
                nodeStats.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(Version.V_3_7_0);
                    NodeStats deserialized = new NodeStats(in);

                    assertNotNull(
                        "nativeMemoryStats should be non-null when deserialized from version >= V_3_7_0, " + "iteration " + i,
                        deserialized.getNativeMemoryStats()
                    );
                    assertEquals(
                        "allocatedBytes mismatch on iteration " + i,
                        allocatedBytes,
                        deserialized.getNativeMemoryStats().getAllocatedBytes()
                    );
                    assertEquals(
                        "residentBytes mismatch on iteration " + i,
                        residentBytes,
                        deserialized.getNativeMemoryStats().getResidentBytes()
                    );
                }
            }
        }
    }

    /**
     * Creates a minimal NodeStats with the given NativeMemoryStats and all other fields null.
     * Uses the current version for the DiscoveryNode.
     */
    private NodeStats createNodeStatsWithNativeMemory(NativeMemoryStats nativeMemoryStats) {
        DiscoveryNode node = new DiscoveryNode("test_node", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);

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
            nativeMemoryStats
        );
    }
}
