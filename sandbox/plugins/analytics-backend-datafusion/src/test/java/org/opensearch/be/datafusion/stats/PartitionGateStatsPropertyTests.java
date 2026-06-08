/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Randomized tests for {@link PartitionGateStats} serialization round trip.
 *
 * <p>Verifies that for any valid combination of the 4 long fields, serializing via
 * {@code writeTo(StreamOutput)} and deserializing via {@code new PartitionGateStats(StreamInput)}
 * produces an equal object.
 *
 * <p><b>Validates: Requirements 10.3</b> — PartitionGateStats survives StreamOutput/StreamInput round-trip.
 */
public class PartitionGateStatsPropertyTests extends OpenSearchTestCase {

    private static final int TRIES = 200;

    // ---- Generators ----

    private long nonNegLong() {
        return randomLongBetween(0, Long.MAX_VALUE / 2);
    }

    private PartitionGateStats randomPartitionGateStats() {
        String name = randomFrom("datanode_gate", "coordinator_gate");
        return new PartitionGateStats(name, nonNegLong(), nonNegLong(), nonNegLong(), nonNegLong());
    }

    // ---- Property: StreamOutput/StreamInput round trip produces equal object ----

    public void testRoundTripPreservesAllFields() throws IOException {
        for (int i = 0; i < TRIES; i++) {
            PartitionGateStats original = randomPartitionGateStats();

            // Serialize
            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            // Deserialize
            StreamInput in = out.bytes().streamInput();
            PartitionGateStats deserialized = new PartitionGateStats(in);

            // Verify equality
            assertEquals("StreamOutput/StreamInput round trip must preserve all fields", original, deserialized);
            assertEquals("hashCode must be consistent after round trip", original.hashCode(), deserialized.hashCode());

            // Verify individual fields
            assertEquals("maxPermits mismatch", original.maxPermits, deserialized.maxPermits);
            assertEquals("activePermits mismatch", original.activePermits, deserialized.activePermits);
            assertEquals("totalWaitDurationMs mismatch", original.totalWaitDurationMs, deserialized.totalWaitDurationMs);
            assertEquals("totalBatchesStarted mismatch", original.totalBatchesStarted, deserialized.totalBatchesStarted);
        }
    }
}
