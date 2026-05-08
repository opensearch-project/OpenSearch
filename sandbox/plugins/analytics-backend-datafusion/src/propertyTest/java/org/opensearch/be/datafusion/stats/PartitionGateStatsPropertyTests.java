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

import java.io.IOException;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Property-based tests for {@link PartitionGateStats} serialization round trip.
 *
 * <p>Verifies that for any valid combination of the 4 long fields, serializing via
 * {@code writeTo(StreamOutput)} and deserializing via {@code new PartitionGateStats(StreamInput)}
 * produces an equal object.
 *
 * <p><b>Validates: Requirements 10.3</b> — PartitionGateStats survives StreamOutput/StreamInput round-trip.
 */
public class PartitionGateStatsPropertyTests {

    // ---- Generators ----

    @Provide
    Arbitrary<PartitionGateStats> partitionGateStats() {
        Arbitrary<Long> nonNegLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNegLong, nonNegLong, nonNegLong, nonNegLong).as(PartitionGateStats::new);
    }

    // ---- Property: StreamOutput/StreamInput round trip produces equal object ----

    /**
     * Property: For any valid PartitionGateStats, serializing via writeTo and
     * deserializing via the StreamInput constructor produces an equal object
     * with identical hashCode.
     *
     * <p><b>Validates: Requirements 10.3</b>
     */
    @Property(tries = 200)
    void roundTripPreservesAllFields(@ForAll("partitionGateStats") PartitionGateStats original) throws IOException {
        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        PartitionGateStats deserialized = new PartitionGateStats(in);

        // Verify equality
        assertEquals(original, deserialized, "StreamOutput/StreamInput round trip must preserve all fields");
        assertEquals(original.hashCode(), deserialized.hashCode(), "hashCode must be consistent after round trip");

        // Verify individual fields
        assertEquals(original.maxPermits, deserialized.maxPermits, "maxPermits mismatch");
        assertEquals(original.activePermits, deserialized.activePermits, "activePermits mismatch");
        assertEquals(original.totalWaitDurationMs, deserialized.totalWaitDurationMs, "totalWaitDurationMs mismatch");
        assertEquals(original.totalBatchesStarted, deserialized.totalBatchesStarted, "totalBatchesStarted mismatch");
    }
}
