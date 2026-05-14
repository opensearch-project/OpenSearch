/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Property-based tests for {@link NativeMemoryStats} serialization round-trip.
 *
 * Uses randomized testing to verify that serialization via {@code writeTo} followed by
 * deserialization via the {@code StreamInput} constructor produces an object with identical
 * field values for all valid long inputs.
 */
public class NativeMemoryStatsSerializationTests extends OpenSearchTestCase {

    /**
     * Property 1: Serialization round-trip.
     *
     * For any valid NativeMemoryStats object (with any long values for allocatedBytes and
     * residentBytes), serializing via writeTo then deserializing via the StreamInput constructor
     * SHALL produce an object with identical field values.
     *
     * Validates: Requirements 2.3, 2.4, 2.5
     */
    public void testSerializationRoundTrip() throws IOException {
        for (int i = 0; i < 100; i++) {
            long allocatedBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long residentBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);

            NativeMemoryStats original = new NativeMemoryStats(allocatedBytes, residentBytes);

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    NativeMemoryStats deserialized = new NativeMemoryStats(in);

                    assertEquals(
                        "allocatedBytes mismatch on iteration " + i + " for values: [" + allocatedBytes + ", " + residentBytes + "]",
                        original.getAllocatedBytes(),
                        deserialized.getAllocatedBytes()
                    );
                    assertEquals(
                        "residentBytes mismatch on iteration " + i + " for values: [" + allocatedBytes + ", " + residentBytes + "]",
                        original.getResidentBytes(),
                        deserialized.getResidentBytes()
                    );
                }
            }
        }
    }

    /**
     * Serialization round-trip with error state values (-1).
     *
     * Verifies that the error sentinel value (-1) for both fields survives serialization
     * round-trip correctly.
     *
     * Validates: Requirements 2.3, 2.4, 2.5
     */
    public void testSerializationRoundTripWithErrorState() throws IOException {
        NativeMemoryStats original = new NativeMemoryStats(-1, -1);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NativeMemoryStats deserialized = new NativeMemoryStats(in);

                assertEquals(-1L, deserialized.getAllocatedBytes());
                assertEquals(-1L, deserialized.getResidentBytes());
            }
        }
    }

    /**
     * Serialization round-trip with boundary values.
     *
     * Verifies that extreme long values (MIN_VALUE, MAX_VALUE, 0) survive serialization
     * round-trip correctly.
     *
     * Validates: Requirements 2.3, 2.4, 2.5
     */
    public void testSerializationRoundTripWithBoundaryValues() throws IOException {
        long[][] boundaryPairs = {
            { Long.MIN_VALUE, Long.MIN_VALUE },
            { Long.MAX_VALUE, Long.MAX_VALUE },
            { 0L, 0L },
            { Long.MIN_VALUE, Long.MAX_VALUE },
            { Long.MAX_VALUE, Long.MIN_VALUE },
            { 0L, Long.MAX_VALUE },
            { Long.MIN_VALUE, 0L } };

        for (long[] pair : boundaryPairs) {
            NativeMemoryStats original = new NativeMemoryStats(pair[0], pair[1]);

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    NativeMemoryStats deserialized = new NativeMemoryStats(in);

                    assertEquals(
                        "allocatedBytes mismatch for boundary values: [" + pair[0] + ", " + pair[1] + "]",
                        original.getAllocatedBytes(),
                        deserialized.getAllocatedBytes()
                    );
                    assertEquals(
                        "residentBytes mismatch for boundary values: [" + pair[0] + ", " + pair[1] + "]",
                        original.getResidentBytes(),
                        deserialized.getResidentBytes()
                    );
                }
            }
        }
    }
}
