/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Property-based tests for {@link NativeMemoryStats} serialization round-trip.
 * <p>
 * <b>Feature: native-stats-refactor, Property 2: NativeMemoryStats serialization round-trip</b>
 * <p>
 * For any valid NativeMemoryStats instance (with allocatedBytes and residentBytes values),
 * serializing via writeTo(StreamOutput) and deserializing via new NativeMemoryStats(StreamInput)
 * SHALL produce an equivalent object with identical field values.
 * <p>
 * <b>Validates: Requirements 8.3, 9.1</b>
 */
public class NativeMemoryStatsPropertyTests extends OpenSearchTestCase {

    /**
     * Property 2: NativeMemoryStats serialization round-trip.
     * <p>
     * For any valid (allocatedBytes, residentBytes) pair including edge values (-1, 0, Long.MAX_VALUE),
     * writeTo followed by readFrom produces an equal object.
     * <p>
     * <b>Validates: Requirements 8.3, 9.1</b>
     */
    public void testSerializationRoundTripProperty() throws IOException {
        for (int i = 0; i < 100; i++) {
            long allocatedBytes = generateRandomLong();
            long residentBytes = generateRandomLong();

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
     * Property 2 edge cases: serialization round-trip with boundary values.
     * <p>
     * Verifies that extreme long values (-1, 0, Long.MIN_VALUE, Long.MAX_VALUE)
     * survive serialization round-trip correctly.
     * <p>
     * <b>Validates: Requirements 8.3, 9.1</b>
     */
    public void testSerializationRoundTripBoundaryValues() throws IOException {
        long[] edgeValues = { -1L, 0L, Long.MIN_VALUE, Long.MAX_VALUE, 1L, -2L, Long.MAX_VALUE - 1 };

        for (long allocated : edgeValues) {
            for (long resident : edgeValues) {
                NativeMemoryStats original = new NativeMemoryStats(allocated, resident);

                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    original.writeTo(out);
                    try (StreamInput in = out.bytes().streamInput()) {
                        NativeMemoryStats deserialized = new NativeMemoryStats(in);

                        assertEquals(
                            "allocatedBytes mismatch for boundary values: [" + allocated + ", " + resident + "]",
                            original.getAllocatedBytes(),
                            deserialized.getAllocatedBytes()
                        );
                        assertEquals(
                            "residentBytes mismatch for boundary values: [" + allocated + ", " + resident + "]",
                            original.getResidentBytes(),
                            deserialized.getResidentBytes()
                        );
                    }
                }
            }
        }
    }

    /**
     * Generates a random long value biased toward interesting edge cases.
     * Occasionally returns -1, 0, Long.MIN_VALUE, or Long.MAX_VALUE.
     */
    private long generateRandomLong() {
        int choice = randomIntBetween(0, 9);
        switch (choice) {
            case 0:
                return -1L;
            case 1:
                return 0L;
            case 2:
                return Long.MAX_VALUE;
            case 3:
                return Long.MIN_VALUE;
            default:
                return randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
        }
    }
}
