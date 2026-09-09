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
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Property-based tests for {@link AnalyticsBackendNativeMemoryStats} serialization round-trip.
 *
 * Uses randomized testing to verify that serialization via {@code writeTo} followed by
 * deserialization via the {@code StreamInput} constructor produces an object with identical
 * field values for all valid long inputs.
 */
public class AnalyticsBackendNativeMemoryStatsSerializationTests extends OpenSearchTestCase {

    /**
     * Property 1: Serialization round-trip.
     *
     * For any valid AnalyticsBackendNativeMemoryStats object (with any long values for allocatedBytes and
     * residentBytes), serializing via writeTo then deserializing via the StreamInput constructor
     * SHALL produce an object with identical field values.
     *
     * Validates: Requirements 2.3, 2.4, 2.5
     */
    public void testSerializationRoundTrip() throws IOException {
        for (int i = 0; i < 100; i++) {
            long allocatedBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long residentBytes = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long purgeCount = randomLongBetween(0, Long.MAX_VALUE);

            AnalyticsBackendNativeMemoryStats original = new AnalyticsBackendNativeMemoryStats(allocatedBytes, residentBytes, purgeCount);

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    AnalyticsBackendNativeMemoryStats deserialized = new AnalyticsBackendNativeMemoryStats(in);

                    assertEquals(
                        "allocatedBytes mismatch on iteration " + i,
                        original.getAllocatedBytes(),
                        deserialized.getAllocatedBytes()
                    );
                    assertEquals("residentBytes mismatch on iteration " + i, original.getResidentBytes(), deserialized.getResidentBytes());
                    assertEquals("purgeCount mismatch on iteration " + i, purgeCount, deserialized.getPurgeCount());
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
        AnalyticsBackendNativeMemoryStats original = new AnalyticsBackendNativeMemoryStats(-1, -1, 0);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                AnalyticsBackendNativeMemoryStats deserialized = new AnalyticsBackendNativeMemoryStats(in);

                assertEquals(-1L, deserialized.getAllocatedBytes());
                assertEquals(-1L, deserialized.getResidentBytes());
                assertEquals(0L, deserialized.getPurgeCount());
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
            AnalyticsBackendNativeMemoryStats original = new AnalyticsBackendNativeMemoryStats(pair[0], pair[1], 0);

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                original.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    AnalyticsBackendNativeMemoryStats deserialized = new AnalyticsBackendNativeMemoryStats(in);

                    assertEquals("allocatedBytes mismatch", original.getAllocatedBytes(), deserialized.getAllocatedBytes());
                    assertEquals("residentBytes mismatch", original.getResidentBytes(), deserialized.getResidentBytes());
                    assertEquals("purgeCount should be 0", 0L, deserialized.getPurgeCount());
                }
            }
        }
    }
}
