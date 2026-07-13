/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Property-based tests for {@link AnalyticsBackendNativeMemoryStats} serialization round-trip.
 * <p>
 * <b>Feature: native-stats-refactor, Property 2: AnalyticsBackendNativeMemoryStats serialization round-trip</b>
 * <p>
 * For any valid AnalyticsBackendNativeMemoryStats instance (with allocatedBytes and residentBytes values),
 * serializing via writeTo(StreamOutput) and deserializing via new AnalyticsBackendNativeMemoryStats(StreamInput)
 * SHALL produce an equivalent object with identical field values.
 * <p>
 * <b>Validates: Requirements 8.3, 9.1</b>
 */
public class AnalyticsBackendNativeMemoryStatsPropertyTests extends OpenSearchTestCase {

    /**
     * Property 2: AnalyticsBackendNativeMemoryStats serialization round-trip.
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
                    assertEquals("purgeCount mismatch on iteration " + i, original.getPurgeCount(), deserialized.getPurgeCount());
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
                AnalyticsBackendNativeMemoryStats original = new AnalyticsBackendNativeMemoryStats(allocated, resident, 0);

                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    original.writeTo(out);
                    try (StreamInput in = out.bytes().streamInput()) {
                        AnalyticsBackendNativeMemoryStats deserialized = new AnalyticsBackendNativeMemoryStats(in);

                        assertEquals("allocatedBytes mismatch", original.getAllocatedBytes(), deserialized.getAllocatedBytes());
                        assertEquals("residentBytes mismatch", original.getResidentBytes(), deserialized.getResidentBytes());
                        assertEquals("purgeCount mismatch", 0L, deserialized.getPurgeCount());
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testToXContentIncludesPurgeCount() throws Exception {
        AnalyticsBackendNativeMemoryStats stats = new AnalyticsBackendNativeMemoryStats(1024L, 2048L, 5);
        String json = Strings.toString(MediaTypeRegistry.JSON, stats);
        Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
        Map<String, Object> ab = (Map<String, Object>) root.get("analytics_backend");
        assertEquals(1024L, ((Number) ab.get("allocated_bytes")).longValue());
        assertEquals(2048L, ((Number) ab.get("resident_bytes")).longValue());
        assertEquals(5L, ((Number) ab.get("purge_count")).longValue());
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
