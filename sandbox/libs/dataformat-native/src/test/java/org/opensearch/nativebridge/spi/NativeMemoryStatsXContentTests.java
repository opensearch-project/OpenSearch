/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Property-based tests for {@link NativeMemoryStats} XContent rendering correctness.
 *
 * Uses randomized testing to verify that {@code toXContent} produces a JSON object with
 * the correct structure and field values for all valid long inputs, including the error
 * sentinel value (-1).
 */
public class NativeMemoryStatsXContentTests extends OpenSearchTestCase {

    /**
     * Property 2: XContent rendering correctness.
     *
     * For any valid NativeMemoryStats object (including error state where fields are -1),
     * calling toXContent SHALL produce a JSON object with key "native_memory" containing
     * exactly two fields: "allocated_bytes" with the correct long value and "resident_bytes"
     * with the correct long value.
     *
     * Validates: Requirements 2.2, 6.1, 6.2, 6.3
     */
    public void testXContentRenderingCorrectness() throws Exception {
        for (int i = 0; i < 100; i++) {
            // Generate random values, occasionally including -1 (error sentinel)
            long allocatedBytes = randomBoolean() ? -1L : randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long residentBytes = randomBoolean() ? -1L : randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);

            NativeMemoryStats stats = new NativeMemoryStats(allocatedBytes, residentBytes);

            // Render to JSON string via Strings.toString (wraps fragment in root object)
            String json = Strings.toString(MediaTypeRegistry.JSON, stats);

            // Parse the JSON into a map
            Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

            // Assert top-level key is "native_memory"
            assertTrue(
                "Expected top-level key 'native_memory' on iteration " + i + " for values: ["
                    + allocatedBytes + ", " + residentBytes + "], got keys: " + root.keySet(),
                root.containsKey("native_memory")
            );
            assertEquals(
                "Expected exactly one top-level key on iteration " + i,
                1,
                root.size()
            );

            // Assert the native_memory object contains exactly the two expected fields
            @SuppressWarnings("unchecked")
            Map<String, Object> nativeMemory = (Map<String, Object>) root.get("native_memory");
            assertNotNull("native_memory value should not be null on iteration " + i, nativeMemory);
            assertEquals(
                "Expected exactly 2 fields in native_memory on iteration " + i,
                2,
                nativeMemory.size()
            );
            assertTrue(
                "Expected 'allocated_bytes' field on iteration " + i,
                nativeMemory.containsKey("allocated_bytes")
            );
            assertTrue(
                "Expected 'resident_bytes' field on iteration " + i,
                nativeMemory.containsKey("resident_bytes")
            );

            // Assert correct values (JSON numbers are parsed as Long or Integer depending on size)
            assertEquals(
                "allocated_bytes mismatch on iteration " + i + " for value: " + allocatedBytes,
                allocatedBytes,
                ((Number) nativeMemory.get("allocated_bytes")).longValue()
            );
            assertEquals(
                "resident_bytes mismatch on iteration " + i + " for value: " + residentBytes,
                residentBytes,
                ((Number) nativeMemory.get("resident_bytes")).longValue()
            );
        }
    }

    /**
     * XContent rendering with error state values (-1).
     *
     * Verifies that the error sentinel value (-1) for both fields is correctly rendered
     * in the JSON output as the numeric value -1.
     *
     * Validates: Requirements 2.2, 6.1, 6.2, 6.3
     */
    public void testXContentRenderingWithErrorState() throws Exception {
        NativeMemoryStats stats = new NativeMemoryStats(-1, -1);

        String json = Strings.toString(MediaTypeRegistry.JSON, stats);
        Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        @SuppressWarnings("unchecked")
        Map<String, Object> nativeMemory = (Map<String, Object>) root.get("native_memory");
        assertNotNull("native_memory should be present", nativeMemory);
        assertEquals(-1L, ((Number) nativeMemory.get("allocated_bytes")).longValue());
        assertEquals(-1L, ((Number) nativeMemory.get("resident_bytes")).longValue());
    }

    /**
     * XContent rendering with zero values.
     *
     * Verifies that zero values are correctly rendered in the JSON output.
     *
     * Validates: Requirements 2.2, 6.1, 6.2, 6.3
     */
    public void testXContentRenderingWithZeroValues() throws Exception {
        NativeMemoryStats stats = new NativeMemoryStats(0L, 0L);

        String json = Strings.toString(MediaTypeRegistry.JSON, stats);
        Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        @SuppressWarnings("unchecked")
        Map<String, Object> nativeMemory = (Map<String, Object>) root.get("native_memory");
        assertNotNull("native_memory should be present", nativeMemory);
        assertEquals(0L, ((Number) nativeMemory.get("allocated_bytes")).longValue());
        assertEquals(0L, ((Number) nativeMemory.get("resident_bytes")).longValue());
    }
}
