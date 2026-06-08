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
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Property-based tests for {@link AnalyticsBackendNativeMemoryStats} XContent rendering correctness.
 *
 * <p>The class now emits a single inner block:
 * <pre>{@code
 * "analytics_backend": { "allocated_bytes": ..., "resident_bytes": ... }
 * }</pre>
 * The {@code native_memory} parent wrapper and {@code total_estimated_bytes} field are owned by
 * {@code NodeStats.toXContent} and tested there; this class is responsible only for the inner
 * {@code analytics_backend} object.
 */
public class AnalyticsBackendNativeMemoryStatsXContentTests extends OpenSearchTestCase {

    /**
     * Property: any valid (allocatedBytes, residentBytes) pair renders as a single
     * {@code analytics_backend} object with both fields verbatim. Iterates 100 random
     * inputs, including the {@code -1} error sentinel.
     */
    public void testXContentRenderingCorrectness() throws Exception {
        for (int i = 0; i < 100; i++) {
            long allocatedBytes = randomBoolean() ? -1L : randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);
            long residentBytes = randomBoolean() ? -1L : randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE);

            AnalyticsBackendNativeMemoryStats stats = new AnalyticsBackendNativeMemoryStats(allocatedBytes, residentBytes);

            // Render to JSON string via Strings.toString (wraps fragment in root object).
            String json = Strings.toString(MediaTypeRegistry.JSON, stats);
            Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

            // Top-level key must be "analytics_backend" and nothing else.
            assertTrue("Expected 'analytics_backend' on iteration " + i + ", got: " + root.keySet(), root.containsKey("analytics_backend"));
            assertEquals("Expected exactly one top-level key on iteration " + i, 1, root.size());

            // analytics_backend must contain exactly the two byte fields.
            @SuppressWarnings("unchecked")
            Map<String, Object> analyticsBackend = (Map<String, Object>) root.get("analytics_backend");
            assertNotNull("analytics_backend should not be null on iteration " + i, analyticsBackend);
            assertEquals("Expected exactly 2 fields on iteration " + i, 2, analyticsBackend.size());
            assertEquals(
                "allocated_bytes mismatch on iteration " + i + " for value: " + allocatedBytes,
                allocatedBytes,
                ((Number) analyticsBackend.get("allocated_bytes")).longValue()
            );
            assertEquals(
                "resident_bytes mismatch on iteration " + i + " for value: " + residentBytes,
                residentBytes,
                ((Number) analyticsBackend.get("resident_bytes")).longValue()
            );

            // Sanity: the parent-level fields are NOT emitted by this class.
            assertFalse("native_memory wrapper should be opened by NodeStats, not this class", root.containsKey("native_memory"));
            assertFalse(
                "total_estimated_bytes is computed in NodeStats from OsProbe, not emitted here",
                root.containsKey("total_estimated_bytes")
            );
        }
    }

    /** Error sentinel (-1, -1) renders verbatim. */
    public void testXContentRenderingWithErrorState() throws Exception {
        AnalyticsBackendNativeMemoryStats stats = new AnalyticsBackendNativeMemoryStats(-1, -1);

        String json = Strings.toString(MediaTypeRegistry.JSON, stats);
        Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        @SuppressWarnings("unchecked")
        Map<String, Object> analyticsBackend = (Map<String, Object>) root.get("analytics_backend");
        assertNotNull("analytics_backend should be present", analyticsBackend);
        assertEquals(-1L, ((Number) analyticsBackend.get("allocated_bytes")).longValue());
        assertEquals(-1L, ((Number) analyticsBackend.get("resident_bytes")).longValue());
    }

    /** Zero values render as 0, not omitted. */
    public void testXContentRenderingWithZeroValues() throws Exception {
        AnalyticsBackendNativeMemoryStats stats = new AnalyticsBackendNativeMemoryStats(0L, 0L);

        String json = Strings.toString(MediaTypeRegistry.JSON, stats);
        Map<String, Object> root = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        @SuppressWarnings("unchecked")
        Map<String, Object> analyticsBackend = (Map<String, Object>) root.get("analytics_backend");
        assertNotNull("analytics_backend should be present", analyticsBackend);
        assertEquals(0L, ((Number) analyticsBackend.get("allocated_bytes")).longValue());
        assertEquals(0L, ((Number) analyticsBackend.get("resident_bytes")).longValue());
    }
}
