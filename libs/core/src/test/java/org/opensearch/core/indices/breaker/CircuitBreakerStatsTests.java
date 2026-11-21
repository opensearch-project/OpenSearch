/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.indices.breaker;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CircuitBreakerStatsTests extends OpenSearchTestCase {

    public void testToXContentWithNegativeEstimated() throws IOException {
        CircuitBreakerStats stats = new CircuitBreakerStats("request", 1024, -59452, 1.0, 0);
        XContentBuilder builder = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent());
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"estimated_size_in_bytes\":-59452"));
        assertTrue(json.contains("\"estimated_size\":\"0b\""));
    }

    public void testToXContentWithValidValues() throws IOException {
        CircuitBreakerStats stats = new CircuitBreakerStats("request", 1024, 512, 1.0, 3);
        XContentBuilder builder = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent());
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"estimated_size_in_bytes\":512"));
        assertTrue(json.contains("\"estimated_size\":\"512b\""));
        assertTrue(json.contains("\"limit_size_in_bytes\":1024"));
        assertTrue(json.contains("\"tripped\":3"));
    }

    public void testToStringWithNegativeEstimated() {
        CircuitBreakerStats stats = new CircuitBreakerStats("request", 1024, -100, 1.0, 0);
        String str = stats.toString();
        assertNotNull(str);
        assertTrue(str.contains("estimated=-100/0b"));
    }

    public void testToStringWithValidValues() {
        CircuitBreakerStats stats = new CircuitBreakerStats("request", 1024, 512, 1.0, 0);
        String str = stats.toString();
        assertTrue(str.contains("estimated=512/512b"));
    }

    public void testToXContentWithMinusOneEstimated() throws IOException {
        CircuitBreakerStats stats = new CircuitBreakerStats("fielddata", -1, -1, 0, 0);
        XContentBuilder builder = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent());
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"estimated_size_in_bytes\":-1"));
    }

    public void testGetters() {
        CircuitBreakerStats stats = new CircuitBreakerStats("test", 2048, 1024, 1.5, 7);
        assertEquals("test", stats.getName());
        assertEquals(2048, stats.getLimit());
        assertEquals(1024, stats.getEstimated());
        assertEquals(7, stats.getTrippedCount());
        assertEquals(1.5, stats.getOverhead(), 0.0);
    }
}
