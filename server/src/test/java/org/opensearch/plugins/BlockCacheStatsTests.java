/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class BlockCacheStatsTests extends OpenSearchTestCase {

    private BlockCacheStats createStats() {
        return new BlockCacheStats(10, 5, 1024, 512, 3, 300, 2, 200, 0, 4096, 8192);
    }

    public void testSerializationRoundTrip() throws IOException {
        BlockCacheStats original = createStats();
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        BlockCacheStats deserialized = new BlockCacheStats(out.bytes().streamInput());
        assertEquals(original.hits(), deserialized.hits());
        assertEquals(original.misses(), deserialized.misses());
        assertEquals(original.hitBytes(), deserialized.hitBytes());
        assertEquals(original.missBytes(), deserialized.missBytes());
        assertEquals(original.evictions(), deserialized.evictions());
        assertEquals(original.evictionBytes(), deserialized.evictionBytes());
        assertEquals(original.removed(), deserialized.removed());
        assertEquals(original.removedBytes(), deserialized.removedBytes());
        assertEquals(original.memoryBytesUsed(), deserialized.memoryBytesUsed());
        assertEquals(original.diskBytesUsed(), deserialized.diskBytesUsed());
        assertEquals(original.totalBytes(), deserialized.totalBytes());
    }

    public void testToXContentStructure() throws IOException {
        BlockCacheStats stats = createStats();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"block_cache\""));
        assertTrue(json.contains("\"over_all_stats\""));
        assertTrue(json.contains("\"block_file_stats\""));
        assertTrue(json.contains("\"full_file_stats\""));
        assertTrue(json.contains("\"pinned_file_stats\""));
        assertTrue(json.contains("\"hit_count\":10"));
        assertTrue(json.contains("\"miss_count\":5"));
        assertTrue(json.contains("\"used_in_bytes\":4096"));
    }

    public void testZeroStats() throws IOException {
        BlockCacheStats stats = new BlockCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"block_cache\""));
        assertTrue(json.contains("\"hit_count\":0"));
    }
}
