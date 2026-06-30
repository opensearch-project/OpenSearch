/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Unit tests for {@link CacheStats} and {@link CacheGroupStats}.
 *
 * <p>Covers transport serialization round-trip, JSON rendering shape,
 * the derived {@code hit_rate}, and equality semantics.
 */
public class CacheStatsTests extends OpenSearchTestCase {

    // ---- CacheGroupStats ----

    public void testCacheGroupStatsFieldsArePopulated() {
        CacheGroupStats g = new CacheGroupStats(10, 2, 5, 4096, 100_000_000);
        assertEquals(10L, g.hitCount);
        assertEquals(2L, g.missCount);
        assertEquals(5L, g.entryCount);
        assertEquals(4096L, g.memoryBytes);
        assertEquals(100_000_000L, g.sizeLimitBytes);
    }

    public void testCacheGroupStatsHitRate() {
        CacheGroupStats hits = new CacheGroupStats(10, 2, 0, 0, 0);
        assertEquals(10.0 / 12.0, hits.hitRate(), 1e-9);

        // No traffic must yield 0.0, not NaN
        CacheGroupStats empty = new CacheGroupStats(0, 0, 0, 0, 0);
        assertEquals(0.0, empty.hitRate(), 0.0);

        CacheGroupStats pure = new CacheGroupStats(7, 0, 0, 0, 0);
        assertEquals(1.0, pure.hitRate(), 0.0);

        CacheGroupStats miss = new CacheGroupStats(0, 5, 0, 0, 0);
        assertEquals(0.0, miss.hitRate(), 0.0);
    }

    public void testCacheGroupStatsWriteableRoundTrip() throws IOException {
        CacheGroupStats original = new CacheGroupStats(123, 45, 67, 89_012, 250_000_000);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        CacheGroupStats decoded = new CacheGroupStats(in);
        assertEquals(original, decoded);
    }

    public void testCacheGroupStatsToXContent() throws IOException {
        CacheGroupStats g = new CacheGroupStats(10, 2, 5, 4096, 100_000_000);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        g.toXContent(builder);
        builder.endObject();
        String json = builder.toString();

        assertTrue(json.contains("\"hit_count\":10"));
        assertTrue(json.contains("\"miss_count\":2"));
        // Match hit_rate by prefix since the JSON formatting of doubles is not pinned
        assertTrue("expected hit_rate ≈ 10/12, got: " + json, json.contains("\"hit_rate\":0.833"));
        assertTrue(json.contains("\"entry_count\":5"));
        assertTrue(json.contains("\"memory_bytes\":4096"));
        assertTrue(json.contains("\"size_limit_bytes\":100000000"));
    }

    public void testCacheGroupStatsEqualsAndHashCode() {
        CacheGroupStats a = new CacheGroupStats(1, 2, 3, 4, 5);
        CacheGroupStats b = new CacheGroupStats(1, 2, 3, 4, 5);
        CacheGroupStats c = new CacheGroupStats(1, 2, 3, 4, 6);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    // ---- CacheStats ----

    public void testCacheStatsConstructorRejectsNullSubGroups() {
        CacheGroupStats g = new CacheGroupStats(0, 0, 0, 0, 0);
        expectThrows(NullPointerException.class, () -> new CacheStats(null, g, g, g));
        expectThrows(NullPointerException.class, () -> new CacheStats(g, null, g, g));
        expectThrows(NullPointerException.class, () -> new CacheStats(g, g, null, g));
        expectThrows(NullPointerException.class, () -> new CacheStats(g, g, g, null));
    }

    public void testCacheStatsAccessors() {
        CacheGroupStats meta = new CacheGroupStats(1, 2, 3, 4, 5);
        CacheGroupStats stats = new CacheGroupStats(6, 7, 8, 9, 10);
        CacheGroupStats scoped = new CacheGroupStats(11, 12, 13, 14, 15);
        CacheGroupStats offset = new CacheGroupStats(11, 12, 13, 14, 15);
        CacheStats c = new CacheStats(meta, stats, scoped, offset);

        assertSame(meta, c.getMetadataCache());
        assertSame(stats, c.getStatisticsCache());
        assertSame(scoped, c.getColumnIndexCache());
        assertSame(offset, c.getOffsetIndexCache());
    }

    public void testCacheStatsWriteableRoundTrip() throws IOException {
        CacheStats original = new CacheStats(
            new CacheGroupStats(11, 12, 13, 14, 15),
            new CacheGroupStats(21, 22, 23, 24, 25),
            new CacheGroupStats(31, 32, 33, 34, 35),
            new CacheGroupStats(41, 42, 43, 44, 45)
        );
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        CacheStats decoded = new CacheStats(in);
        assertEquals(original, decoded);
    }

    public void testCacheStatsToXContentShape() throws IOException {
        CacheStats c = new CacheStats(
            new CacheGroupStats(11, 0, 3, 1024, 250_000_000),
            new CacheGroupStats(0, 7, 0, 0, 100_000_000),
            new CacheGroupStats(5, 1, 2, 512, 64_000_000),
            new CacheGroupStats(5, 2, 3, 256, 32_000_000)
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        c.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        // Top-level wrapper
        assertTrue("expected cache_stats wrapper, got: " + json, json.contains("\"cache_stats\""));
        // All four sub-groups present
        assertTrue(json.contains("\"metadata_cache\""));
        assertTrue(json.contains("\"statistics_cache\""));
        assertTrue(json.contains("\"column_index_cache\""));
        assertTrue(json.contains("\"offset_index_cache\""));
        // Per-group fields
        assertTrue(json.contains("\"hit_count\":11"));
        assertTrue(json.contains("\"miss_count\":7"));
        assertTrue(json.contains("\"entry_count\":3"));
        assertTrue(json.contains("\"memory_bytes\":1024"));
        assertTrue(json.contains("\"size_limit_bytes\":250000000"));
        assertTrue(json.contains("\"size_limit_bytes\":100000000"));
        assertTrue(json.contains("\"size_limit_bytes\":64000000"));
        assertTrue(json.contains("\"size_limit_bytes\":32000000"));
    }

    public void testCacheStatsZeroedRendersAllZeros() throws IOException {
        CacheStats zero = new CacheStats(
            new CacheGroupStats(0, 0, 0, 0, 0),
            new CacheGroupStats(0, 0, 0, 0, 0),
            new CacheGroupStats(0, 0, 0, 0, 0),
            new CacheGroupStats(0, 0, 0, 0, 0)
        );
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        zero.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        // Disabled-cache sentinel: all four size_limit_bytes are 0
        long sizeLimitOccurrences = json.split("\"size_limit_bytes\":0", -1).length - 1;
        assertEquals("expected size_limit_bytes:0 to appear four times (one per sub-cache)", 4, sizeLimitOccurrences);
        // hit_rate must not be NaN
        assertFalse("hit_rate must not be NaN: " + json, json.contains("NaN"));
    }

    public void testCacheStatsEqualsAndHashCode() {
        // c differs only in statisticsCache (sizeLimitBytes 10 → 99)
        CacheStats a = new CacheStats(
            new CacheGroupStats(1, 2, 3, 4, 5),
            new CacheGroupStats(6, 7, 8, 9, 10),
            new CacheGroupStats(11, 12, 13, 14, 15),
            new CacheGroupStats(16, 17, 18, 19, 20)
        );
        CacheStats b = new CacheStats(
            new CacheGroupStats(1, 2, 3, 4, 5),
            new CacheGroupStats(6, 7, 8, 9, 10),
            new CacheGroupStats(11, 12, 13, 14, 15),
            new CacheGroupStats(16, 17, 18, 19, 20)
        );
        CacheStats c = new CacheStats(
            new CacheGroupStats(1, 2, 3, 4, 5),
            new CacheGroupStats(6, 7, 8, 9, 99),
            new CacheGroupStats(11, 12, 13, 14, 15),
            new CacheGroupStats(16, 17, 18, 19, 20)
        );

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
