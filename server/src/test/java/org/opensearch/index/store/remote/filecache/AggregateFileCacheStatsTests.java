/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats.FileCacheStatsType;
import org.opensearch.index.store.remote.utils.cache.stats.AggregateRefCountedCacheStats;
import org.opensearch.index.store.remote.utils.cache.stats.RefCountedCacheStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class AggregateFileCacheStatsTests extends OpenSearchTestCase {
    private static final long BYTES_IN_GB = 1024 * 1024 * 1024;

    public static AggregateRefCountedCacheStats getMockCacheStats() {
        final long evicted = randomLongBetween(10000, BYTES_IN_GB);
        final long removed = randomLongBetween(10000, BYTES_IN_GB);
        final long replaced = randomLongBetween(0, 10000);
        final long hits = randomLongBetween(0, 10000);
        final long miss = randomLongBetween(0, 10000);
        final long usage = randomLongBetween(10000, BYTES_IN_GB);
        final long pinnedUsage = randomLongBetween(10000, BYTES_IN_GB);
        final long activeUsage = randomLongBetween(10000, BYTES_IN_GB);
        final long fullFileHitCount = randomLongBetween(0, 10000);
        final long fullFileRemoveCount = randomLongBetween(0, 10000);
        final long fullFileRemoveWeight = randomLongBetween(10000, BYTES_IN_GB);
        final long fullFileReplaceCount = randomLongBetween(0, 10000);
        final long fullFileEvictionCount = randomLongBetween(0, 10000);
        final long fullFileEvictionWeight = randomLongBetween(10000, BYTES_IN_GB);
        final long fullFileUsage = randomLongBetween(0, 10000);
        final long fullFileActiveUsage = randomLongBetween(0, 10000);

        return new AggregateRefCountedCacheStats(

            new RefCountedCacheStats(hits, miss, 0, removed, replaced, 0, evicted, usage, activeUsage, pinnedUsage),
            new RefCountedCacheStats(
                fullFileHitCount,
                0,
                fullFileRemoveCount,
                fullFileRemoveWeight,
                fullFileReplaceCount,
                fullFileEvictionCount,
                fullFileEvictionWeight,
                fullFileUsage,
                fullFileActiveUsage,
                pinnedUsage
            ),
            new RefCountedCacheStats(
                fullFileHitCount,
                0,
                fullFileRemoveCount,
                fullFileRemoveWeight,
                fullFileReplaceCount,
                fullFileEvictionCount,
                fullFileEvictionWeight,
                fullFileUsage,
                fullFileActiveUsage,
                pinnedUsage
            ),
            new RefCountedCacheStats(
                fullFileHitCount,
                0,
                fullFileRemoveCount,
                fullFileRemoveWeight,
                fullFileReplaceCount,
                fullFileEvictionCount,
                fullFileEvictionWeight,
                fullFileUsage,
                fullFileActiveUsage,
                pinnedUsage
            )

        );
    }

    public static long getMockCacheCapacity() {
        return randomLongBetween(10 * BYTES_IN_GB, 1000 * BYTES_IN_GB);
    }

    public static AggregateFileCacheStats getFileCacheStats(final long fileCacheCapacity, final AggregateRefCountedCacheStats stats)
        throws IOException {
        return new AggregateFileCacheStats(
            System.currentTimeMillis(),
            new FileCacheStats(
                stats.activeUsage(),
                fileCacheCapacity,
                stats.usage(),
                stats.pinnedUsage(),
                stats.evictionWeight(),
                stats.hitCount(),
                stats.missCount(),
                FileCacheStatsType.OVER_ALL_STATS
            ),
            new FileCacheStats(
                stats.activeUsage(),
                fileCacheCapacity,
                stats.usage(),
                stats.pinnedUsage(),
                stats.evictionWeight(),
                stats.hitCount(),
                stats.missCount(),
                FileCacheStatsType.FULL_FILE_STATS
            ),
            new FileCacheStats(
                stats.activeUsage(),
                fileCacheCapacity,
                stats.usage(),
                stats.pinnedUsage(),
                stats.evictionWeight(),
                stats.hitCount(),
                stats.missCount(),
                FileCacheStatsType.BLOCK_FILE_STATS
            ),
            new FileCacheStats(
                stats.activeUsage(),
                fileCacheCapacity,
                stats.usage(),
                stats.pinnedUsage(),
                stats.evictionWeight(),
                stats.hitCount(),
                stats.missCount(),
                FileCacheStatsType.PINNED_FILE_STATS
            )
        );
    }

    public static FileCacheStats getMockFullFileCacheStats() {
        final long active = randomLongBetween(100000, BYTES_IN_GB);
        final long total = randomLongBetween(100000, BYTES_IN_GB);
        final long used = randomLongBetween(100000, BYTES_IN_GB);
        final long pinned = randomLongBetween(100000, BYTES_IN_GB);
        final long evicted = randomLongBetween(0, getMockCacheStats().getFullFileCacheStats().evictionWeight());
        final long hit = randomLongBetween(0, 10);
        final long misses = randomLongBetween(0, 10);
        return new FileCacheStats(active, total, used, pinned, evicted, hit, misses, FileCacheStatsType.OVER_ALL_STATS);
    }

    public static AggregateFileCacheStats getMockFileCacheStats() throws IOException {
        final long fcSize = getMockCacheCapacity();
        return getFileCacheStats(fcSize, getMockCacheStats());
    }

    public static void validateFullFileStats(FileCacheStats original, FileCacheStats deserialized) {
        assertEquals(original.getHits(), deserialized.getHits());
        assertEquals(original.getActive(), deserialized.getActive());
        assertEquals(original.getUsed(), deserialized.getUsed());
        assertEquals(original.getEvicted(), deserialized.getEvicted());
        assertEquals(original.getActivePercent(), deserialized.getActivePercent());
    }

    public static void validateFileCacheStats(AggregateFileCacheStats original, AggregateFileCacheStats deserialized) throws IOException {
        assertEquals(original.getTotal(), deserialized.getTotal());
        assertEquals(original.getUsed(), deserialized.getUsed());
        assertEquals(original.getUsedPercent(), deserialized.getUsedPercent());
        assertEquals(original.getActive(), deserialized.getActive());
        assertEquals(original.getActivePercent(), deserialized.getActivePercent());
        assertEquals(original.getOverallActivePercent(), deserialized.getOverallActivePercent(), 0.0);
        assertEquals(original.getEvicted(), deserialized.getEvicted());
        assertEquals(original.getCacheHits(), deserialized.getCacheHits());
        assertEquals(original.getCacheMisses(), deserialized.getCacheMisses());
        assertEquals(original.getTimestamp(), deserialized.getTimestamp());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder = original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentBuilder deserializedBuilder = XContentFactory.jsonBuilder();
        deserializedBuilder.startObject();
        deserializedBuilder = deserialized.toXContent(deserializedBuilder, ToXContent.EMPTY_PARAMS);
        deserializedBuilder.endObject();

        assertTrue(builder.toString().equals(deserializedBuilder.toString()));
    }

    public void testFileCacheStatsSerialization() throws IOException {
        final AggregateFileCacheStats fileCacheStats = getMockFileCacheStats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            fileCacheStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                // Validate original object against deserialized values
                validateFileCacheStats(fileCacheStats, new AggregateFileCacheStats(in));
            }
        }
    }
}
