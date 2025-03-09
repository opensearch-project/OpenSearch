/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.store.remote.utils.cache.CacheUsage;
import org.opensearch.index.store.remote.utils.cache.stats.CacheStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class FileCacheStatsTests extends OpenSearchTestCase {
    private static final long BYTES_IN_GB = 1024 * 1024 * 1024;

    public static CacheStats getMockCacheStats() {
        final long evicted = randomLongBetween(10000, BYTES_IN_GB);
        final long removed = randomLongBetween(10000, BYTES_IN_GB);
        final long replaced = randomLongBetween(0, 10000);
        final long hits = randomLongBetween(0, 10000);
        final long miss = randomLongBetween(0, 10000);
        return new CacheStats(hits, miss, 0, removed, replaced, 0, evicted);
    }

    public static CacheUsage getMockCacheUsage(long total) {
        final long used = randomLongBetween(100, total);
        final long active = randomLongBetween(10, used);
        return new CacheUsage(used, active);
    }

    public static long getMockCacheCapacity() {
        return randomLongBetween(10 * BYTES_IN_GB, 1000 * BYTES_IN_GB);
    }

    public static FileCacheStats getFileCacheStats(final long fileCacheCapacity, final CacheStats stats, final CacheUsage usage) {
        return new FileCacheStats(
            System.currentTimeMillis(),
            usage.activeUsage(),
            fileCacheCapacity,
            usage.usage(),
            stats.evictionWeight(),
            stats.hitCount(),
            stats.missCount(),
            getMockFullFileCacheStats()
        );
    }

    public static FullFileCacheStats getMockFullFileCacheStats() {
        final long activeFullFileBytes = randomLongBetween(100000, BYTES_IN_GB);
        final long activeFullFileCount = randomLongBetween(10, 100);
        final long usedFullFileBytes = randomLongBetween(100000, BYTES_IN_GB);
        final long usedFullFileCount = randomLongBetween(activeFullFileCount, 200);
        final long evictedFullFileBytes = randomLongBetween(0, activeFullFileBytes);
        final long evictedFullFileCount = randomLongBetween(0, activeFullFileCount);
        final long hitCount = randomLongBetween(0, 10);
        final long missCount = 10 - hitCount;
        return new FullFileCacheStats(
            activeFullFileBytes,
            activeFullFileCount,
            usedFullFileBytes,
            usedFullFileCount,
            evictedFullFileBytes,
            evictedFullFileCount,
            hitCount,
            missCount
        );
    }

    public static FileCacheStats getMockFileCacheStats() {
        final long fcSize = getMockCacheCapacity();
        return getFileCacheStats(fcSize, getMockCacheStats(), getMockCacheUsage(fcSize));
    }

    public static void validateFullFileStats(FullFileCacheStats original, FullFileCacheStats deserialized) {
        assertEquals(original.getActiveFullFileBytes(), deserialized.getActiveFullFileBytes());
        assertEquals(original.getActiveFullFileCount(), deserialized.getActiveFullFileCount());
        assertEquals(original.getUsedFullFileBytes(), deserialized.getUsedFullFileBytes());
        assertEquals(original.getUsedFullFileCount(), deserialized.getUsedFullFileCount());
        assertEquals(original.getEvictedFullFileBytes(), deserialized.getEvictedFullFileBytes());
        assertEquals(original.getEvictedFullFileCount(), deserialized.getEvictedFullFileCount());
        assertEquals(original.getHitCount(), deserialized.getHitCount());
        assertEquals(original.getMissCount(), deserialized.getMissCount());
    }

    public static void validateFileCacheStats(FileCacheStats original, FileCacheStats deserialized) {
        assertEquals(original.getTotal(), deserialized.getTotal());
        assertEquals(original.getUsed(), deserialized.getUsed());
        assertEquals(original.getUsedPercent(), deserialized.getUsedPercent());
        assertEquals(original.getActive(), deserialized.getActive());
        assertEquals(original.getActivePercent(), deserialized.getActivePercent());
        assertEquals(original.getEvicted(), deserialized.getEvicted());
        assertEquals(original.getCacheHits(), deserialized.getCacheHits());
        assertEquals(original.getCacheMisses(), deserialized.getCacheMisses());
        validateFullFileStats(original.fullFileCacheStats(), deserialized.fullFileCacheStats());
    }

    public void testFileCacheStatsSerialization() throws IOException {
        final FileCacheStats fileCacheStats = getMockFileCacheStats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            fileCacheStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                // Validate original object against deserialized values
                validateFileCacheStats(fileCacheStats, new FileCacheStats(in));
            }
        }
    }
}
