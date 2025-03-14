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
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class FullFileCacheStatsTests extends OpenSearchTestCase {

    private static final long BYTES_IN_GB = 1024 * 1024 * 1024;

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

    public static void validateFullFileCacheStats(FullFileCacheStats expected, FullFileCacheStats actual) {
        assertEquals(expected.getActiveFullFileBytes(), actual.getActiveFullFileBytes());
        assertEquals(expected.getActiveFullFileCount(), actual.getActiveFullFileCount());
        assertEquals(expected.getUsedFullFileBytes(), actual.getUsedFullFileBytes());
        assertEquals(expected.getUsedFullFileCount(), actual.getUsedFullFileCount());
        assertEquals(expected.getEvictedFullFileBytes(), actual.getEvictedFullFileBytes());
        assertEquals(expected.getEvictedFullFileCount(), actual.getEvictedFullFileCount());
        assertEquals(expected.getHitCount(), actual.getHitCount());
        assertEquals(expected.getMissCount(), actual.getMissCount());
    }

    public void testFullFileCacheStatsSerialization() throws IOException {
        final FullFileCacheStats fullFileCacheStats = getMockFullFileCacheStats();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            fullFileCacheStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                validateFullFileCacheStats(fullFileCacheStats, new FullFileCacheStats(in));
            }
        }

    }
}
