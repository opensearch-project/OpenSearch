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
        final long active = randomLongBetween(100000, BYTES_IN_GB);
        final long used = randomLongBetween(100000, BYTES_IN_GB);
        final long evicted = randomLongBetween(0, active);
        final long hits = randomLongBetween(0, 10);
        return new FullFileCacheStats(active, used, evicted, hits);
    }

    public static void validateFullFileCacheStats(FullFileCacheStats expected, FullFileCacheStats actual) {
        assertEquals(expected.getActive(), actual.getActive());
        assertEquals(expected.getUsed(), actual.getUsed());
        assertEquals(expected.getEvicted(), actual.getEvicted());
        assertEquals(expected.getHits(), actual.getHits());
        assertEquals(expected.getActivePercent(), actual.getActivePercent());
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
