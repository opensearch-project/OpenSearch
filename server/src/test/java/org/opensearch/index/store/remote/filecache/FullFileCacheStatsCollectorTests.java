/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.opensearch.test.OpenSearchTestCase;

public class FullFileCacheStatsCollectorTests extends OpenSearchTestCase {

    public void testRecordUsedFileBytes() {
        FullFileCacheStatsCollector collector = new FullFileCacheStatsCollector();
        collector.recordUsedFileBytes(100, true);
        collector.recordUsedFileBytes(200, false);
        collector.recordUsedFileBytes(300, true);

        FullFileCacheStats stats = collector.getStats();
        assertEquals(200, stats.getUsedFullFileBytes());
        assertEquals(1, stats.getUsedFullFileCount());
    }

    public void testRecordActiveFileBytes() {
        FullFileCacheStatsCollector collector = new FullFileCacheStatsCollector();
        collector.recordActiveFileBytes(100, true);
        collector.recordActiveFileBytes(200, false);
        collector.recordActiveFileBytes(300, true);

        FullFileCacheStats stats = collector.getStats();
        assertEquals(200, stats.getActiveFullFileBytes());
        assertEquals(1, stats.getActiveFullFileCount());
    }

    public void testRecordHits() {
        FullFileCacheStatsCollector collector = new FullFileCacheStatsCollector();
        collector.recordHit(true);
        collector.recordHit(false);
        collector.recordHit(true);

        FullFileCacheStats stats = collector.getStats();
        assertEquals(2, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
    }
}
