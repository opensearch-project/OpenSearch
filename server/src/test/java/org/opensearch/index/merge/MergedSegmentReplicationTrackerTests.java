/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.common.settings.Settings.builder;

public class MergedSegmentReplicationTrackerTests extends OpenSearchTestCase {

    private MergedSegmentReplicationTracker tracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ShardId shardId = new ShardId("test", "uuid", 0);
        IndexSettings indexSettings = new IndexSettings(
            newIndexMeta("test", builder().build()),
            builder().build()
        );
        tracker = new MergedSegmentReplicationTracker(shardId, indexSettings);
    }

    public void testInitialStats() {
        MergedSegmentWarmerStats stats = tracker.stats();
        assertEquals(0, stats.getTotalWarmInvocationsCount());
        assertEquals(0, stats.getTotalWarmTimeMillis());
        assertEquals(0, stats.getTotalWarmFailureCount());
        assertEquals(0, stats.getTotalBytesUploaded());
        assertEquals(0, stats.getTotalBytesDownloaded());
        assertEquals(0, stats.getTotalUploadTimeMillis());
        assertEquals(0, stats.getTotalDownloadTimeMillis());
        assertEquals(0, stats.getOngoingWarms());
    }

    public void testIncrementCounters() {
        tracker.incrementTotalWarmInvocationsCount();
        tracker.incrementTotalWarmFailureCount();
        tracker.incrementTotalRejectedWarms();
        
        MergedSegmentWarmerStats stats = tracker.stats();
        assertEquals(1, stats.getTotalWarmInvocationsCount());
        assertEquals(1, stats.getTotalWarmFailureCount());
    }

    public void testOngoingWarms() {
        tracker.incrementOngoingWarms();
        tracker.incrementOngoingWarms();
        assertEquals(2, tracker.stats().getOngoingWarms());
        
        tracker.decrementOngoingWarms();
        assertEquals(1, tracker.stats().getOngoingWarms());
    }

    public void testAddTimeAndBytes() {
        tracker.addTotalWarmTimeMillis(100);
        tracker.addTotalUploadTimeMillis(200);
        tracker.addTotalDownloadTimeMillis(300);
        tracker.addTotalBytesUploaded(1024);
        tracker.addTotalBytesDownloaded(2048);
        
        MergedSegmentWarmerStats stats = tracker.stats();
        assertEquals(100, stats.getTotalWarmTimeMillis());
        assertEquals(200, stats.getTotalUploadTimeMillis());
        assertEquals(300, stats.getTotalDownloadTimeMillis());
        assertEquals(1024, stats.getTotalBytesUploaded());
        assertEquals(2048, stats.getTotalBytesDownloaded());
    }

    public void testAccumulativeStats() {
        tracker.addTotalWarmTimeMillis(100);
        tracker.addTotalWarmTimeMillis(50);
        assertEquals(150, tracker.stats().getTotalWarmTimeMillis());
        
        tracker.addTotalBytesUploaded(1000);
        tracker.addTotalBytesUploaded(500);
        assertEquals(1500, tracker.stats().getTotalBytesUploaded());
    }
}