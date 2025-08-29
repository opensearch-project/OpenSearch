/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.common.settings.Settings.builder;
import static org.opensearch.index.IndexSettingsTests.newIndexMeta;

public class MergedSegmentTransferTrackerTests extends OpenSearchTestCase {

    private MergedSegmentTransferTracker tracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ShardId shardId = new ShardId("test", "uuid", 0);
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", builder().build()), builder().build());
        tracker = new MergedSegmentTransferTracker(shardId, indexSettings);
    }

    public void testInitialStats() {
        MergedSegmentWarmerStats stats = tracker.stats();
        assertEquals(0, stats.getTotalWarmInvocationsCount());
        assertEquals(TimeValue.ZERO, stats.getTotalWarmTime());
        assertEquals(0, stats.getTotalWarmFailureCount());
        assertEquals(0, stats.getTotalSentSize());
        assertEquals(0, stats.getTotalReceivedSize());
        assertEquals(TimeValue.ZERO, stats.getTotalUploadTime());
        assertEquals(TimeValue.ZERO, stats.getTotalDownloadTime());
        assertEquals(0, stats.getOngoingWarms());
    }

    public void testIncrementCounters() {
        tracker.incrementTotalWarmInvocationsCount();
        tracker.incrementTotalWarmFailureCount();

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
        assertEquals(new TimeValue(100), stats.getTotalWarmTime());
        assertEquals(new TimeValue(200), stats.getTotalUploadTime());
        assertEquals(new TimeValue(300), stats.getTotalDownloadTime());
        assertEquals(1024, stats.getTotalSentSize());
        assertEquals(2048, stats.getTotalReceivedSize());
    }

    public void testAccumulativeStats() {
        tracker.addTotalWarmTimeMillis(100);
        tracker.addTotalWarmTimeMillis(50);
        assertEquals(new TimeValue(150), tracker.stats().getTotalWarmTime());

        tracker.addTotalBytesUploaded(1000);
        tracker.addTotalBytesUploaded(500);
        assertEquals(1500, tracker.stats().getTotalSentSize());
    }
}
