/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

public class MergedSegmentTransferTrackerTests extends OpenSearchTestCase {

    private MergedSegmentTransferTracker tracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tracker = new MergedSegmentTransferTracker();
    }

    public void testInitialStats() {
        MergedSegmentWarmerStats stats = tracker.stats();
        assertEquals(0, stats.getTotalInvocationsCount());
        assertEquals(TimeValue.ZERO, stats.getTotalTime());
        assertEquals(0, stats.getTotalFailureCount());
        assertEquals(ByteSizeValue.class, stats.getTotalSentSize().getClass());
        assertEquals(0, stats.getTotalSentSize().getBytes());
        assertEquals(ByteSizeValue.class, stats.getTotalReceivedSize().getClass());
        assertEquals(0, stats.getTotalReceivedSize().getBytes());
        assertEquals(TimeValue.ZERO, stats.getTotalSendTime());
        assertEquals(TimeValue.ZERO, stats.getTotalReceiveTime());
        assertEquals(0, stats.getOngoingCount());
    }

    public void testIncrementCounters() {
        tracker.incrementTotalWarmInvocationsCount();
        tracker.incrementTotalWarmFailureCount();

        MergedSegmentWarmerStats stats = tracker.stats();
        assertEquals(1, stats.getTotalInvocationsCount());
        assertEquals(1, stats.getTotalFailureCount());
    }

    public void testOngoingWarms() {
        tracker.incrementOngoingWarms();
        tracker.incrementOngoingWarms();
        assertEquals(2, tracker.stats().getOngoingCount());

        tracker.decrementOngoingWarms();
        assertEquals(1, tracker.stats().getOngoingCount());
    }

    public void testAddTimeAndBytes() {
        tracker.addTotalWarmTimeMillis(100);
        tracker.addTotalSendTimeMillis(200);
        tracker.addTotalReceiveTimeMillis(300);
        tracker.addTotalBytesSent(1024);
        tracker.addTotalBytesReceived(2048);

        MergedSegmentWarmerStats stats = tracker.stats();
        assertEquals(new TimeValue(100), stats.getTotalTime());
        assertEquals(new TimeValue(200), stats.getTotalSendTime());
        assertEquals(new TimeValue(300), stats.getTotalReceiveTime());
        assertEquals(1024, stats.getTotalSentSize().getBytes());
        assertEquals(2048, stats.getTotalReceivedSize().getBytes());
    }

    public void testCumulativeStats() {
        tracker.addTotalWarmTimeMillis(100);
        tracker.addTotalWarmTimeMillis(50);
        assertEquals(new TimeValue(150), tracker.stats().getTotalTime());

        tracker.addTotalBytesSent(1000);
        tracker.addTotalBytesSent(500);
        assertEquals(1500, tracker.stats().getTotalSentSize().getBytes());
    }
}
