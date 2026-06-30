/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class MergeStatsTests extends OpenSearchTestCase {

    public void testDefaultConstructor() {
        MergeStats stats = new MergeStats();
        assertEquals(0, stats.getTotal());
        assertEquals(0, stats.getTotalTimeInMillis());
        assertEquals(0, stats.getTotalNumDocs());
        assertEquals(0, stats.getTotalSizeInBytes());
        assertEquals(0, stats.getCurrent());
        assertEquals(0, stats.getCurrentNumDocs());
        assertEquals(0, stats.getCurrentSizeInBytes());
        assertEquals(0, stats.getTotalStoppedTimeInMillis());
        assertEquals(0, stats.getTotalThrottledTimeInMillis());
        assertEquals(0, stats.getUnreferencedFileCleanUpsPerformed());
        assertNotNull(stats.getWarmerStats());
    }

    public void testAdd() {
        MergeStats stats = new MergeStats();
        MergedSegmentWarmerStats warmerStats = new MergedSegmentWarmerStats();
        warmerStats.add(1, 10, 0, 100, 200, 5, 15, 0);

        stats.add(5, 100, 50, 1024, 2, 25, 512, 10, 20, 1.5, warmerStats);

        assertEquals(5, stats.getTotal());
        assertEquals(100, stats.getTotalTimeInMillis());
        assertEquals(50, stats.getTotalNumDocs());
        assertEquals(1024, stats.getTotalSizeInBytes());
        assertEquals(2, stats.getCurrent());
        assertEquals(25, stats.getCurrentNumDocs());
        assertEquals(512, stats.getCurrentSizeInBytes());
        assertEquals(10, stats.getTotalStoppedTimeInMillis());
        assertEquals(20, stats.getTotalThrottledTimeInMillis());

        assertEquals(1, stats.getWarmerStats().getTotalInvocationsCount());
        assertEquals(10, stats.getWarmerStats().getTotalTime().getMillis());
        assertEquals(0, stats.getWarmerStats().getTotalFailureCount());
        assertEquals(new ByteSizeValue(100), stats.getWarmerStats().getTotalSentSize());
        assertEquals(new ByteSizeValue(200), stats.getWarmerStats().getTotalReceivedSize());
        assertEquals(0, stats.getWarmerStats().getOngoingCount());
        assertEquals(5, stats.getWarmerStats().getTotalSendTime().getMillis());
        assertEquals(15, stats.getWarmerStats().getTotalReceiveTime().getMillis());
    }

    public void testAddWithoutMergedSegmentWarmer() {
        MergeStats stats = new MergeStats();
        stats.add(5, 100, 50, 1024, 2, 25, 512, 10, 20, 1.5);

        assertEquals(5, stats.getTotal());
        assertEquals(100, stats.getTotalTimeInMillis());
        assertEquals(50, stats.getTotalNumDocs());
        assertEquals(1024, stats.getTotalSizeInBytes());
        assertEquals(2, stats.getCurrent());
        assertEquals(25, stats.getCurrentNumDocs());
        assertEquals(512, stats.getCurrentSizeInBytes());
        assertEquals(10, stats.getTotalStoppedTimeInMillis());
        assertEquals(20, stats.getTotalThrottledTimeInMillis());

        assertNotNull(stats.getWarmerStats());
        assertEquals(0, stats.getWarmerStats().getTotalInvocationsCount());
        assertEquals(0, stats.getWarmerStats().getTotalTime().getMillis());
        assertEquals(0, stats.getWarmerStats().getTotalFailureCount());
        assertEquals(new ByteSizeValue(0), stats.getWarmerStats().getTotalSentSize());
        assertEquals(new ByteSizeValue(0), stats.getWarmerStats().getTotalReceivedSize());
        assertEquals(0, stats.getWarmerStats().getOngoingCount());
        assertEquals(0, stats.getWarmerStats().getTotalSendTime().getMillis());
        assertEquals(0, stats.getWarmerStats().getTotalReceiveTime().getMillis());
    }

    public void testAddMergeStats() {
        MergeStats stats1 = new MergeStats();
        MergeStats stats2 = new MergeStats();

        MergedSegmentWarmerStats warmerStats = new MergedSegmentWarmerStats();
        warmerStats.add(1, 10, 0, 100, 200, 5, 15, 0);

        stats1.add(5, 100, 50, 1024, 2, 25, 512, 10, 20, 1.5, warmerStats);
        stats2.add(3, 50, 30, 512, 1, 15, 256, 5, 10, 1.0, warmerStats);

        stats1.add(stats2);

        assertEquals(8, stats1.getTotal());
        assertEquals(3, stats1.getCurrent());
        assertEquals(40, stats1.getCurrentNumDocs());
        assertEquals(768, stats1.getCurrentSizeInBytes());

        assertEquals(2, stats1.getWarmerStats().getTotalInvocationsCount());
        assertEquals(20, stats1.getWarmerStats().getTotalTime().getMillis());
        assertEquals(0, stats1.getWarmerStats().getTotalFailureCount());
        assertEquals(new ByteSizeValue(200), stats1.getWarmerStats().getTotalSentSize());
        assertEquals(new ByteSizeValue(400), stats1.getWarmerStats().getTotalReceivedSize());
        assertEquals(0, stats1.getWarmerStats().getOngoingCount());
        assertEquals(10, stats1.getWarmerStats().getTotalSendTime().getMillis());
        assertEquals(30, stats1.getWarmerStats().getTotalReceiveTime().getMillis());
    }

    public void testAddTotals() {
        MergeStats stats1 = new MergeStats();
        MergeStats stats2 = new MergeStats();

        MergedSegmentWarmerStats warmerStats = new MergedSegmentWarmerStats();
        warmerStats.add(1, 10, 0, 100, 200, 5, 15, 7);

        stats1.add(5, 100, 50, 1024, 2, 25, 512, 10, 20, 1.5, warmerStats);
        stats2.add(3, 50, 30, 512, 1, 15, 256, 5, 10, 1.0, warmerStats);

        stats1.addTotals(stats2);

        assertEquals(8, stats1.getTotal());
        assertEquals(2, stats1.getCurrent()); // not expected to get added with addTotals
        assertEquals(25, stats1.getCurrentNumDocs()); // not expected to get added with addTotals
        assertEquals(512, stats1.getCurrentSizeInBytes()); // not expected to get added with addTotals
        assertEquals(150, stats1.getTotalTimeInMillis());
        assertEquals(80, stats1.getTotalNumDocs());
        assertEquals(1536, stats1.getTotalSizeInBytes());
        assertEquals(15, stats1.getTotalStoppedTimeInMillis());
        assertEquals(30, stats1.getTotalThrottledTimeInMillis());

        assertEquals(2, stats1.getWarmerStats().getTotalInvocationsCount());
        assertEquals(20, stats1.getWarmerStats().getTotalTime().getMillis());
        assertEquals(0, stats1.getWarmerStats().getTotalFailureCount());
        assertEquals(new ByteSizeValue(200), stats1.getWarmerStats().getTotalSentSize());
        assertEquals(new ByteSizeValue(400), stats1.getWarmerStats().getTotalReceivedSize());
        assertEquals(7, stats1.getWarmerStats().getOngoingCount()); // not expected to get added with addTotals
        assertEquals(10, stats1.getWarmerStats().getTotalSendTime().getMillis());
        assertEquals(30, stats1.getWarmerStats().getTotalReceiveTime().getMillis());
    }

    public void testAddWithNull() {
        MergeStats stats = new MergeStats();
        stats.add((MergeStats) null);
        stats.addTotals(null);

        assertEquals(0, stats.getTotal());
        assertEquals(0, stats.getCurrent());
    }

    public void testUnreferencedFileCleanUpStats() {
        MergeStats stats = new MergeStats();
        stats.addUnreferencedFileCleanUpStats(5);
        assertEquals(5, stats.getUnreferencedFileCleanUpsPerformed());

        stats.addUnreferencedFileCleanUpStats(3);
        assertEquals(8, stats.getUnreferencedFileCleanUpsPerformed());
    }

    public void testGetters() {
        MergeStats stats = new MergeStats();
        MergedSegmentWarmerStats warmerStats = new MergedSegmentWarmerStats();
        warmerStats.add(1, 10, 0, 100, 200, 5, 15, 0);

        stats.add(5, 100, 50, 1024, 2, 25, 512, 10, 20, 1.5, warmerStats);

        assertEquals(new TimeValue(100), stats.getTotalTime());
        assertEquals(new TimeValue(10), stats.getTotalStoppedTime());
        assertEquals(new TimeValue(20), stats.getTotalThrottledTime());
        assertEquals(new ByteSizeValue(1024), stats.getTotalSize());
        assertEquals(new ByteSizeValue(512), stats.getCurrentSize());
        assertTrue(stats.getTotalBytesPerSecAutoThrottle() > 0);
    }

    public void testAutoThrottleMaxValue() {
        MergeStats stats1 = new MergeStats();
        MergeStats stats2 = new MergeStats();

        MergedSegmentWarmerStats warmerStats = new MergedSegmentWarmerStats();

        stats1.add(1, 10, 5, 100, 0, 0, 0, 0, 0, Double.MAX_VALUE, warmerStats);
        stats2.add(1, 10, 5, 100, 0, 0, 0, 0, 0, 1.0, warmerStats);

        stats1.addTotals(stats2);
        assertEquals(Long.MAX_VALUE, stats1.getTotalBytesPerSecAutoThrottle());
    }

    public void testSerialization() throws IOException {
        MergeStats original = new MergeStats();
        MergedSegmentWarmerStats warmerStats = new MergedSegmentWarmerStats();
        warmerStats.add(1, 10, 0, 100, 200, 5, 15, 0);

        original.add(5, 100, 50, 1024, 2, 25, 512, 10, 20, 1.5, warmerStats);
        original.addUnreferencedFileCleanUpStats(3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        BytesReference bytes = out.bytes();
        StreamInput in = bytes.streamInput();
        MergeStats deserialized = new MergeStats(in);

        assertEquals(original.getTotal(), deserialized.getTotal());
        assertEquals(original.getTotalTimeInMillis(), deserialized.getTotalTimeInMillis());
        assertEquals(original.getTotalNumDocs(), deserialized.getTotalNumDocs());
        assertEquals(original.getTotalSizeInBytes(), deserialized.getTotalSizeInBytes());
        assertEquals(original.getCurrent(), deserialized.getCurrent());
        assertEquals(original.getCurrentNumDocs(), deserialized.getCurrentNumDocs());
        assertEquals(original.getCurrentSizeInBytes(), deserialized.getCurrentSizeInBytes());
        assertEquals(original.getTotalStoppedTimeInMillis(), deserialized.getTotalStoppedTimeInMillis());
        assertEquals(original.getTotalThrottledTimeInMillis(), deserialized.getTotalThrottledTimeInMillis());
        assertEquals(original.getTotalBytesPerSecAutoThrottle(), deserialized.getTotalBytesPerSecAutoThrottle());
    }

    public void testToXContent() throws IOException {
        MergeStats stats = new MergeStats();
        MergedSegmentWarmerStats warmerStats = new MergedSegmentWarmerStats();
        warmerStats.add(1, 10, 0, 100, 200, 5, 15, 0);

        stats.add(5, 100, 50, 1024, 2, 25, 512, 10, 20, 1.5, warmerStats);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("merges"));
        assertTrue(json.contains("current"));
        assertTrue(json.contains("total"));
        assertTrue(json.contains("total_time_in_millis"));
        assertTrue(json.contains("total_docs"));
        assertTrue(json.contains("total_size_in_bytes"));
        assertTrue(json.contains("warmer"));
    }
}
