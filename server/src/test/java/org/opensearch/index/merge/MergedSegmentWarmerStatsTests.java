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

public class MergedSegmentWarmerStatsTests extends OpenSearchTestCase {

    public void testDefaultConstructor() {
        MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        assertEquals(0, stats.getTotalInvocationsCount());
        assertEquals(0, stats.getTotalTime().getMillis());
        assertEquals(0, stats.getTotalFailureCount());
        assertEquals(0, stats.getTotalSentSize().getBytes());
        assertEquals(0, stats.getTotalReceivedSize().getBytes());
        assertEquals(0, stats.getTotalSendTime().millis());
        assertEquals(0, stats.getTotalReceiveTime().millis());
        assertEquals(0, stats.getOngoingCount());
    }

    public void testAdd() {
        MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        stats.add(5, 100, 2, 1024, 2048, 50, 75, 3);

        assertEquals(5, stats.getTotalInvocationsCount());
        assertEquals(100, stats.getTotalTime().getMillis());
        assertEquals(2, stats.getTotalFailureCount());
        assertEquals(1024, stats.getTotalSentSize().getBytes());
        assertEquals(2048, stats.getTotalReceivedSize().getBytes());
        assertEquals(50, stats.getTotalSendTime().millis());
        assertEquals(75, stats.getTotalReceiveTime().millis());
        assertEquals(3, stats.getOngoingCount());
    }

    public void testAddMultiple() {
        MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        stats.add(5, 100, 2, 1024, 2048, 50, 75, 3);
        stats.add(3, 50, 1, 512, 1024, 25, 30, 1);

        assertEquals(8, stats.getTotalInvocationsCount());
        assertEquals(150, stats.getTotalTime().getMillis());
        assertEquals(3, stats.getTotalFailureCount());
        assertEquals(1536, stats.getTotalSentSize().getBytes());
        assertEquals(3072, stats.getTotalReceivedSize().getBytes());
        assertEquals(75, stats.getTotalSendTime().millis());
        assertEquals(105, stats.getTotalReceiveTime().millis());
        assertEquals(4, stats.getOngoingCount());
    }

    public void testAddStats() {
        MergedSegmentWarmerStats stats1 = new MergedSegmentWarmerStats();
        stats1.add(5, 100, 2, 1024, 2048, 50, 75, 3);

        MergedSegmentWarmerStats stats2 = new MergedSegmentWarmerStats();
        stats2.add(3, 50, 1, 512, 1024, 25, 30, 1);

        stats1.add(stats2);
        assertEquals(4, stats1.getOngoingCount());
    }

    public void testAddTotals() {
        MergedSegmentWarmerStats stats1 = new MergedSegmentWarmerStats();
        stats1.add(5, 100, 2, 1024, 2048, 50, 75, 3);

        MergedSegmentWarmerStats stats2 = new MergedSegmentWarmerStats();
        stats2.add(3, 50, 1, 512, 1024, 25, 30, 1);

        stats1.addTotals(stats2);
        assertEquals(8, stats1.getTotalInvocationsCount());
        assertEquals(150, stats1.getTotalTime().getMillis());
        assertEquals(3, stats1.getTotalFailureCount());
        assertEquals(1536, stats1.getTotalSentSize().getBytes());
        assertEquals(3072, stats1.getTotalReceivedSize().getBytes());
        assertEquals(75, stats1.getTotalSendTime().millis());
        assertEquals(105, stats1.getTotalReceiveTime().millis());
    }

    public void testAddTotalsWithNull() {
        MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        stats.add(5, 100, 2, 1024, 2048, 50, 75, 3);

        stats.addTotals(null);
        assertEquals(5, stats.getTotalInvocationsCount());
    }

    public void testSerialization() throws IOException {
        MergedSegmentWarmerStats original = new MergedSegmentWarmerStats();
        original.add(5, 100, 2, 1024, 2048, 50, 75, 3);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        BytesReference bytes = out.bytes();
        StreamInput in = bytes.streamInput();
        MergedSegmentWarmerStats deserialized = new MergedSegmentWarmerStats(in);

        assertEquals(original.getTotalInvocationsCount(), deserialized.getTotalInvocationsCount());
        assertEquals(original.getTotalTime().getMillis(), deserialized.getTotalTime().getMillis());
        assertEquals(original.getTotalFailureCount(), deserialized.getTotalFailureCount());
        assertEquals(original.getTotalSentSize().getBytes(), deserialized.getTotalSentSize().getBytes());
        assertEquals(original.getTotalReceivedSize().getBytes(), deserialized.getTotalReceivedSize().getBytes());
        assertEquals(original.getTotalSendTime().millis(), deserialized.getTotalSendTime().millis());
        assertEquals(original.getTotalReceiveTime().millis(), deserialized.getTotalReceiveTime().millis());
        assertEquals(original.getOngoingCount(), deserialized.getOngoingCount());
    }

    public void testToXContent() throws IOException {
        MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        stats.add(5, 100, 2, 1024, 2048, 50, 75, 3);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("warmer"));
        assertTrue(json.contains("total_invocations_count"));
        assertTrue(json.contains("total_time_millis"));
        assertTrue(json.contains("total_failure_count"));
        assertTrue(json.contains("total_bytes_sent"));
        assertTrue(json.contains("total_bytes_received"));
        assertTrue(json.contains("total_send_time_millis"));
        assertTrue(json.contains("total_receive_time_millis"));
        assertTrue(json.contains("ongoing_count"));
    }

    public void testGetters() {
        MergedSegmentWarmerStats stats = new MergedSegmentWarmerStats();
        stats.add(5, 100, 2, 1024, 2048, 50, 75, 3);

        assertEquals(5, stats.getTotalInvocationsCount());
        assertEquals(new TimeValue(100), stats.getTotalTime());
        assertEquals(2, stats.getTotalFailureCount());
        assertEquals(new ByteSizeValue(1024), stats.getTotalSentSize());
        assertEquals(new ByteSizeValue(2048), stats.getTotalReceivedSize());
        assertEquals(new TimeValue(50), stats.getTotalSendTime());
        assertEquals(new TimeValue(75), stats.getTotalReceiveTime());
        assertEquals(3, stats.getOngoingCount());
    }
}
