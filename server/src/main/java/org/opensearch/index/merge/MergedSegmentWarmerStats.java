/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stores stats about a merged segment warmer process
 *
 * @opensearch.api
 */
@ExperimentalApi
public class MergedSegmentWarmerStats implements Writeable, ToXContentFragment {

    // [PRIMARY SHARD] Number of times segment MergedSegmentWarmer.warm has been invoked
    private long totalInvocationsCount;

    // [PRIMARY SHARD] Total time spent warming segments in milliseconds
    private long totalTimeMillis;

    // [PRIMARY SHARD] Number of times segment warming has failed
    private long totalFailureCount;

    // [PRIMARY SHARD] Total bytes sent during segment warming
    private long totalBytesSent;

    // [REPLICA SHARD] Total bytes received during segment warming
    private long totalBytesReceived;

    // [PRIMARY SHARD] Total time spent sending segments in milliseconds by a primary shard
    private long totalSendTimeMillis;

    // [REPLICA SHARD] Total time spent receiving segments in milliseconds
    private long totalReceiveTimeMillis;

    // [PRIMARY SHARD] Current number of ongoing segment warming operations
    private long ongoingCount;

    public MergedSegmentWarmerStats() {}

    public MergedSegmentWarmerStats(StreamInput in) throws IOException {
        totalInvocationsCount = in.readVLong();
        totalTimeMillis = in.readVLong();
        totalFailureCount = in.readVLong();
        totalBytesSent = in.readVLong();
        totalBytesReceived = in.readVLong();
        totalSendTimeMillis = in.readVLong();
        totalReceiveTimeMillis = in.readVLong();
        ongoingCount = in.readVLong();
    }

    public synchronized void add(
        long totalInvocationsCount,
        long totalTimeMillis,
        long totalFailureCount,
        long totalBytesSent,
        long totalBytesReceived,
        long totalSendTimeMillis,
        long totalReceiveTimeMillis,
        long ongoingCount
    ) {
        this.totalInvocationsCount += totalInvocationsCount;
        this.totalTimeMillis += totalTimeMillis;
        this.totalFailureCount += totalFailureCount;
        this.totalBytesSent += totalBytesSent;
        this.totalBytesReceived += totalBytesReceived;
        this.totalSendTimeMillis += totalSendTimeMillis;
        this.totalReceiveTimeMillis += totalReceiveTimeMillis;
        this.ongoingCount += ongoingCount;
    }

    public void add(MergedSegmentWarmerStats mergedSegmentWarmerStats) {
        this.ongoingCount += mergedSegmentWarmerStats.ongoingCount;
    }

    public synchronized void addTotals(MergedSegmentWarmerStats mergedSegmentWarmerStats) {
        if (mergedSegmentWarmerStats == null) {
            return;
        }
        this.totalInvocationsCount += mergedSegmentWarmerStats.totalInvocationsCount;
        this.totalTimeMillis += mergedSegmentWarmerStats.totalTimeMillis;
        this.totalFailureCount += mergedSegmentWarmerStats.totalFailureCount;
        this.totalBytesSent += mergedSegmentWarmerStats.totalBytesSent;
        this.totalBytesReceived += mergedSegmentWarmerStats.totalBytesReceived;
        this.totalSendTimeMillis += mergedSegmentWarmerStats.totalSendTimeMillis;
        this.totalReceiveTimeMillis += mergedSegmentWarmerStats.totalReceiveTimeMillis;
    }

    public long getTotalInvocationsCount() {
        return this.totalInvocationsCount;
    }

    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeMillis);
    }

    public long getOngoingCount() {
        return ongoingCount;
    }

    public ByteSizeValue getTotalReceivedSize() {
        return new ByteSizeValue(totalBytesReceived);
    }

    public ByteSizeValue getTotalSentSize() {
        return new ByteSizeValue(totalBytesSent);
    }

    public TimeValue getTotalReceiveTime() {
        return new TimeValue(totalReceiveTimeMillis);
    }

    public long getTotalFailureCount() {
        return totalFailureCount;
    }

    public TimeValue getTotalSendTime() {
        return new TimeValue(totalSendTimeMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.WARMER);
        builder.field(Fields.WARM_INVOCATIONS_COUNT, totalInvocationsCount);
        builder.humanReadableField(Fields.TOTAL_TIME_MILLIS, Fields.TOTAL_TIME, getTotalTime());
        builder.field(Fields.TOTAL_FAILURE_COUNT, totalFailureCount);
        builder.humanReadableField(Fields.TOTAL_BYTES_SENT, Fields.TOTAL_SENT_SIZE, getTotalSentSize());
        builder.humanReadableField(Fields.TOTAL_BYTES_RECEIVED, Fields.TOTAL_RECEIVED_SIZE, getTotalReceivedSize());
        builder.humanReadableField(Fields.TOTAL_SEND_TIME_MILLIS, Fields.TOTAL_SEND_TIME, getTotalSendTime());
        builder.humanReadableField(Fields.TOTAL_RECEIVE_TIME_MILLIS, Fields.TOTAL_RECEIVE_TIME, getTotalReceiveTime());
        builder.field(Fields.ONGOING_COUNT, ongoingCount);
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for merge statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String WARMER = "warmer";
        static final String WARM_INVOCATIONS_COUNT = "total_invocations_count";
        static final String TOTAL_TIME_MILLIS = "total_time_millis";
        static final String TOTAL_FAILURE_COUNT = "total_failure_count";
        static final String TOTAL_BYTES_SENT = "total_bytes_sent";
        static final String TOTAL_BYTES_RECEIVED = "total_bytes_received";
        static final String TOTAL_SEND_TIME_MILLIS = "total_send_time_millis";
        static final String TOTAL_RECEIVE_TIME_MILLIS = "total_receive_time_millis";
        static final String ONGOING_COUNT = "ongoing_count";

        public static final String TOTAL_TIME = "total_time";
        public static final String TOTAL_SEND_TIME = "total_send_time";
        public static final String TOTAL_RECEIVE_TIME = "total_receive_time";
        public static final String TOTAL_SENT_SIZE = "total_sent_size";
        public static final String TOTAL_RECEIVED_SIZE = "total_received_size";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalInvocationsCount);
        out.writeVLong(totalTimeMillis);
        out.writeVLong(totalFailureCount);
        out.writeVLong(totalBytesSent);
        out.writeVLong(totalBytesReceived);
        out.writeVLong(totalSendTimeMillis);
        out.writeVLong(totalReceiveTimeMillis);
        out.writeVLong(ongoingCount);
    }
}
