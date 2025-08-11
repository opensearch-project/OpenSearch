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

    // [PRIMARY SHARD] Total number of warms rejected by the {@Link MergedSegmentWarmerPressureService}
    private long totalRejectedCount;

    // [PRIMARY SHARD] Current number of ongoing segment warming operations
    private long ongoingCount;

    public MergedSegmentWarmerStats() {}

    public MergedSegmentWarmerStats(StreamInput in) throws IOException {
        totalWarmInvocationsCount = in.readVLong();
        totalWarmTimeMillis = in.readVLong();
        totalWarmFailureCount = in.readVLong();
        totalBytesUploaded = in.readVLong();
        totalBytesDownloaded = in.readVLong();
        totalUploadTimeMillis = in.readVLong();
        totalDownloadTimeMillis = in.readVLong();
        totalRejectedCount = in.readVLong();
        ongoingWarms = in.readVLong();
    }

    public synchronized void add(
        long totalWarmInvocationsCount,
        long totalWarmTimeMillis,
        long totalWarmFailureCount,
        long totalBytesUploaded,
        long totalBytesDownloaded,
        long totalUploadTimeMillis,
        long totalDownloadTimeMillis,
        long totalRejectedCount,
        long ongoingWarms
    ) {
        this.totalInvocationsCount += totalInvocationsCount;
        this.totalTimeMillis += totalTimeMillis;
        this.totalFailureCount += totalFailureCount;
        this.totalBytesSent += totalBytesSent;
        this.totalBytesReceived += totalBytesReceived;
        this.totalSendTimeMillis += totalSendTimeMillis;
        this.totalReceiveTimeMillis += totalReceiveTimeMillis;
        this.totalRejectedCount += totalRejectedWarms;
        this.ongoingCount += ongoingCount;
    }

    public void add(MergedSegmentWarmerStats mergedSegmentWarmerStats) {
        this.ongoingCount += mergedSegmentWarmerStats.ongoingCount;
    }

    public synchronized void addTotals(MergedSegmentWarmerStats mergedSegmentWarmerStats) {
        if (mergedSegmentWarmerStats == null) {
            return;
        }
        this.totalWarmInvocationsCount += mergedSegmentWarmerStats.totalWarmInvocationsCount;
        this.totalWarmTimeMillis += mergedSegmentWarmerStats.totalWarmTimeMillis;
        this.totalWarmFailureCount += mergedSegmentWarmerStats.totalWarmFailureCount;
        this.totalBytesUploaded += mergedSegmentWarmerStats.totalBytesUploaded;
        this.totalBytesDownloaded += mergedSegmentWarmerStats.totalBytesDownloaded;
        this.totalUploadTimeMillis += mergedSegmentWarmerStats.totalUploadTimeMillis;
        this.totalDownloadTimeMillis += mergedSegmentWarmerStats.totalDownloadTimeMillis;
        this.totalRejectedCount += mergedSegmentWarmerStats.totalRejectedWarms;
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

    public long getTotalRejectedCount() {
        return totalRejectedCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.MERGED_SEGMENT_WARMER);
        builder.field(Fields.TOTAL_WARM_INVOCATIONS_COUNT, totalWarmInvocationsCount);
        builder.field(Fields.TOTAL_WARM_TIME_MILLIS, totalWarmTimeMillis);
        builder.field(Fields.TOTAL_WARM_FAILURE_COUNT, totalWarmFailureCount);
        builder.humanReadableField(Fields.TOTAL_BYTES_UPLOADED, Fields.TOTAL_BYTES_UPLOADED, new ByteSizeValue(totalBytesUploaded));
        builder.humanReadableField(Fields.TOTAL_BYTES_DOWNLOADED, Fields.TOTAL_BYTES_DOWNLOADED, new ByteSizeValue(totalBytesDownloaded));
        builder.field(Fields.TOTAL_UPLOAD_TIME_MILLIS, totalUploadTimeMillis);
        builder.field(Fields.TOTAL_DOWNLOAD_TIME_MILLIS, totalDownloadTimeMillis);
        builder.field(Fields.TOTAL_REJECTED_COUNT, totalRejectedCount);
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
        static final String MERGED_SEGMENT_WARMER = "merged_segment_warmer";
        static final String WARM_INVOCATIONS_COUNT = "total_invocations_count";
        static final String TOTAL_TIME_MILLIS = "total_time_millis";
        static final String TOTAL_FAILURE_COUNT = "total_failure_count";
        static final String TOTAL_BYTES_SENT = "total_bytes_sent";
        static final String TOTAL_BYTES_RECEIVED = "total_bytes_received";
        static final String TOTAL_SEND_TIME_MILLIS = "total_send_time_millis";
        static final String TOTAL_RECEIVE_TIME_MILLIS = "total_receive_time_millis";
        static final String TOTAL_REJECTED_COUNT = "total_rejected_count";
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
        out.writeVLong(totalRejectedCount);
        out.writeVLong(ongoingCount);
    }
}
