/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stores stats about a merge process
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MergedSegmentWarmerStats implements Writeable, ToXContentFragment {

    // [PRIMARY SHARD] Number of times segment MergedSegmentWarmer.warm has been invoked
    private long totalWarmInvocationsCount;

    // [PRIMARY SHARD] Total time spent warming segments in milliseconds
    private long totalWarmTimeMillis;

    // [PRIMARY SHARD] Number of times segment warming has failed
    private long totalWarmFailureCount;

    // [PRIMARY SHARD] Total bytes sent during segment warming
    private long totalBytesSent;

    // [REPLICA SHARD] Total bytes received during segment warming
    private long totalBytesReceived;

    // [PRIMARY SHARD] Total time spent uploading segments in milliseconds by a primary shard
    private long totalUploadTimeMillis;

    // [REPLICA SHARD] Total time spent downloading segments in milliseconds
    private long totalDownloadTimeMillis;

    // [PRIMARY SHARD] Current number of ongoing segment warming operations
    private long ongoingWarms;

    public MergedSegmentWarmerStats() {}

    public MergedSegmentWarmerStats(StreamInput in) throws IOException {
        totalWarmInvocationsCount = in.readVLong();
        totalWarmTimeMillis = in.readVLong();
        totalWarmFailureCount = in.readVLong();
        totalBytesSent = in.readVLong();
        totalBytesReceived = in.readVLong();
        totalUploadTimeMillis = in.readVLong();
        totalDownloadTimeMillis = in.readVLong();
        ongoingWarms = in.readVLong();
    }

    public synchronized void add(
        long totalWarmInvocationsCount,
        long totalWarmTimeMillis,
        long totalWarmFailureCount,
        long totalBytesSent,
        long totalBytesReceived,
        long totalUploadTimeMillis,
        long totalDownloadTimeMillis,
        long ongoingWarms
    ) {
        this.totalWarmInvocationsCount += totalWarmInvocationsCount;
        this.totalWarmTimeMillis += totalWarmTimeMillis;
        this.totalWarmFailureCount += totalWarmFailureCount;
        this.totalBytesSent += totalBytesSent;
        this.totalBytesReceived += totalBytesReceived;
        this.totalUploadTimeMillis += totalUploadTimeMillis;
        this.totalDownloadTimeMillis += totalDownloadTimeMillis;
        this.ongoingWarms += ongoingWarms;
    }

    public void add(MergedSegmentWarmerStats mergedSegmentWarmerStats) {
        add(mergedSegmentWarmerStats, true);
    }

    public void add(MergedSegmentWarmerStats mergedSegmentWarmerStats, boolean addTotals) {
        if (mergedSegmentWarmerStats == null) {
            return;
        }
        this.ongoingWarms += mergedSegmentWarmerStats.ongoingWarms;

        if (addTotals) {
            addTotals(mergedSegmentWarmerStats);
        }
    }

    public synchronized void addTotals(MergedSegmentWarmerStats mergedSegmentWarmerStats) {
        if (mergedSegmentWarmerStats == null) {
            return;
        }
        this.totalWarmInvocationsCount += mergedSegmentWarmerStats.totalWarmInvocationsCount;
        this.totalWarmTimeMillis += mergedSegmentWarmerStats.totalWarmTimeMillis;
        this.totalWarmFailureCount += mergedSegmentWarmerStats.totalWarmFailureCount;
        this.totalBytesSent += mergedSegmentWarmerStats.totalBytesSent;
        this.totalBytesReceived += mergedSegmentWarmerStats.totalBytesReceived;
        this.totalUploadTimeMillis += mergedSegmentWarmerStats.totalUploadTimeMillis;
        this.totalDownloadTimeMillis += mergedSegmentWarmerStats.totalDownloadTimeMillis;
    }

    public long getTotalWarmInvocationsCount() {
        return this.totalWarmInvocationsCount;
    }

    public TimeValue getTotalWarmTime() {
        return new TimeValue(totalWarmTimeMillis);
    }

    public long getOngoingWarms() {
        return ongoingWarms;
    }

    public ByteSizeValue getTotalReceivedSize() {
        return new ByteSizeValue(totalBytesReceived);
    }

    public ByteSizeValue getTotalSentSize() {
        return new ByteSizeValue(totalBytesSent);
    }

    public TimeValue getTotalDownloadTime() {
        return new TimeValue(totalDownloadTimeMillis);
    }

    public long getTotalWarmFailureCount() {
        return totalWarmFailureCount;
    }

    public TimeValue getTotalUploadTime() {
        return new TimeValue(totalUploadTimeMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.MERGED_SEGMENT_WARMER);
        builder.field(Fields.TOTAL_WARM_INVOCATIONS_COUNT, totalWarmInvocationsCount);
        builder.humanReadableField(Fields.TOTAL_WARM_TIME_MILLIS, Fields.TOTAL_WARM_TIME, getTotalWarmTime());
        builder.field(Fields.TOTAL_WARM_FAILURE_COUNT, totalWarmFailureCount);
        builder.humanReadableField(Fields.TOTAL_BYTES_SENT, Fields.TOTAL_SENT_SIZE, getTotalSentSize());
        builder.humanReadableField(Fields.TOTAL_BYTES_RECEIVED, Fields.TOTAL_RECEIVED_SIZE, getTotalReceivedSize());
        builder.humanReadableField(Fields.TOTAL_UPLOAD_TIME_MILLIS, Fields.TOTAL_UPLOAD_TIME, totalUploadTimeMillis);
        builder.humanReadableField(Fields.TOTAL_DOWNLOAD_TIME_MILLIS, Fields.TOTAL_DOWNLOAD_TIME, totalDownloadTimeMillis);
        builder.field(Fields.ONGOING_WARMS, ongoingWarms);
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
        static final String TOTAL_WARM_INVOCATIONS_COUNT = "total_warm_invocations_count";
        static final String TOTAL_WARM_TIME_MILLIS = "total_warm_time_millis";
        static final String TOTAL_WARM_FAILURE_COUNT = "total_warm_failure_count";
        static final String TOTAL_BYTES_SENT = "total_bytes_sent";
        static final String TOTAL_BYTES_RECEIVED = "total_bytes_received";
        static final String TOTAL_UPLOAD_TIME_MILLIS = "total_upload_time_millis";
        static final String TOTAL_DOWNLOAD_TIME_MILLIS = "total_download_time_millis";
        static final String ONGOING_WARMS = "ongoing_warms";

        public static final String TOTAL_WARM_TIME = "total_warm_time";
        public static final String TOTAL_UPLOAD_TIME = "total_upload_time";
        public static final String TOTAL_DOWNLOAD_TIME = "total_download_time";
        public static final String TOTAL_SENT_SIZE = "total_sent_size";
        public static final String TOTAL_RECEIVED_SIZE = "total_received_size";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalWarmInvocationsCount);
        out.writeVLong(totalWarmTimeMillis);
        out.writeVLong(totalWarmFailureCount);
        out.writeVLong(totalBytesSent);
        out.writeVLong(totalBytesReceived);
        out.writeVLong(totalUploadTimeMillis);
        out.writeVLong(totalDownloadTimeMillis);
        out.writeVLong(ongoingWarms);
    }
}
