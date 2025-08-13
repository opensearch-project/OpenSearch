/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.common.annotation.PublicApi;
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
    private long totalWarmInvocationsCount;
    private long totalWarmTimeMillis;
    private long totalWarmFailureCount;
    private long totalBytesUploaded;
    private long totalBytesDownloaded;
    private long totalUploadTimeMillis;
    private long totalDownloadTimeMillis;
    private long ongoingWarms;

    public MergedSegmentWarmerStats() {}

    public MergedSegmentWarmerStats(StreamInput in) throws IOException {
        totalWarmInvocationsCount = in.readVLong();
        totalWarmTimeMillis = in.readVLong();
        totalWarmFailureCount = in.readVLong();
        totalBytesUploaded = in.readVLong();
        totalBytesDownloaded = in.readVLong();
        totalUploadTimeMillis = in.readVLong();
        totalDownloadTimeMillis = in.readVLong();
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
        long ongoingWarms
    ) {
        this.totalWarmInvocationsCount += totalWarmInvocationsCount;
        this.totalWarmTimeMillis += totalWarmTimeMillis;
        this.totalWarmFailureCount += totalWarmFailureCount;
        this.totalBytesUploaded += totalBytesUploaded;
        this.totalBytesDownloaded += totalBytesDownloaded;
        this.totalUploadTimeMillis += totalUploadTimeMillis;
        this.totalDownloadTimeMillis += totalDownloadTimeMillis;
        this.ongoingWarms += ongoingWarms;
    }

    public void add(MergedSegmentWarmerStats mergedSegmentWarmerStats) {
        if (mergedSegmentWarmerStats == null) {
            return;
        }
        this.ongoingWarms += mergedSegmentWarmerStats.ongoingWarms;

        addTotals(mergedSegmentWarmerStats);
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
    }

    public long getTotalWarmInvocationsCount() {
        return this.totalWarmInvocationsCount;
    }

    public long getTotalWarmTimeMillis() {
        return this.totalWarmTimeMillis;
    }

    public long getOngoingWarms() {
        return ongoingWarms;
    }

    public long getTotalBytesDownloaded() {
        return totalBytesDownloaded;
    }

    public long getTotalBytesUploaded() {
        return totalBytesUploaded;
    }

    public long getTotalDownloadTimeMillis() {
        return totalDownloadTimeMillis;
    }

    public long getTotalWarmFailureCount() {
        return totalWarmFailureCount;
    }

    public long getTotalUploadTimeMillis() {
        return totalUploadTimeMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.MERGED_SEGMENT_WARMER);
        builder.field(Fields.TOTAL_WARM_INVOCATIONS_COUNT, totalWarmInvocationsCount);
        builder.field(Fields.TOTAL_WARM_TIME_MILLIS, totalWarmTimeMillis);
        builder.field(Fields.TOTAL_WARM_FAILURE_COUNT, totalWarmFailureCount);
        builder.field(Fields.TOTAL_BYTES_UPLOADED, new ByteSizeValue(totalBytesUploaded));
        builder.field(Fields.TOTAL_BYTES_DOWNLOADED, new ByteSizeValue(totalBytesDownloaded));
        builder.field(Fields.TOTAL_UPLOAD_TIME_MILLIS, totalUploadTimeMillis);
        builder.field(Fields.TOTAL_DOWNLOAD_TIME_MILLIS, totalDownloadTimeMillis);
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
        static final String TOTAL_BYTES_UPLOADED = "total_bytes_uploaded";
        static final String TOTAL_BYTES_DOWNLOADED = "total_bytes_downloaded";
        static final String TOTAL_UPLOAD_TIME_MILLIS = "total_upload_time_millis";
        static final String TOTAL_DOWNLOAD_TIME_MILLIS = "total_download_time_millis";
        static final String ONGOING_WARMS = "ongoing_warms";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalWarmInvocationsCount);
        out.writeVLong(totalWarmTimeMillis);
        out.writeVLong(totalWarmFailureCount);
        out.writeVLong(totalBytesUploaded);
        out.writeVLong(totalBytesDownloaded);
        out.writeVLong(totalUploadTimeMillis);
        out.writeVLong(totalDownloadTimeMillis);
        out.writeVLong(ongoingWarms);
    }
}
