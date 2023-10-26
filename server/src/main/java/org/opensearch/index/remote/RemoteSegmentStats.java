/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.util.Objects;

/**
 * Tracks remote store segment download and upload stats
 * Used for displaying remote store stats in IndicesStats/NodeStats API
 *
 * @opensearch.internal
 */
public class RemoteSegmentStats implements Writeable, ToXContentFragment {
    /**
     * Cumulative bytes attempted to be uploaded to remote store
     */
    private long uploadBytesStarted;
    /**
     * Cumulative bytes failed to be uploaded to the remote store
     */
    private long uploadBytesFailed;
    /**
     * Cumulative bytes successfully uploaded to the remote store
     */
    private long uploadBytesSucceeded;
    /**
     * Cumulative bytes attempted to be downloaded from the remote store
     */
    private long downloadBytesStarted;
    /**
     * Cumulative bytes failed to be downloaded from the remote store
     */
    private long downloadBytesFailed;
    /**
     * Cumulative bytes successfully downloaded from the remote store
     */
    private long downloadBytesSucceeded;
    /**
     * Maximum refresh lag (in milliseconds) between local and the remote store
     * Used to check for data freshness in the remote store
     */
    private long maxRefreshTimeLag;
    /**
     * Maximum refresh lag (in bytes) between local and the remote store
     * Used to check for data freshness in the remote store
     */
    private long maxRefreshBytesLag;
    /**
     * Total refresh lag (in bytes) between local and the remote store
     * Used to check for data freshness in the remote store
     */
    private long totalRefreshBytesLag;
    /**
     * Total time spent in uploading segments to remote store
     */
    private long totalUploadTime;
    /**
     * Total time spent in downloading segments from remote store
     */
    private long totalDownloadTime;
    /**
     * Total rejections due to remote store upload backpressure
     */
    private long totalRejections;

    public RemoteSegmentStats() {}

    public RemoteSegmentStats(StreamInput in) throws IOException {
        uploadBytesStarted = in.readLong();
        uploadBytesFailed = in.readLong();
        uploadBytesSucceeded = in.readLong();
        downloadBytesStarted = in.readLong();
        downloadBytesFailed = in.readLong();
        downloadBytesSucceeded = in.readLong();
        maxRefreshTimeLag = in.readLong();
        maxRefreshBytesLag = in.readLong();
        totalRefreshBytesLag = in.readLong();
        totalUploadTime = in.readLong();
        totalDownloadTime = in.readLong();
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            totalRejections = in.readVLong();
        }
    }

    /**
     * Constructor to retrieve metrics from {@link RemoteSegmentTransferTracker.Stats} which is used in {@link RemoteStoreStats} and
     * provides verbose index level stats of segments transferred to the remote store.
     * <p>
     * This method is used in {@link IndexShard} to port over a subset of metrics to be displayed in IndexStats and subsequently rolled up to NodesStats
     *
     * @param trackerStats: Source {@link RemoteSegmentTransferTracker.Stats} object from which metrics would be retrieved
     */
    public RemoteSegmentStats(RemoteSegmentTransferTracker.Stats trackerStats) {
        this.uploadBytesStarted = trackerStats.uploadBytesStarted;
        this.uploadBytesFailed = trackerStats.uploadBytesFailed;
        this.uploadBytesSucceeded = trackerStats.uploadBytesSucceeded;
        this.downloadBytesSucceeded = trackerStats.directoryFileTransferTrackerStats.transferredBytesSucceeded;
        this.downloadBytesStarted = trackerStats.directoryFileTransferTrackerStats.transferredBytesStarted;
        this.downloadBytesFailed = trackerStats.directoryFileTransferTrackerStats.transferredBytesFailed;
        this.maxRefreshTimeLag = trackerStats.refreshTimeLagMs;
        // Initializing both total and max bytes lag to the same `bytesLag`
        // value from the tracker object
        // Aggregations would be performed on the add method
        this.maxRefreshBytesLag = trackerStats.bytesLag;
        this.totalRefreshBytesLag = trackerStats.bytesLag;
        this.totalUploadTime = trackerStats.totalUploadTimeInMs;
        this.totalDownloadTime = trackerStats.directoryFileTransferTrackerStats.totalTransferTimeInMs;
        this.totalRejections = trackerStats.rejectionCount;
    }

    // Getter and setters. All are visible for testing
    // Setters are only used for testing
    public long getUploadBytesStarted() {
        return uploadBytesStarted;
    }

    public void addUploadBytesStarted(long uploadsStarted) {
        this.uploadBytesStarted += uploadsStarted;
    }

    public long getUploadBytesFailed() {
        return uploadBytesFailed;
    }

    public void addUploadBytesFailed(long uploadsFailed) {
        this.uploadBytesFailed += uploadsFailed;
    }

    public long getUploadBytesSucceeded() {
        return uploadBytesSucceeded;
    }

    public void addUploadBytesSucceeded(long uploadsSucceeded) {
        this.uploadBytesSucceeded += uploadsSucceeded;
    }

    public long getDownloadBytesStarted() {
        return downloadBytesStarted;
    }

    public void addDownloadBytesStarted(long downloadsStarted) {
        this.downloadBytesStarted += downloadsStarted;
    }

    public long getDownloadBytesFailed() {
        return downloadBytesFailed;
    }

    public void addDownloadBytesFailed(long downloadsFailed) {
        this.downloadBytesFailed += downloadsFailed;
    }

    public long getDownloadBytesSucceeded() {
        return downloadBytesSucceeded;
    }

    public void addDownloadBytesSucceeded(long downloadsSucceeded) {
        this.downloadBytesSucceeded += downloadsSucceeded;
    }

    public long getMaxRefreshTimeLag() {
        return maxRefreshTimeLag;
    }

    public void setMaxRefreshTimeLag(long maxRefreshTimeLag) {
        this.maxRefreshTimeLag = Math.max(this.maxRefreshTimeLag, maxRefreshTimeLag);
    }

    public long getMaxRefreshBytesLag() {
        return maxRefreshBytesLag;
    }

    public void addMaxRefreshBytesLag(long maxRefreshBytesLag) {
        this.maxRefreshBytesLag = Math.max(this.maxRefreshBytesLag, maxRefreshBytesLag);
    }

    public long getTotalRefreshBytesLag() {
        return totalRefreshBytesLag;
    }

    public void addTotalRefreshBytesLag(long totalRefreshBytesLag) {
        this.totalRefreshBytesLag += totalRefreshBytesLag;
    }

    public long getTotalUploadTime() {
        return totalUploadTime;
    }

    public void addTotalUploadTime(long totalUploadTime) {
        this.totalUploadTime += totalUploadTime;
    }

    public long getTotalDownloadTime() {
        return totalDownloadTime;
    }

    public void addTotalDownloadTime(long totalDownloadTime) {
        this.totalDownloadTime += totalDownloadTime;
    }

    public long getTotalRejections() {
        return totalRejections;
    }

    public void addTotalRejections(long totalRejections) {
        this.totalRejections += totalRejections;
    }

    /**
     * Adds existing stats. Used for stats roll-ups at index or node level
     *
     * @param existingStats: Existing {@link RemoteSegmentStats} to add
     */
    public void add(RemoteSegmentStats existingStats) {
        if (existingStats != null) {
            this.uploadBytesStarted += existingStats.getUploadBytesStarted();
            this.uploadBytesSucceeded += existingStats.getUploadBytesSucceeded();
            this.uploadBytesFailed += existingStats.getUploadBytesFailed();
            this.downloadBytesStarted += existingStats.getDownloadBytesStarted();
            this.downloadBytesFailed += existingStats.getDownloadBytesFailed();
            this.downloadBytesSucceeded += existingStats.getDownloadBytesSucceeded();
            this.maxRefreshTimeLag = Math.max(this.maxRefreshTimeLag, existingStats.getMaxRefreshTimeLag());
            this.maxRefreshBytesLag = Math.max(this.maxRefreshBytesLag, existingStats.getMaxRefreshBytesLag());
            this.totalRefreshBytesLag += existingStats.getTotalRefreshBytesLag();
            this.totalUploadTime += existingStats.getTotalUploadTime();
            this.totalDownloadTime += existingStats.getTotalDownloadTime();
            this.totalRejections += existingStats.totalRejections;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(uploadBytesStarted);
        out.writeLong(uploadBytesFailed);
        out.writeLong(uploadBytesSucceeded);
        out.writeLong(downloadBytesStarted);
        out.writeLong(downloadBytesFailed);
        out.writeLong(downloadBytesSucceeded);
        out.writeLong(maxRefreshTimeLag);
        out.writeLong(maxRefreshBytesLag);
        out.writeLong(totalRefreshBytesLag);
        out.writeLong(totalUploadTime);
        out.writeLong(totalDownloadTime);
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeVLong(totalRejections);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REMOTE_STORE);

        builder.startObject(Fields.UPLOAD);
        buildUploadStats(builder);
        builder.endObject(); // UPLOAD

        builder.startObject(Fields.DOWNLOAD);
        buildDownloadStats(builder);
        builder.endObject(); // DOWNLOAD

        builder.endObject(); // REMOTE_STORE

        return builder;
    }

    private void buildUploadStats(XContentBuilder builder) throws IOException {
        builder.startObject(Fields.TOTAL_UPLOAD_SIZE);
        builder.humanReadableField(Fields.STARTED_BYTES, Fields.STARTED, new ByteSizeValue(uploadBytesStarted));
        builder.humanReadableField(Fields.SUCCEEDED_BYTES, Fields.SUCCEEDED, new ByteSizeValue(uploadBytesSucceeded));
        builder.humanReadableField(Fields.FAILED_BYTES, Fields.FAILED, new ByteSizeValue(uploadBytesFailed));
        builder.endObject(); // TOTAL_UPLOAD_SIZE

        builder.startObject(Fields.REFRESH_SIZE_LAG);
        builder.humanReadableField(Fields.TOTAL_BYTES, Fields.TOTAL, new ByteSizeValue(totalRefreshBytesLag));
        builder.humanReadableField(Fields.MAX_BYTES, Fields.MAX, new ByteSizeValue(maxRefreshBytesLag));
        builder.endObject(); // REFRESH_SIZE_LAG

        builder.humanReadableField(Fields.MAX_REFRESH_TIME_LAG_IN_MILLIS, Fields.MAX_REFRESH_TIME_LAG, new TimeValue(maxRefreshTimeLag));
        builder.humanReadableField(Fields.TOTAL_TIME_SPENT_IN_MILLIS, Fields.TOTAL_TIME_SPENT, new TimeValue(totalUploadTime));

        builder.startObject(Fields.PRESSURE);
        builder.field(Fields.TOTAL_REJECTIONS, totalRejections);
        builder.endObject(); // PRESSURE
    }

    private void buildDownloadStats(XContentBuilder builder) throws IOException {
        builder.startObject(Fields.TOTAL_DOWNLOAD_SIZE);
        builder.humanReadableField(Fields.STARTED_BYTES, Fields.STARTED, new ByteSizeValue(downloadBytesStarted));
        builder.humanReadableField(Fields.SUCCEEDED_BYTES, Fields.SUCCEEDED, new ByteSizeValue(downloadBytesSucceeded));
        builder.humanReadableField(Fields.FAILED_BYTES, Fields.FAILED, new ByteSizeValue(downloadBytesFailed));
        builder.endObject();
        builder.humanReadableField(Fields.TOTAL_TIME_SPENT_IN_MILLIS, Fields.TOTAL_TIME_SPENT, new TimeValue(totalDownloadTime));
    }

    static final class Fields {
        static final String REMOTE_STORE = "remote_store";
        static final String UPLOAD = "upload";
        static final String DOWNLOAD = "download";
        static final String TOTAL_UPLOAD_SIZE = "total_upload_size";
        static final String TOTAL_DOWNLOAD_SIZE = "total_download_size";
        static final String MAX_REFRESH_TIME_LAG = "max_refresh_time_lag";
        static final String MAX_REFRESH_TIME_LAG_IN_MILLIS = "max_refresh_time_lag_in_millis";
        static final String REFRESH_SIZE_LAG = "refresh_size_lag";
        static final String STARTED = "started";
        static final String STARTED_BYTES = "started_bytes";
        static final String FAILED = "failed";
        static final String FAILED_BYTES = "failed_bytes";
        static final String SUCCEEDED = "succeeded";
        static final String SUCCEEDED_BYTES = "succeeded_bytes";
        static final String TOTAL = "total";
        static final String TOTAL_BYTES = "total_bytes";
        static final String MAX = "max";
        static final String MAX_BYTES = "max_bytes";
        static final String TOTAL_TIME_SPENT = "total_time_spent";
        static final String TOTAL_TIME_SPENT_IN_MILLIS = "total_time_spent_in_millis";
        static final String PRESSURE = "pressure";
        static final String TOTAL_REJECTIONS = "total_rejections";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteSegmentStats that = (RemoteSegmentStats) o;
        return uploadBytesStarted == that.uploadBytesStarted
            && uploadBytesFailed == that.uploadBytesFailed
            && uploadBytesSucceeded == that.uploadBytesSucceeded
            && downloadBytesStarted == that.downloadBytesStarted
            && downloadBytesFailed == that.downloadBytesFailed
            && downloadBytesSucceeded == that.downloadBytesSucceeded
            && maxRefreshTimeLag == that.maxRefreshTimeLag
            && maxRefreshBytesLag == that.maxRefreshBytesLag
            && totalRefreshBytesLag == that.totalRefreshBytesLag
            && totalUploadTime == that.totalUploadTime
            && totalDownloadTime == that.totalDownloadTime
            && totalRejections == that.totalRejections;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            uploadBytesStarted,
            uploadBytesFailed,
            uploadBytesSucceeded,
            downloadBytesStarted,
            downloadBytesFailed,
            downloadBytesSucceeded,
            maxRefreshTimeLag,
            maxRefreshBytesLag,
            totalRefreshBytesLag,
            totalUploadTime,
            totalDownloadTime,
            totalRejections
        );
    }
}
