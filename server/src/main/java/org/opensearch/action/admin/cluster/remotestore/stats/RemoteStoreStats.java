/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;

import java.io.IOException;

/**
 * Encapsulates all remote store stats
 *
 * @opensearch.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreStats implements Writeable, ToXContentFragment {
    /**
     * Stats related to Remote Segment Store operations
     */
    private final RemoteSegmentTransferTracker.Stats remoteSegmentShardStats;

    /**
     * Stats related to Remote Translog Store operations
     */
    private final RemoteTranslogTransferTracker.Stats remoteTranslogShardStats;
    private final ShardRouting shardRouting;

    RemoteStoreStats(
        RemoteSegmentTransferTracker.Stats remoteSegmentUploadShardStats,
        RemoteTranslogTransferTracker.Stats remoteTranslogShardStats,
        ShardRouting shardRouting
    ) {
        this.remoteSegmentShardStats = remoteSegmentUploadShardStats;
        this.remoteTranslogShardStats = remoteTranslogShardStats;
        this.shardRouting = shardRouting;
    }

    RemoteStoreStats(StreamInput in) throws IOException {
        remoteSegmentShardStats = in.readOptionalWriteable(RemoteSegmentTransferTracker.Stats::new);
        remoteTranslogShardStats = in.readOptionalWriteable(RemoteTranslogTransferTracker.Stats::new);
        this.shardRouting = new ShardRouting(in);
    }

    public RemoteSegmentTransferTracker.Stats getSegmentStats() {
        return remoteSegmentShardStats;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public RemoteTranslogTransferTracker.Stats getTranslogStats() {
        return remoteTranslogShardStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        buildShardRouting(builder);

        builder.startObject(Fields.SEGMENT);
        builder.startObject(SubFields.DOWNLOAD);
        // Ensuring that we are not showing 0 metrics to the user
        if (remoteSegmentShardStats.directoryFileTransferTrackerStats.transferredBytesStarted != 0) {
            buildSegmentDownloadStats(builder);
        }
        builder.endObject(); // segment.download
        builder.startObject(SubFields.UPLOAD);
        // Ensuring that we are not showing 0 metrics to the user
        if (remoteSegmentShardStats.totalUploadsStarted != 0) {
            buildSegmentUploadStats(builder);
        }
        builder.endObject(); // segment.upload
        builder.endObject(); // segment

        builder.startObject(Fields.TRANSLOG);
        builder.startObject(SubFields.UPLOAD);
        // Ensuring that we are not showing 0 metrics to the user
        if (remoteTranslogShardStats.totalUploadsStarted > 0) {
            buildTranslogUploadStats(builder);
        }
        builder.endObject(); // translog.upload
        builder.startObject(SubFields.DOWNLOAD);
        // Ensuring that we are not showing 0 metrics to the user
        if (remoteTranslogShardStats.totalDownloadsSucceeded > 0) {
            buildTranslogDownloadStats(builder);
        }
        builder.endObject(); // translog.download
        builder.endObject(); // translog

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentShardStats);
        out.writeOptionalWriteable(remoteTranslogShardStats);
        shardRouting.writeTo(out);
    }

    private void buildTranslogUploadStats(XContentBuilder builder) throws IOException {
        builder.field(UploadStatsFields.LAST_SUCCESSFUL_UPLOAD_TIMESTAMP, remoteTranslogShardStats.lastSuccessfulUploadTimestamp);

        builder.startObject(UploadStatsFields.TOTAL_UPLOADS);
        builder.field(SubFields.STARTED, remoteTranslogShardStats.totalUploadsStarted)
            .field(SubFields.FAILED, remoteTranslogShardStats.totalUploadsFailed)
            .field(SubFields.SUCCEEDED, remoteTranslogShardStats.totalUploadsSucceeded);
        builder.endObject();

        builder.startObject(UploadStatsFields.TOTAL_UPLOAD_SIZE);
        builder.field(SubFields.STARTED_BYTES, remoteTranslogShardStats.uploadBytesStarted)
            .field(SubFields.FAILED_BYTES, remoteTranslogShardStats.uploadBytesFailed)
            .field(SubFields.SUCCEEDED_BYTES, remoteTranslogShardStats.uploadBytesSucceeded);
        builder.endObject();

        builder.field(UploadStatsFields.TOTAL_UPLOAD_TIME_IN_MILLIS, remoteTranslogShardStats.totalUploadTimeInMillis);

        builder.startObject(UploadStatsFields.UPLOAD_SIZE_IN_BYTES);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.uploadBytesMovingAverage);
        builder.endObject();

        builder.startObject(UploadStatsFields.UPLOAD_SPEED_IN_BYTES_PER_SEC);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.uploadBytesPerSecMovingAverage);
        builder.endObject();

        builder.startObject(UploadStatsFields.UPLOAD_TIME_IN_MILLIS);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.uploadTimeMovingAverage);
        builder.endObject();
    }

    private void buildTranslogDownloadStats(XContentBuilder builder) throws IOException {
        builder.field(DownloadStatsFields.LAST_SUCCESSFUL_DOWNLOAD_TIMESTAMP, remoteTranslogShardStats.lastSuccessfulDownloadTimestamp);

        builder.startObject(DownloadStatsFields.TOTAL_DOWNLOADS);
        builder.field(SubFields.SUCCEEDED, remoteTranslogShardStats.totalDownloadsSucceeded);
        builder.endObject();

        builder.startObject(DownloadStatsFields.TOTAL_DOWNLOAD_SIZE);
        builder.field(SubFields.SUCCEEDED_BYTES, remoteTranslogShardStats.downloadBytesSucceeded);
        builder.endObject();

        builder.field(DownloadStatsFields.TOTAL_DOWNLOAD_TIME_IN_MILLIS, remoteTranslogShardStats.totalDownloadTimeInMillis);

        builder.startObject(DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.downloadBytesMovingAverage);
        builder.endObject();

        builder.startObject(DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.downloadBytesPerSecMovingAverage);
        builder.endObject();

        builder.startObject(DownloadStatsFields.DOWNLOAD_TIME_IN_MILLIS);
        builder.field(SubFields.MOVING_AVG, remoteTranslogShardStats.downloadTimeMovingAverage);
        builder.endObject();
    }

    private void buildSegmentUploadStats(XContentBuilder builder) throws IOException {
        builder.field(UploadStatsFields.LOCAL_REFRESH_TIMESTAMP, remoteSegmentShardStats.localRefreshClockTimeMs)
            .field(UploadStatsFields.REMOTE_REFRESH_TIMESTAMP, remoteSegmentShardStats.remoteRefreshClockTimeMs)
            .field(UploadStatsFields.REFRESH_TIME_LAG_IN_MILLIS, remoteSegmentShardStats.refreshTimeLagMs)
            .field(UploadStatsFields.REFRESH_LAG, remoteSegmentShardStats.localRefreshNumber - remoteSegmentShardStats.remoteRefreshNumber)
            .field(UploadStatsFields.BYTES_LAG, remoteSegmentShardStats.bytesLag)
            .field(UploadStatsFields.BACKPRESSURE_REJECTION_COUNT, remoteSegmentShardStats.rejectionCount)
            .field(UploadStatsFields.CONSECUTIVE_FAILURE_COUNT, remoteSegmentShardStats.consecutiveFailuresCount);
        builder.startObject(UploadStatsFields.TOTAL_UPLOADS)
            .field(SubFields.STARTED, remoteSegmentShardStats.totalUploadsStarted)
            .field(SubFields.SUCCEEDED, remoteSegmentShardStats.totalUploadsSucceeded)
            .field(SubFields.FAILED, remoteSegmentShardStats.totalUploadsFailed);
        builder.endObject();
        builder.startObject(UploadStatsFields.TOTAL_UPLOAD_SIZE)
            .field(SubFields.STARTED_BYTES, remoteSegmentShardStats.uploadBytesStarted)
            .field(SubFields.SUCCEEDED_BYTES, remoteSegmentShardStats.uploadBytesSucceeded)
            .field(SubFields.FAILED_BYTES, remoteSegmentShardStats.uploadBytesFailed);
        builder.endObject();
        builder.startObject(UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)
            .field(SubFields.LAST_SUCCESSFUL, remoteSegmentShardStats.lastSuccessfulRemoteRefreshBytes)
            .field(SubFields.MOVING_AVG, remoteSegmentShardStats.uploadBytesMovingAverage);
        builder.endObject();
        builder.startObject(UploadStatsFields.UPLOAD_SPEED_IN_BYTES_PER_SEC)
            .field(SubFields.MOVING_AVG, remoteSegmentShardStats.uploadBytesPerSecMovingAverage);
        builder.endObject();
        builder.startObject(UploadStatsFields.REMOTE_REFRESH_LATENCY_IN_MILLIS)
            .field(SubFields.MOVING_AVG, remoteSegmentShardStats.uploadTimeMovingAverage);
        builder.endObject();
    }

    private void buildSegmentDownloadStats(XContentBuilder builder) throws IOException {
        builder.field(
            DownloadStatsFields.LAST_SYNC_TIMESTAMP,
            remoteSegmentShardStats.directoryFileTransferTrackerStats.lastTransferTimestampMs
        );
        builder.startObject(DownloadStatsFields.TOTAL_DOWNLOAD_SIZE)
            .field(SubFields.STARTED_BYTES, remoteSegmentShardStats.directoryFileTransferTrackerStats.transferredBytesStarted)
            .field(SubFields.SUCCEEDED_BYTES, remoteSegmentShardStats.directoryFileTransferTrackerStats.transferredBytesSucceeded)
            .field(SubFields.FAILED_BYTES, remoteSegmentShardStats.directoryFileTransferTrackerStats.transferredBytesFailed);
        builder.endObject();
        builder.startObject(DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)
            .field(SubFields.LAST_SUCCESSFUL, remoteSegmentShardStats.directoryFileTransferTrackerStats.lastSuccessfulTransferInBytes)
            .field(SubFields.MOVING_AVG, remoteSegmentShardStats.directoryFileTransferTrackerStats.transferredBytesMovingAverage);
        builder.endObject();
        builder.startObject(DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC)
            .field(SubFields.MOVING_AVG, remoteSegmentShardStats.directoryFileTransferTrackerStats.transferredBytesPerSecMovingAverage);
        builder.endObject();
    }

    private void buildShardRouting(XContentBuilder builder) throws IOException {
        builder.startObject(Fields.ROUTING);
        builder.field(RoutingFields.STATE, shardRouting.state());
        builder.field(RoutingFields.PRIMARY, shardRouting.primary());
        builder.field(RoutingFields.NODE_ID, shardRouting.currentNodeId());
        builder.endObject();
    }

    /**
     * Fields for remote store stats response
     */
    static final class Fields {
        static final String ROUTING = "routing";
        static final String SEGMENT = "segment";
        static final String TRANSLOG = "translog";
    }

    static final class RoutingFields {
        static final String STATE = "state";
        static final String PRIMARY = "primary";
        static final String NODE_ID = "node";
    }

    /**
     * Fields for remote store stats response
     */
    public static final class UploadStatsFields {
        /**
         * Lag in terms of bytes b/w local and remote store
         */
        static final String BYTES_LAG = "bytes_lag";

        /**
         * No of refresh remote store is lagging behind local
         */
        static final String REFRESH_LAG = "refresh_lag";

        /**
         * Time in millis remote refresh is behind local refresh
         */
        static final String REFRESH_TIME_LAG_IN_MILLIS = "refresh_time_lag_in_millis";

        /**
         * Last successful local refresh timestamp in milliseconds
         */
        static final String LOCAL_REFRESH_TIMESTAMP = "local_refresh_timestamp_in_millis";

        /**
         * Last successful remote refresh timestamp in milliseconds
         */
        static final String REMOTE_REFRESH_TIMESTAMP = "remote_refresh_timestamp_in_millis";

        /**
         * Total write rejections due to remote store backpressure kick in
         */
        static final String BACKPRESSURE_REJECTION_COUNT = "backpressure_rejection_count";

        /**
         * No of consecutive remote refresh failures without a single success since the first failures
         */
        static final String CONSECUTIVE_FAILURE_COUNT = "consecutive_failure_count";

        /**
         * Represents the size of new data to be uploaded as part of a refresh
         */
        static final String REMOTE_REFRESH_SIZE_IN_BYTES = "remote_refresh_size_in_bytes";

        /**
         * Time taken by a single remote refresh
         */
        static final String REMOTE_REFRESH_LATENCY_IN_MILLIS = "remote_refresh_latency_in_millis";

        /**
         * Timestamp of last successful remote store upload
         */
        static final String LAST_SUCCESSFUL_UPLOAD_TIMESTAMP = "last_successful_upload_timestamp";

        /**
         * Count of files uploaded to remote store
         */
        public static final String TOTAL_UPLOADS = "total_uploads";

        /**
         * Represents the total uploads to remote store in bytes
         */
        public static final String TOTAL_UPLOAD_SIZE = "total_upload_size";

        /**
         * Total time spent on remote store uploads
         */
        static final String TOTAL_UPLOAD_TIME_IN_MILLIS = "total_upload_time_in_millis";

        /**
         * Represents the size of new data to be transferred as part of a remote store upload
         */
        static final String UPLOAD_SIZE_IN_BYTES = "upload_size_in_bytes";

        /**
         * Represents the speed of remote store uploads in bytes per sec
         */
        static final String UPLOAD_SPEED_IN_BYTES_PER_SEC = "upload_speed_in_bytes_per_sec";

        /**
         * Time taken by a remote store upload
         */
        static final String UPLOAD_TIME_IN_MILLIS = "upload_time_in_millis";
    }

    static final class DownloadStatsFields {
        /**
         * Epoch timestamp of the last successful download
         */
        public static final String LAST_SUCCESSFUL_DOWNLOAD_TIMESTAMP = "last_successful_download_timestamp";

        /**
         * Last successful sync from remote in milliseconds
         */
        static final String LAST_SYNC_TIMESTAMP = "last_sync_timestamp";

        /**
         * Count of files downloaded from remote store
         */
        public static final String TOTAL_DOWNLOADS = "total_downloads";

        /**
         * Total time spent in downloads from remote store
         */
        public static final String TOTAL_DOWNLOAD_TIME_IN_MILLIS = "total_download_time_in_millis";

        /**
         * Total bytes of files downloaded from the remote store
         */
        static final String TOTAL_DOWNLOAD_SIZE = "total_download_size";

        /**
         * Average size of a file downloaded from the remote store
         */
        static final String DOWNLOAD_SIZE_IN_BYTES = "download_size_in_bytes";

        /**
         * Average speed (in bytes/sec) of a remote store download
         */
        static final String DOWNLOAD_SPEED_IN_BYTES_PER_SEC = "download_speed_in_bytes_per_sec";

        /**
         * Average time spent on a remote store download
         */
        public static final String DOWNLOAD_TIME_IN_MILLIS = "download_time_in_millis";
    }

    /**
     * Reusable sub fields for {@link UploadStatsFields} and {@link DownloadStatsFields}
     */
    public static final class SubFields {
        public static final String STARTED = "started";
        public static final String SUCCEEDED = "succeeded";
        public static final String FAILED = "failed";

        public static final String STARTED_BYTES = "started_bytes";
        public static final String SUCCEEDED_BYTES = "succeeded_bytes";
        public static final String FAILED_BYTES = "failed_bytes";

        static final String DOWNLOAD = "download";
        public static final String UPLOAD = "upload";

        /**
         * Moving avg over last N values stat
         */
        static final String MOVING_AVG = "moving_avg";

        /**
         * Most recent successful attempt stat
         */
        static final String LAST_SUCCESSFUL = "last_successful";
    }

}
