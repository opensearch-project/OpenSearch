/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.remote.RemoteRefreshSegmentTracker;

import java.io.IOException;

/**
 * Encapsulates all remote store stats
 *
 * @opensearch.internal
 */
public class RemoteStoreStats implements Writeable, ToXContentFragment {

    private final RemoteRefreshSegmentTracker.Stats remoteSegmentShardStats;

    private final ShardRouting currentRouting;

    public RemoteStoreStats(RemoteRefreshSegmentTracker.Stats remoteSegmentUploadShardStats, ShardRouting currentRouting) {
        this.remoteSegmentShardStats = remoteSegmentUploadShardStats;
        this.currentRouting = currentRouting;
    }

    public RemoteStoreStats(StreamInput in) throws IOException {
        this.remoteSegmentShardStats = in.readOptionalWriteable(RemoteRefreshSegmentTracker.Stats::new);
        this.currentRouting = new ShardRouting(in);
    }

    public RemoteRefreshSegmentTracker.Stats getStats() {
        return remoteSegmentShardStats;
    }

    public ShardRouting getShardRouting() {
        return currentRouting;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(Fields.ROUTING);
        builder.field(RoutingFields.STATE, currentRouting.state());
        builder.field(RoutingFields.PRIMARY, currentRouting.primary());
        builder.field(RoutingFields.NODE_ID, currentRouting.currentNodeId());
        builder.endObject();
        builder.startObject(Fields.SEGMENT);
        if (!currentRouting.primary()) {
            builder.startObject(SubFields.DOWNLOAD);
            builder.field(DownloadStatsFields.LAST_DOWNLOAD_TIMESTAMP, remoteSegmentShardStats.lastDownloadTimestampMs);
            builder.startObject(DownloadStatsFields.TOTAL_FILE_DOWNLOADS)
                .field(SubFields.STARTED, remoteSegmentShardStats.totalDownloadsStarted)
                .field(SubFields.SUCCEEDED, remoteSegmentShardStats.totalDownloadsSucceeded)
                .field(SubFields.FAILED, remoteSegmentShardStats.totalDownloadsFailed);
            builder.endObject();
            builder.startObject(DownloadStatsFields.TOTAL_FILE_DOWNLOADS_IN_BYTES)
                .field(SubFields.STARTED, remoteSegmentShardStats.downloadBytesStarted)
                .field(SubFields.SUCCEEDED, remoteSegmentShardStats.downloadBytesSucceeded)
                .field(SubFields.FAILED, remoteSegmentShardStats.downloadBytesFailed);
            builder.endObject();
            builder.startObject(DownloadStatsFields.DOWNLOAD_SIZE_IN_BYTES)
                .field(SubFields.LAST_SUCCESSFUL, remoteSegmentShardStats.lastSuccessfulSegmentDownloadBytes)
                .field(SubFields.MOVING_AVG, remoteSegmentShardStats.downloadBytesMovingAverage);
            builder.endObject();
            builder.startObject(DownloadStatsFields.DOWNLOAD_SPEED_IN_BYTES_PER_SEC)
                .field(SubFields.MOVING_AVG, remoteSegmentShardStats.downloadBytesPerSecMovingAverage);
            builder.endObject();
            builder.startObject(DownloadStatsFields.DOWNLOAD_LATENCY_IN_MILLIS)
                .field(SubFields.MOVING_AVG, remoteSegmentShardStats.downloadTimeMovingAverage);
            builder.endObject();
            builder.endObject();
        } else {
            builder.startObject(SubFields.DOWNLOAD);
            builder.endObject();
        }
        if (currentRouting.primary()) {
            builder.startObject(SubFields.UPLOAD)
                .field(UploadStatsFields.LOCAL_REFRESH_TIMESTAMP, remoteSegmentShardStats.localRefreshClockTimeMs)
                .field(UploadStatsFields.REMOTE_REFRESH_TIMESTAMP, remoteSegmentShardStats.remoteRefreshClockTimeMs)
                .field(UploadStatsFields.REFRESH_TIME_LAG_IN_MILLIS, remoteSegmentShardStats.refreshTimeLagMs)
                .field(
                    UploadStatsFields.REFRESH_LAG,
                    remoteSegmentShardStats.localRefreshNumber - remoteSegmentShardStats.remoteRefreshNumber
                )
                .field(UploadStatsFields.BYTES_LAG, remoteSegmentShardStats.bytesLag)
                .field(UploadStatsFields.BACKPRESSURE_REJECTION_COUNT, remoteSegmentShardStats.rejectionCount)
                .field(UploadStatsFields.CONSECUTIVE_FAILURE_COUNT, remoteSegmentShardStats.consecutiveFailuresCount);
            builder.startObject(UploadStatsFields.TOTAL_REMOTE_REFRESH)
                .field(SubFields.STARTED, remoteSegmentShardStats.totalUploadsStarted)
                .field(SubFields.SUCCEEDED, remoteSegmentShardStats.totalUploadsSucceeded)
                .field(SubFields.FAILED, remoteSegmentShardStats.totalUploadsFailed);
            builder.endObject();
            builder.startObject(UploadStatsFields.TOTAL_UPLOADS_IN_BYTES)
                .field(SubFields.STARTED, remoteSegmentShardStats.uploadBytesStarted)
                .field(SubFields.SUCCEEDED, remoteSegmentShardStats.uploadBytesSucceeded)
                .field(SubFields.FAILED, remoteSegmentShardStats.uploadBytesFailed);
            builder.endObject();
            builder.startObject(UploadStatsFields.REMOTE_REFRESH_SIZE_IN_BYTES)
                .field(SubFields.LAST_SUCCESSFUL, remoteSegmentShardStats.lastSuccessfulRemoteRefreshBytes)
                .field(SubFields.MOVING_AVG, remoteSegmentShardStats.uploadBytesMovingAverage);
            builder.endObject();
            builder.startObject(UploadStatsFields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)
                .field(SubFields.MOVING_AVG, remoteSegmentShardStats.uploadBytesPerSecMovingAverage);
            builder.endObject();
            builder.startObject(UploadStatsFields.REMOTE_REFRESH_LATENCY_IN_MILLIS)
                .field(SubFields.MOVING_AVG, remoteSegmentShardStats.uploadTimeMovingAverage);
            builder.endObject();
            builder.endObject();
        } else {
            builder.startObject(SubFields.UPLOAD);
            builder.endObject();
        }
        builder.endObject();
        return builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, false);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentShardStats);
        currentRouting.writeTo(out);
    }

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
    static final class UploadStatsFields {
        static final String SHARD_ID = "shard_id";

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
         * Represents the number of remote refreshes
         */
        static final String TOTAL_REMOTE_REFRESH = "total_remote_refresh";

        /**
         * Represents the total uploads to remote store in bytes
         */
        static final String TOTAL_UPLOADS_IN_BYTES = "total_uploads_in_bytes";

        /**
         * Represents the size of new data to be uploaded as part of a refresh
         */
        static final String REMOTE_REFRESH_SIZE_IN_BYTES = "remote_refresh_size_in_bytes";

        /**
         * Represents the speed of remote store uploads in bytes per sec
         */
        static final String UPLOAD_LATENCY_IN_BYTES_PER_SEC = "upload_latency_in_bytes_per_sec";

        /**
         * Time taken by a single remote refresh
         */
        static final String REMOTE_REFRESH_LATENCY_IN_MILLIS = "remote_refresh_latency_in_millis";
    }

    static final class DownloadStatsFields {
        static final String LAST_DOWNLOAD_TIMESTAMP = "last_download_timestamp";
        static final String TOTAL_FILE_DOWNLOADS = "total_file_downloads";
        static final String TOTAL_FILE_DOWNLOADS_IN_BYTES = "total_file_downloads_in_bytes";
        static final String DOWNLOAD_SIZE_IN_BYTES = "download_size_in_bytes";
        static final String DOWNLOAD_SPEED_IN_BYTES_PER_SEC = "download_speed_in_bytes_per_sec";
        static final String DOWNLOAD_LATENCY_IN_MILLIS = "download_latency_in_millis";
    }

    /**
     * Reusable sub fields for {@link Fields}
     */
    static final class SubFields {
        static final String STARTED = "started";
        static final String SUCCEEDED = "succeeded";
        static final String FAILED = "failed";

        static final String DOWNLOAD = "download";
        static final String UPLOAD = "upload";

        /**
         * Moving avg over last N values stat for a {@link Fields}
         */
        static final String MOVING_AVG = "moving_avg";

        /**
         * Most recent successful attempt stat for a {@link Fields}
         */
        static final String LAST_SUCCESSFUL = "last_successful";
    }

}
