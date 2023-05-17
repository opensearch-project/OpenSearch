/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
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

    private final RemoteRefreshSegmentTracker.Stats remoteSegmentUploadShardStats;

    public RemoteStoreStats(RemoteRefreshSegmentTracker.Stats remoteSegmentUploadShardStats) {
        this.remoteSegmentUploadShardStats = remoteSegmentUploadShardStats;
    }

    public RemoteStoreStats(StreamInput in) throws IOException {
        remoteSegmentUploadShardStats = in.readOptionalWriteable(RemoteRefreshSegmentTracker.Stats::new);
    }

    public RemoteRefreshSegmentTracker.Stats getStats() {
        return remoteSegmentUploadShardStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field(Fields.SHARD_ID, remoteSegmentUploadShardStats.shardId)

            .field(Fields.LOCAL_REFRESH_TIMESTAMP, remoteSegmentUploadShardStats.localRefreshTimeMs)
            .field(Fields.LOCAL_REFRESH_CUMULATIVE_COUNT, remoteSegmentUploadShardStats.localRefreshCount)
            .field(Fields.REMOTE_REFRESH_TIMESTAMP, remoteSegmentUploadShardStats.remoteRefreshTimeMs)
            .field(Fields.REMOTE_REFRESH_CUMULATIVE_COUNT, remoteSegmentUploadShardStats.remoteRefreshCount)
            .field(Fields.BYTES_LAG, remoteSegmentUploadShardStats.bytesLag)

            .field(Fields.REJECTION_COUNT, remoteSegmentUploadShardStats.rejectionCount)
            .field(Fields.CONSECUTIVE_FAILURE_COUNT, remoteSegmentUploadShardStats.consecutiveFailuresCount);

        builder.startObject(Fields.TOTAL_REMOTE_REFRESH);
        builder.field(Fields.STARTED, remoteSegmentUploadShardStats.totalUploadsStarted)
            .field(Fields.SUCCEEDED, remoteSegmentUploadShardStats.totalUploadsSucceeded)
            .field(Fields.FAILED, remoteSegmentUploadShardStats.totalUploadsFailed);
        builder.endObject();

        builder.startObject(Fields.TOTAL_UPLOADS_IN_BYTES);
        builder.field(Fields.STARTED, remoteSegmentUploadShardStats.uploadBytesStarted)
            .field(Fields.SUCCEEDED, remoteSegmentUploadShardStats.uploadBytesSucceeded)
            .field(Fields.FAILED, remoteSegmentUploadShardStats.uploadBytesFailed);
        builder.endObject();

        builder.startObject(Fields.REMOTE_REFRESH_SIZE_IN_BYTES);
        builder.field(Fields.LAST_SUCCESSFUL, remoteSegmentUploadShardStats.lastSuccessfulRemoteRefreshBytes);
        builder.field(Fields.MOVING_AVG, remoteSegmentUploadShardStats.uploadBytesMovingAverage);
        builder.endObject();

        builder.startObject(Fields.UPLOAD_LATENCY_IN_BYTES_PER_SEC);
        builder.field(Fields.MOVING_AVG, remoteSegmentUploadShardStats.uploadBytesPerSecMovingAverage);
        builder.endObject();
        builder.startObject(Fields.REMOTE_REFRESH_LATENCY_IN_MILLIS);
        builder.field(Fields.MOVING_AVG, remoteSegmentUploadShardStats.uploadTimeMovingAverage);
        builder.endObject();
        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentUploadShardStats);
    }

    static final class Fields {
        static final String SHARD_ID = "shard_id";
        static final String LOCAL_REFRESH_TIMESTAMP = "local_refresh_timestamp_in_millis";
        static final String LOCAL_REFRESH_CUMULATIVE_COUNT = "local_refresh_cumulative_count";
        static final String REMOTE_REFRESH_TIMESTAMP = "remote_refresh_timestamp_in_millis";
        static final String REMOTE_REFRESH_CUMULATIVE_COUNT = "remote_refresh_cumulative_count";
        static final String BYTES_LAG = "bytes_lag";
        static final String REJECTION_COUNT = "rejection_count";
        static final String CONSECUTIVE_FAILURE_COUNT = "consecutive_failure_count";
        static final String TOTAL_REMOTE_REFRESH = "total_remote_refresh";
        static final String TOTAL_UPLOADS_IN_BYTES = "total_uploads_in_bytes";
        static final String REMOTE_REFRESH_SIZE_IN_BYTES = "remote_refresh_size_in_bytes";
        static final String UPLOAD_LATENCY_IN_BYTES_PER_SEC = "upload_latency_in_bytes_per_sec";
        static final String REMOTE_REFRESH_LATENCY_IN_MILLIS = "remote_refresh_latency_in_millis";
        static final String STARTED = "started";
        static final String SUCCEEDED = "succeeded";
        static final String FAILED = "failed";
        static final String MOVING_AVG = "moving_avg";
        static final String LAST_SUCCESSFUL = "last_successful";
    }

}
