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
            .field("shardId", remoteSegmentUploadShardStats.shardId)

            .field("local_refresh_timestamp_in_millis", remoteSegmentUploadShardStats.localRefreshTimeMs)
            .field("local_refresh_cumulative_count", remoteSegmentUploadShardStats.localRefreshCount)
            .field("remote_refresh_timestamp_in_millis", remoteSegmentUploadShardStats.remoteRefreshTimeMs)
            .field("remote_refresh_cumulative_count", remoteSegmentUploadShardStats.remoteRefreshCount)
            .field("bytes_lag", remoteSegmentUploadShardStats.bytesLag)

            .field("rejection_count", remoteSegmentUploadShardStats.rejectionCount)
            .field("consecutive_failure_count", remoteSegmentUploadShardStats.consecutiveFailuresCount)
            .field("failing_since_timestamp_in_millis", -1) // need another PR to be merged before intergration.
            .field("latest_failure_timestamp_in_millis", -1);

        builder.startObject("total_uploads_in_bytes");
        builder.field("started", remoteSegmentUploadShardStats.uploadBytesStarted)
            .field("succeeded", remoteSegmentUploadShardStats.uploadBytesSucceeded)
            .field("failed", remoteSegmentUploadShardStats.uploadBytesFailed);
        builder.endObject();
        builder.startObject("remote_refresh_size_in_bytes");
        builder.field("last_successful", remoteSegmentUploadShardStats.lastSuccessfulRemoteRefreshBytes);
        builder.field("moving_avg", remoteSegmentUploadShardStats.uploadBytesMovingAverage);
        builder.endObject();
        builder.startObject("upload_latency_in_bytes_per_sec");
        builder.field("moving_avg", remoteSegmentUploadShardStats.uploadBytesPerSecMovingAverage);
        builder.endObject();

        builder.startObject("total_remote_refresh");
        builder.field("started", remoteSegmentUploadShardStats.totalUploadsStarted)
            .field("succeeded", remoteSegmentUploadShardStats.totalUploadsSucceeded)
            .field("failed", remoteSegmentUploadShardStats.totalUploadsFailed);
        builder.endObject();

        builder.startObject("remote_refresh_latency_in_nanos");
        builder.field("moving_avg", remoteSegmentUploadShardStats.uploadTimeMovingAverage);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentUploadShardStats);
    }
}
