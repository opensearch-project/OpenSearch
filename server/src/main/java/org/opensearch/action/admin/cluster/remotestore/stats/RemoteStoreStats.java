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
            .field("latest_upload_files_count", remoteSegmentUploadShardStats.latestUploadFilesCount)
            .field("latest_local_files_count", remoteSegmentUploadShardStats.latestLocalFilesCount)
            .field("local_refresh_time", remoteSegmentUploadShardStats.localRefreshTime)
            .field("local_refresh_seq_no", remoteSegmentUploadShardStats.localRefreshSeqNo)
            .field("remote_refresh_time", remoteSegmentUploadShardStats.remoteRefreshTime)
            .field("remote_refresh_seqno", remoteSegmentUploadShardStats.remoteRefreshSeqNo)
            .field("bytes_lag", remoteSegmentUploadShardStats.bytesLag)
            .field("inflight_upload_bytes", remoteSegmentUploadShardStats.inflightUploadBytes)
            .field("inflight_uploads", remoteSegmentUploadShardStats.inflightUploads)
            .field("rejection_count", remoteSegmentUploadShardStats.rejectionCount)
            .field("consecutive_failure_count", remoteSegmentUploadShardStats.consecutiveFailuresCount);
        builder.startObject("last_upload_time").field("started", -1).field("succeeded", -1).field("failed", -1);
        builder.endObject();
        builder.startObject("upload_bytes");
        builder.field("started", remoteSegmentUploadShardStats.uploadBytesStarted)
            .field("succeeded", remoteSegmentUploadShardStats.uploadBytesSucceeded)
            .field("failed", remoteSegmentUploadShardStats.uploadBytesFailed);
        builder.startObject("moving_avg");
        builder.field("started", remoteSegmentUploadShardStats.uploadBytesMovingAverage)
            .field("succeeded", remoteSegmentUploadShardStats.uploadBytesMovingAverage)
            .field("failed", -1);
        builder.endObject();
        builder.startObject("per_sec_moving_avg");
        builder.field("started", remoteSegmentUploadShardStats.uploadBytesPerSecMovingAverage)
            .field("succeeded", remoteSegmentUploadShardStats.uploadBytesPerSecMovingAverage)
            .field("failed", -1);
        builder.endObject();
        builder.endObject();

        builder.startObject("total_uploads");
        builder.field("started", remoteSegmentUploadShardStats.totalUploadsStarted)
            .field("succeeded", remoteSegmentUploadShardStats.totalUploadsSucceeded)
            .field("failed", remoteSegmentUploadShardStats.totalUploadsFailed);
        builder.endObject();

        builder.startObject("total_deletes");
        builder.field("started", -1).field("succeeded", -1).field("failed", -1);
        builder.endObject();

        builder.startObject("upload_latency");
        builder.field("avg", -1)
            .field("moving_avg", remoteSegmentUploadShardStats.uploadTimeMovingAverage)
            .field("max", -1)
            .field("min", -1)
            .field("p90", -1);
        builder.endObject();

        builder.startObject("delete_latency");
        builder.field("avg", -1).field("moving_avg", -1).field("max", -1).field("min", -1).field("p90", -1);
        builder.endObject();

        builder.startObject("latest_segments_filesize");
        builder.field("avg", remoteSegmentUploadShardStats.latestLocalFileSizeAvg)
            .field("max", remoteSegmentUploadShardStats.latestLocalFileSizeMax)
            .field("min", remoteSegmentUploadShardStats.latestLocalFileSizeMin)
            .field("p90", -1);
        builder.endObject();

        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentUploadShardStats);
    }
}
