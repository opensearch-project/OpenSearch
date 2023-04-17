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
import org.opensearch.index.RemoteSegmentUploadShardStatsTracker;

import java.io.IOException;

/**
 * Do we need this stats wrapper
 */
public class RemoteStoreStats implements Writeable, ToXContentFragment {

    private RemoteSegmentUploadShardStatsTracker remoteSegmentUploadShardStatsTracker;

    public RemoteStoreStats(RemoteSegmentUploadShardStatsTracker remoteSegmentUploadShardStatsTracker) {
        this.remoteSegmentUploadShardStatsTracker = remoteSegmentUploadShardStatsTracker;
    }

    public RemoteStoreStats(StreamInput in) {
        try {
            remoteSegmentUploadShardStatsTracker = in.readOptionalWriteable(RemoteSegmentUploadShardStatsTracker::new);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RemoteSegmentUploadShardStatsTracker getStats() {
        return remoteSegmentUploadShardStatsTracker;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("shardId", remoteSegmentUploadShardStatsTracker.getShardId())
            .field("local_refresh_time", remoteSegmentUploadShardStatsTracker.getLocalRefreshTime())
            .field("local_refresh_seq_no", remoteSegmentUploadShardStatsTracker.getLocalRefreshSeqNo())
            .field("remote_refresh_time", remoteSegmentUploadShardStatsTracker.getRemoteRefreshTime())
            .field("remote_refresh_seqno", remoteSegmentUploadShardStatsTracker.getRemoteRefreshSeqNo())
            .field("upload_bytes_started", remoteSegmentUploadShardStatsTracker.getUploadBytesStarted())
            .field("upload_bytes_succeeded", remoteSegmentUploadShardStatsTracker.getUploadBytesSucceeded())
            .field("upload_bytes_failed", remoteSegmentUploadShardStatsTracker.getUploadBytesFailed())
            .field("total_upload_started", remoteSegmentUploadShardStatsTracker.getTotalUploadsStarted())
            .field("total_upload_succeeded", remoteSegmentUploadShardStatsTracker.getTotalUploadsSucceeded())
            .field("total_upload_failed", remoteSegmentUploadShardStatsTracker.getTotalUploadsFailed())
            .field("upload_time_average", remoteSegmentUploadShardStatsTracker.getUploadTimeAverage())
            .field("upload_bytes_per_sec_average", remoteSegmentUploadShardStatsTracker.getUploadBytesPerSecondAverage())
            .field("upload_bytes_average", remoteSegmentUploadShardStatsTracker.getUploadBytesAverage())
            .field("bytes_behind", remoteSegmentUploadShardStatsTracker.getBytesBehind())
            .field("inflight_upload_bytes", remoteSegmentUploadShardStatsTracker.getInflightUploadBytes())
            .field("inflight_uploads", remoteSegmentUploadShardStatsTracker.getInflightUploads())
            .endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(remoteSegmentUploadShardStatsTracker);
    }
}
