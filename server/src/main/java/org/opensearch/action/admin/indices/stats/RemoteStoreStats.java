/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.RemoteSegmentUploadShardStatsTracker;

import java.io.IOException;

public class RemoteStoreStats implements Writeable, ToXContentFragment {

    private RemoteSegmentUploadShardStatsTracker remoteSegmentUploadShardStatsTracker;

    public RemoteStoreStats(RemoteSegmentUploadShardStatsTracker remoteSegmentUploadShardStatsTracker) {
        this.remoteSegmentUploadShardStatsTracker = remoteSegmentUploadShardStatsTracker;
    }

    public RemoteStoreStats(StreamInput in) {
        try {
            String a = in.readString();
            a.equals("random string");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RemoteSegmentUploadShardStatsTracker getStats() {
        return remoteSegmentUploadShardStatsTracker;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ShardStats.Fields.ROUTING)
            .field("totalUploadBytes", remoteSegmentUploadShardStatsTracker.getUploadBytesSucceeded())
            .field(ShardStats.Fields.PRIMARY, 10)
            .field(ShardStats.Fields.NODE, 100)
            .field(ShardStats.Fields.RELOCATING_NODE, 1000)
            .endObject();

        return builder;

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString("random string");
    }
}
