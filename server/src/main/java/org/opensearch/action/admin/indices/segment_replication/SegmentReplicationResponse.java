/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segment_replication;

import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.indices.replication.SegmentReplicationStatsState;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SegmentReplicationResponse extends BroadcastResponse {
    private final Map<String, List<SegmentReplicationStatsState>> shardSegmentReplicationStatsStates;

    public SegmentReplicationResponse(StreamInput in) throws IOException {
        super(in);
        shardSegmentReplicationStatsStates = in.readMapOfLists(StreamInput::readString, SegmentReplicationStatsState:: new);
    }

    /**
     * Constructs segment replication information for a collection of indices and associated shards. Keeps track of how many total shards
     * were seen, and out of those how many were successfully processed and how many failed.
     *
     * @param totalShards       Total count of shards seen
     * @param successfulShards  Count of shards successfully processed
     * @param failedShards      Count of shards which failed to process
     * @param shardSegmentReplicationStatsStates    Map of indices to shard recovery information
     * @param shardFailures     List of failures processing shards
     */
    public SegmentReplicationResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        Map<String, List<SegmentReplicationStatsState>> shardSegmentReplicationStatsStates,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardSegmentReplicationStatsStates = shardSegmentReplicationStatsStates;
    }

    public boolean hasSegmentReplication() {
        return shardSegmentReplicationStatsStates.size() > 0;
    }

    public Map<String, List<SegmentReplicationStatsState>> shardSegmentReplicationStatsStates() {
        return shardSegmentReplicationStatsStates;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (hasSegmentReplication()) {
            for (String index : shardSegmentReplicationStatsStates.keySet()) {
                List<SegmentReplicationStatsState> segmentReplicationStatsStates = shardSegmentReplicationStatsStates.get(index);
                if (segmentReplicationStatsStates == null || segmentReplicationStatsStates.size() == 0) {
                    continue;
                }
                builder.startObject(index);
                builder.startArray("shards");
                for (SegmentReplicationStatsState segmentReplicationStatsState : segmentReplicationStatsStates) {
                    builder.startObject();
                    segmentReplicationStatsState.toXContent(builder, params);
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMapOfLists(shardSegmentReplicationStatsStates, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
