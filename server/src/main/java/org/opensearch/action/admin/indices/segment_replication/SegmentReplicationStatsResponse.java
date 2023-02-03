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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.indices.replication.SegmentReplicationState;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Information regarding the Segment Replication state of indices and their associated shards.
 *
 * @opensearch.internal
 */
public class SegmentReplicationStatsResponse extends BroadcastResponse {
    private final Map<String, List<SegmentReplicationState>> shardSegmentReplicationStates;

    public SegmentReplicationStatsResponse(StreamInput in) throws IOException {
        super(in);
        shardSegmentReplicationStates = in.readMapOfLists(StreamInput::readString, SegmentReplicationState::new);
    }

    /**
     * Constructs segment replication information for a collection of indices and associated shards. Keeps track of how many total shards
     * were seen, and out of those how many were successfully processed and how many failed.
     *
     * @param totalShards       Total count of shards seen
     * @param successfulShards  Count of shards successfully processed
     * @param failedShards      Count of shards which failed to process
     * @param shardSegmentReplicationStates    Map of indices to shard replication information
     * @param shardFailures     List of failures processing shards
     */
    public SegmentReplicationStatsResponse(
        int totalShards,
        int successfulShards,
        int failedShards,
        Map<String, List<SegmentReplicationState>> shardSegmentReplicationStates,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shardSegmentReplicationStates = shardSegmentReplicationStates;
    }

    public boolean hasSegmentReplication() {
        return shardSegmentReplicationStates.size() > 0;
    }

    public Map<String, List<SegmentReplicationState>> shardSegmentReplicationStates() {
        return shardSegmentReplicationStates;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (hasSegmentReplication()) {
            for (String index : shardSegmentReplicationStates.keySet()) {
                List<SegmentReplicationState> segmentReplicationStates = shardSegmentReplicationStates.get(index);
                if (segmentReplicationStates == null || segmentReplicationStates.size() == 0) {
                    continue;
                }
                builder.startObject(index);
                builder.startArray("shards");
                for (SegmentReplicationState segmentReplicationState : segmentReplicationStates) {
                    builder.startObject();
                    segmentReplicationState.toXContent(builder, params);
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
        out.writeMapOfLists(shardSegmentReplicationStates, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public String toString() {
        return Strings.toString(XContentType.JSON, this, true, true);
    }
}
