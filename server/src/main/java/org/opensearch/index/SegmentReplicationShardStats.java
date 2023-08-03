/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.replication.SegmentReplicationState;

import java.io.IOException;

/**
 * SegRep stats for a single shard.
 *
 * @opensearch.internal
 */
public class SegmentReplicationShardStats implements Writeable, ToXContentFragment {
    private final String allocationId;
    private final long checkpointsBehindCount;
    private final long bytesBehindCount;
    private final long currentReplicationTimeMillis;
    private final long lastCompletedReplicationTimeMillis;

    @Nullable
    private SegmentReplicationState currentReplicationState;

    public SegmentReplicationShardStats(
        String allocationId,
        long checkpointsBehindCount,
        long bytesBehindCount,
        long currentReplicationTimeMillis,
        long lastCompletedReplicationTime
    ) {
        this.allocationId = allocationId;
        this.checkpointsBehindCount = checkpointsBehindCount;
        this.bytesBehindCount = bytesBehindCount;
        this.currentReplicationTimeMillis = currentReplicationTimeMillis;
        this.lastCompletedReplicationTimeMillis = lastCompletedReplicationTime;
    }

    public SegmentReplicationShardStats(StreamInput in) throws IOException {
        this.allocationId = in.readString();
        this.checkpointsBehindCount = in.readVLong();
        this.bytesBehindCount = in.readVLong();
        this.currentReplicationTimeMillis = in.readVLong();
        this.lastCompletedReplicationTimeMillis = in.readVLong();
    }

    public String getAllocationId() {
        return allocationId;
    }

    public long getCheckpointsBehindCount() {
        return checkpointsBehindCount;
    }

    public long getBytesBehindCount() {
        return bytesBehindCount;
    }

    public long getCurrentReplicationTimeMillis() {
        return currentReplicationTimeMillis;
    }

    public long getLastCompletedReplicationTimeMillis() {
        return lastCompletedReplicationTimeMillis;
    }

    public void setCurrentReplicationState(SegmentReplicationState currentReplicationState) {
        this.currentReplicationState = currentReplicationState;
    }

    @Nullable
    public SegmentReplicationState getCurrentReplicationState() {
        return currentReplicationState;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("allocation_id", allocationId);
        builder.field("checkpoints_behind", checkpointsBehindCount);
        builder.field("bytes_behind", new ByteSizeValue(bytesBehindCount).toString());
        builder.field("current_replication_time", new TimeValue(currentReplicationTimeMillis));
        builder.field("last_completed_replication_time", new TimeValue(lastCompletedReplicationTimeMillis));
        if (currentReplicationState != null) {
            builder.startObject();
            currentReplicationState.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(allocationId);
        out.writeVLong(checkpointsBehindCount);
        out.writeVLong(bytesBehindCount);
        out.writeVLong(currentReplicationTimeMillis);
        out.writeVLong(lastCompletedReplicationTimeMillis);
    }

    @Override
    public String toString() {
        return "SegmentReplicationShardStats{"
            + "allocationId="
            + allocationId
            + ", checkpointsBehindCount="
            + checkpointsBehindCount
            + ", bytesBehindCount="
            + bytesBehindCount
            + ", currentReplicationTimeMillis="
            + currentReplicationTimeMillis
            + ", lastCompletedReplicationTimeMillis="
            + lastCompletedReplicationTimeMillis
            + ", currentReplicationState="
            + currentReplicationState
            + '}';
    }
}
