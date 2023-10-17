/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Segment Replication Stats.
 *
 * @opensearch.internal
 */
public class SegmentReplicationStats implements Writeable, ToXContentFragment {

    private final Map<ShardId, SegmentReplicationPerGroupStats> shardStats;

    /**
     * Total rejections due to segment replication backpressure
     */
    private long totalRejectionCount;

    public SegmentReplicationStats(final Map<ShardId, SegmentReplicationPerGroupStats> shardStats, final long totalRejectionCount) {
        this.shardStats = shardStats;
        this.totalRejectionCount = totalRejectionCount;
    }

    public SegmentReplicationStats(StreamInput in) throws IOException {
        int shardEntries = in.readInt();
        shardStats = new HashMap<>();
        for (int i = 0; i < shardEntries; i++) {
            ShardId shardId = new ShardId(in);
            SegmentReplicationPerGroupStats groupStats = new SegmentReplicationPerGroupStats(in);
            shardStats.put(shardId, groupStats);
        }
        // TODO: change to V_2_12_0 on main after backport to 2.x
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.totalRejectionCount = in.readVLong();
        }
    }

    public Map<ShardId, SegmentReplicationPerGroupStats> getShardStats() {
        return shardStats;
    }

    public long getTotalRejectionCount() {
        return totalRejectionCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("segment_replication_backpressure");
        builder.field("total_rejected_requests", totalRejectionCount);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardStats.size());
        for (Map.Entry<ShardId, SegmentReplicationPerGroupStats> entry : shardStats.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
        // TODO: change to V_2_12_0 on main after backport to 2.x
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeVLong(totalRejectionCount);
        }
    }

    @Override
    public String toString() {
        return "SegmentReplicationStats{" + "shardStats=" + shardStats + ", totalRejectedRequestCount=" + totalRejectionCount + '}';
    }
}
