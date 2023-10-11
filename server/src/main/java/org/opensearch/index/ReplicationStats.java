/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.Version;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * ReplicationStats is used to provide segment replication statistics at an index,
 * node and cluster level on a segment replication enabled cluster.
 *
 * @opensearch.internal
 */
public class ReplicationStats implements ToXContentFragment, Writeable {

    public long maxBytesBehind;
    public long maxReplicationLag;
    public long totalBytesBehind;
    public long totalRejections;
    public ShardId shardId;

    public ReplicationStats(ShardId shardId, long maxBytesBehind, long totalBytesBehind, long maxReplicationLag, long totalRejections) {
        this.shardId = shardId;
        this.maxBytesBehind = maxBytesBehind;
        this.totalBytesBehind = totalBytesBehind;
        this.maxReplicationLag = maxReplicationLag;
        this.totalRejections = totalRejections;
    }

    public ReplicationStats(StreamInput in) throws IOException {
        this.maxBytesBehind = in.readVLong();
        this.totalBytesBehind = in.readVLong();
        this.maxReplicationLag = in.readVLong();
        // TODO: change to V_2_11_0 on main after backport to 2.x
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.totalRejections = in.readVLong();
            this.shardId = in.readOptionalWriteable(ShardId::new);
        }
    }

    public ReplicationStats() {

    }

    public void add(ReplicationStats other) {
        if (other != null) {
            maxBytesBehind = Math.max(other.maxBytesBehind, maxBytesBehind);
            totalBytesBehind += other.totalBytesBehind;
            maxReplicationLag = Math.max(other.maxReplicationLag, maxReplicationLag);
            if (this.shardId != other.shardId) {
                totalRejections += other.totalRejections;
            }
        }
    }

    public long getMaxBytesBehind() {
        return this.maxBytesBehind;
    }

    public long getTotalBytesBehind() {
        return this.totalBytesBehind;
    }

    public long getMaxReplicationLag() {
        return this.maxReplicationLag;
    }

    public long getTotalRejections() {
        return totalRejections;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(maxBytesBehind);
        out.writeVLong(totalBytesBehind);
        out.writeVLong(maxReplicationLag);
        // TODO: change to V_2_11_0 on main after backport to 2.x
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeVLong(totalRejections);
            out.writeOptionalWriteable(shardId);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEGMENT_REPLICATION);
        builder.field(Fields.MAX_BYTES_BEHIND, new ByteSizeValue(maxBytesBehind).toString());
        builder.field(Fields.TOTAL_BYTES_BEHIND, new ByteSizeValue(totalBytesBehind).toString());
        builder.field(Fields.MAX_REPLICATION_LAG, new TimeValue(maxReplicationLag));

        builder.startObject("pressure");
        builder.field("total_rejections", totalRejections);
        builder.endObject();

        builder.endObject();
        return builder;
    }

    /**
     * Fields for segment replication statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SEGMENT_REPLICATION = "segment_replication";
        static final String MAX_BYTES_BEHIND = "max_bytes_behind";
        static final String TOTAL_BYTES_BEHIND = "total_bytes_behind";
        static final String MAX_REPLICATION_LAG = "max_replication_lag";
    }
}
