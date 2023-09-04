/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
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

    private final ShardReplicationStats primaryStats;
    private final ShardReplicationStats replicaStats;

    public ReplicationStats(StreamInput in) throws IOException {
        this.primaryStats = new ShardReplicationStats(in);
        this.replicaStats = new ShardReplicationStats(in);
    }

    public ReplicationStats() {
        primaryStats = new ShardReplicationStats();
        replicaStats = new ShardReplicationStats();
    }

    public void add(ReplicationStats other) {
        if (other != null) {
            primaryStats.add(other.primaryStats);
            replicaStats.add(other.replicaStats);
        }
    }

    public ShardReplicationStats getPrimaryStats() {
        return primaryStats;
    }

    public ShardReplicationStats getReplicaStats() {
        return primaryStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        primaryStats.writeTo(out);
        replicaStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEGMENT_REPLICATION);
        builder.startObject(Fields.PRIMARY_STATS);
        builder.field(Fields.MAX_BYTES_AHEAD, new ByteSizeValue(primaryStats.maxBytes).toString());
        builder.field(Fields.TOTAL_BYTES_AHEAD, new ByteSizeValue(primaryStats.totalBytes).toString());
        builder.field(Fields.MAX_REPLICATION_LAG, new TimeValue(primaryStats.maxReplicationLag));
        builder.endObject();
        builder.startObject(Fields.REPLICA_STATS);
        builder.field(Fields.MAX_BYTES_BEHIND, new ByteSizeValue(replicaStats.maxBytes).toString());
        builder.field(Fields.TOTAL_BYTES_BEHIND, new ByteSizeValue(replicaStats.totalBytes).toString());
        builder.field(Fields.MAX_REPLICATION_LAG, new TimeValue(replicaStats.maxReplicationLag));
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public void addReplicaStats(ShardReplicationStats other) {
        if (other != null) {
            replicaStats.add(other);
        }
    }

    public void addPrimaryStats(ShardReplicationStats other) {
        if (other != null) {
            primaryStats.add(other);
        }
    }

    /**
     * Fields for segment replication statistics
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SEGMENT_REPLICATION = "segment_replication";
        static final String REPLICA_STATS = "replica_stats";
        static final String PRIMARY_STATS = "primary_stats";
        static final String MAX_BYTES_BEHIND = "max_bytes_behind";
        static final String TOTAL_BYTES_BEHIND = "total_bytes_behind";
        static final String MAX_REPLICATION_LAG = "max_replication_lag";
        static final String MAX_BYTES_AHEAD = "max_bytes_ahead";
        static final String TOTAL_BYTES_AHEAD = "total_bytes_ahead";
    }

    /**
     * Replication stats for a shard. This class is reused by primary and replicas
     */
    public static class ShardReplicationStats implements Writeable {
        public long maxBytes;
        public long totalBytes;
        public long maxReplicationLag;

        public ShardReplicationStats() {}

        public ShardReplicationStats(long bytesBehind, long replicationLag) {
            this(bytesBehind, bytesBehind, replicationLag);
        }

        public ShardReplicationStats(long maxBytes, long totalBytes, long maxReplicationLag) {
            this.maxBytes = maxBytes;
            this.totalBytes = totalBytes;
            this.maxReplicationLag = maxReplicationLag;
        }

        public ShardReplicationStats(StreamInput in) throws IOException {
            this.maxBytes = in.readVLong();
            this.totalBytes = in.readVLong();
            this.maxReplicationLag = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(maxBytes);
            out.writeVLong(totalBytes);
            out.writeVLong(maxReplicationLag);
        }

        public long getMaxBytes() {
            return this.maxBytes;
        }

        public long getTotalBytes() {
            return this.totalBytes;
        }

        public long getMaxReplicationLag() {
            return this.maxReplicationLag;
        }

        public void add(ShardReplicationStats other) {
            if (other != null) {
                maxBytes = Math.max(other.maxBytes, maxBytes);
                totalBytes += other.totalBytes;
                maxReplicationLag = Math.max(other.maxReplicationLag, maxReplicationLag);
            }
        }
    }
}
