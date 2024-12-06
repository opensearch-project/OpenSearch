/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.replication;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.indices.replication.SegmentReplicationState;

import java.io.IOException;

/**
 * Segment Replication specific response object for fetching stats from either a primary
 * or replica shard. The stats returned are different depending on primary or replica.
 *
 * @opensearch.internal
 */
public class SegmentReplicationShardStatsResponse implements Writeable {

    @Nullable
    private final SegmentReplicationPerGroupStats primaryStats;

    @Nullable
    private final SegmentReplicationState replicaStats;

    @Nullable
    private final SegmentReplicationShardStats searchReplicaReplicationStats;

    public SegmentReplicationShardStatsResponse(StreamInput in) throws IOException {
        this.primaryStats = in.readOptionalWriteable(SegmentReplicationPerGroupStats::new);
        this.replicaStats = in.readOptionalWriteable(SegmentReplicationState::new);
        this.searchReplicaReplicationStats = in.readOptionalWriteable(SegmentReplicationShardStats::new);
    }

    public SegmentReplicationShardStatsResponse(SegmentReplicationPerGroupStats primaryStats) {
        this.primaryStats = primaryStats;
        this.replicaStats = null;
        this.searchReplicaReplicationStats = null;
    }

    public SegmentReplicationShardStatsResponse(SegmentReplicationState replicaStats) {
        this.replicaStats = replicaStats;
        this.primaryStats = null;
        this.searchReplicaReplicationStats = null;
    }

    public SegmentReplicationShardStatsResponse(SegmentReplicationShardStats segmentReplicationShardStats) {
        this.primaryStats = null;
        this.replicaStats = null;
        this.searchReplicaReplicationStats = segmentReplicationShardStats;
    }

    public SegmentReplicationPerGroupStats getPrimaryStats() {
        return primaryStats;
    }

    public SegmentReplicationState getReplicaStats() {
        return replicaStats;
    }

    public SegmentReplicationShardStats getSearchReplicaReplicationStats() {
        return searchReplicaReplicationStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(primaryStats);
        out.writeOptionalWriteable(replicaStats);
        out.writeOptionalWriteable(searchReplicaReplicationStats);
    }

    @Override
    public String toString() {
        return "SegmentReplicationShardStatsResponse{" +
            "primaryStats=" + primaryStats +
            ", replicaStats=" + replicaStats +
            ", searchReplicaReplicationStats=" + searchReplicaReplicationStats +
            '}';
    }
}
