/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tracks the list of SegRep Replication Checkpoints which are still not processed on the node.
 */
public class SegmentReplicationPendingCheckpoints {
    protected final Map<ShardId, List<Tuple<ReplicationCheckpoint, Long>>> checkpointsTracker = ConcurrentCollections.newConcurrentMap();
    private volatile TimeValue maxAllowedReplicationTime;

    public SegmentReplicationPendingCheckpoints(TimeValue maxAllowedReplicationTime) {
        this.maxAllowedReplicationTime = maxAllowedReplicationTime;
    }

    public void setMaxAllowedReplicationTime(TimeValue maxAllowedReplicationTime) {
        this.maxAllowedReplicationTime = maxAllowedReplicationTime;
    }

    public void addNewReceivedCheckpoint(ShardId shardId, ReplicationCheckpoint checkpoint) {
        checkpointsTracker.putIfAbsent(shardId, new ArrayList<>());
        checkpointsTracker.computeIfPresent(shardId, (k, v) -> {
            v.add(new Tuple<>(checkpoint, System.currentTimeMillis()));
            return v;
        });
    }

    public void updateCheckpointProcessed(ShardId shardId, ReplicationCheckpoint checkpoint) {
        checkpointsTracker.computeIfPresent(
            shardId,
            (k, v) -> v.stream().filter(t -> t.v1().isAheadOf(checkpoint)).collect(Collectors.toCollection(ArrayList::new))
        );
    }

    public ReplicationCheckpoint getLatestReplicationCheckpoint(ShardId shardId) {
        List<Tuple<ReplicationCheckpoint, Long>> checkpoints = checkpointsTracker.getOrDefault(shardId, Collections.emptyList());
        if (checkpoints.size() > 0) {
            return checkpoints.get(checkpoints.size() - 1).v1();
        }
        return null;
    }

    public void remove(ShardId shardId) {
        checkpointsTracker.remove(shardId);
    }

    /**
     * Returns the list of stale shards to fail. A shard is stale if there exists at least one pending checkpoint which
     * has not been processed under the defined time limit(configurable. default 10 min). The time is calculated from
     * the time when the replication event was received by the replica.
     * @return List of stale shards.
     */
    public List<ShardId> getStaleShardsToFail() {
        long currentTime = System.currentTimeMillis();
        return checkpointsTracker.entrySet()
            .stream()
            .map(
                e -> Tuple.tuple(
                    e.getKey(),
                    e.getValue().stream().min(Comparator.comparingLong(Tuple::v2)).map(t -> t.v2()).orElse(currentTime)
                )
            )
            .filter(t -> currentTime - t.v2() > maxAllowedReplicationTime.millis())
            .map(t -> t.v1())
            .sorted(Comparator.comparing(ShardId::getIndexName))
            .collect(Collectors.toList());
    }
}
