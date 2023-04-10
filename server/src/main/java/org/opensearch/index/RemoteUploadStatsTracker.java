/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.ShardId;

import java.util.Map;

/**
 * Tracker responsible for computing Remote Upload Stats.
 *
 * @opensearch.internal
 */
public class RemoteUploadStatsTracker {

    private final Map<ShardId, RemoteSegmentUploadShardStatsTracker> shardLevelStats;

    RemoteUploadStatsTracker() {
        this.shardLevelStats = ConcurrentCollections.newConcurrentMap();
    }

    RemoteSegmentUploadShardStatsTracker getStatsTracker(ShardId shardId) {
        return shardLevelStats.computeIfAbsent(shardId, k -> new RemoteSegmentUploadShardStatsTracker());
    }

    void remove(ShardId shardId) {
        shardLevelStats.remove(shardId);
    }
}
