/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracker responsible for computing SegmentReplicationStats.
 *
 * @opensearch.internal
 */
public class SegmentReplicationStatsTracker {

    private final IndicesService indicesService;
    private final Map<ShardId, AtomicInteger> rejectionCount;

    public SegmentReplicationStatsTracker(IndicesService indicesService) {
        this.indicesService = indicesService;
        rejectionCount = ConcurrentCollections.newConcurrentMap();
    }

    public SegmentReplicationStats getStats() {
        Map<ShardId, SegmentReplicationPerGroupStats> stats = new HashMap<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard indexShard : indexService) {
                if (indexShard.indexSettings().isSegRepEnabled() && indexShard.routingEntry().primary()) {
                    stats.putIfAbsent(indexShard.shardId(), getStatsForShard(indexShard));
                }
            }
        }
        return new SegmentReplicationStats(stats);
    }

    public void incrementRejectionCount(ShardId shardId) {
        rejectionCount.compute(shardId, (k, v) -> {
            if (v == null) {
                return new AtomicInteger(1);
            } else {
                v.incrementAndGet();
                return v;
            }
        });
    }

    public SegmentReplicationPerGroupStats getStatsForShard(IndexShard indexShard) {
        return new SegmentReplicationPerGroupStats(
            indexShard.shardId(),
            indexShard.getReplicationStats(),
            Optional.ofNullable(rejectionCount.get(indexShard.shardId())).map(AtomicInteger::get).orElse(0)
        );
    }
}
