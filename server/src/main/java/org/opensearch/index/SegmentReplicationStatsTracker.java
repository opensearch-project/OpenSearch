/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;

import java.util.HashMap;
import java.util.Map;

/**
 * Tracker responsible for computing SegmentReplicationStats.
 *
 * @opensearch.internal
 */
public class SegmentReplicationStatsTracker {

    private final IndicesService indicesService;

    public SegmentReplicationStatsTracker(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    public SegmentReplicationStats getStats() {
        Map<ShardId, SegmentReplicationPerGroupStats> stats = new HashMap<>();
        for (IndexService indexShards : indicesService) {
            for (IndexShard indexShard : indexShards) {
                if (indexShard.indexSettings().isSegRepEnabled() && indexShard.routingEntry().primary()) {
                    stats.putIfAbsent(
                        indexShard.shardId(),
                        new SegmentReplicationPerGroupStats(
                            // TODO: Store rejected counts.
                            indexShard.getReplicationStats(),
                            0L
                        )
                    );
                }
            }
        }
        return new SegmentReplicationStats(stats);
    }
}
