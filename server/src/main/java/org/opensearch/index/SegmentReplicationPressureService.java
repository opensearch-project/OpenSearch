/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;

/**
 * Service responsible for applying backpressure for lagging behind replicas when Segment Replication is enabled.
 *
 * @opensearch.internal
 */
public class SegmentReplicationPressureService {

    private final IndicesService indicesService;
    private final SegmentReplicationStatsTracker tracker;

    public SegmentReplicationPressureService(IndicesService indexService) {
        this.indicesService = indexService;
        this.tracker = new SegmentReplicationStatsTracker(indicesService);
    }

    public void isSegrepLimitBreached(ShardId shardId) {
        // TODO.
    }

    public SegmentReplicationStats nodeStats() {
        return tracker.getStats();
    }
}
