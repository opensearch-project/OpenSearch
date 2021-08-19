/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.stats.IndexingPressureStats;
import org.opensearch.index.stats.ShardIndexingPressureStats;

/**
 * Sets up classes for node/shard level indexing pressure.
 * Provides abstraction and orchestration for indexing pressure interfaces when called from Transport Actions or for Stats.
 */
public class IndexingPressureService {

    private final ShardIndexingPressure shardIndexingPressure;

    public IndexingPressureService(Settings settings, ClusterService clusterService) {
        shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
    }

    public Releasable markCoordinatingOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if (isShardIndexingPressureEnabled()) {
            return shardIndexingPressure.markCoordinatingOperationStarted(shardId, bytes, forceExecution);
        } else {
            return shardIndexingPressure.markCoordinatingOperationStarted(bytes, forceExecution);
        }
    }

    public Releasable markPrimaryOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if (isShardIndexingPressureEnabled()) {
            return shardIndexingPressure.markPrimaryOperationStarted(shardId, bytes, forceExecution);
        } else {
            return shardIndexingPressure.markPrimaryOperationStarted(bytes, forceExecution);
        }
    }

    public Releasable markPrimaryOperationLocalToCoordinatingNodeStarted(ShardId shardId, long bytes) {
        if (isShardIndexingPressureEnabled()) {
            return shardIndexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(shardId, bytes);
        } else {
            return shardIndexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(bytes);
        }
    }

    public Releasable markReplicaOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if (isShardIndexingPressureEnabled()) {
            return shardIndexingPressure.markReplicaOperationStarted(shardId, bytes, forceExecution);
        } else {
            return shardIndexingPressure.markReplicaOperationStarted(bytes, forceExecution);
        }
    }

    public IndexingPressureStats nodeStats() {
        return shardIndexingPressure.stats();
    }

    public ShardIndexingPressureStats shardStats(CommonStatsFlags statsFlags) {
        return shardIndexingPressure.shardStats(statsFlags);
    }

    private boolean isShardIndexingPressureEnabled() {
        return shardIndexingPressure.isShardIndexingPressureEnabled();
    }
}
