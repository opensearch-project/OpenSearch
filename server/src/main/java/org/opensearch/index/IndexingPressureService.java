/*
 * Copyright OpenSearch Contributors.
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.index;

import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.stats.IndexingPressureStats;
import org.opensearch.index.stats.ShardIndexingPressureStats;

import java.util.function.LongSupplier;

/**
 * Sets up classes for node/shard level indexing pressure.
 * Provides abstraction and orchestration for indexing pressure interfaces when called from Transport Actions or for Stats.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.3.0")
public class IndexingPressureService {

    private final ShardIndexingPressure shardIndexingPressure;

    public IndexingPressureService(Settings settings, ClusterService clusterService) {
        shardIndexingPressure = new ShardIndexingPressure(settings, clusterService);
    }

    /**
     * Marks the beginning of coordinating operation for an indexing request on the node. Rejects the operation if node's
     * memory limit is breached.
     * Performs the node level accounting only if shard indexing pressure is disabled. Else empty releasable is returned.
     * @param bytes memory bytes to be tracked for the current operation
     * @param forceExecution permits operation even if the node level memory limit is breached
     * @return Releasable to mark the completion of operation and release the accounted bytes
     */
    public Releasable markCoordinatingOperationStarted(LongSupplier bytes, boolean forceExecution) {
        if (isShardIndexingPressureEnabled() == false) {
            return shardIndexingPressure.markCoordinatingOperationStarted(bytes.getAsLong(), forceExecution);
        } else {
            return () -> {};
        }
    }

    /**
     * Marks the beginning of coordinating operation for an indexing request on the Shard. Rejects the operation if shard's
     * memory limit is breached.
     * Performs the shard level accounting only if shard indexing pressure is enabled. Else empty releasable is returned.
     * @param shardId Shard ID for which the current indexing operation is targeted for
     * @param bytes memory bytes to be tracked for the current operation
     * @param forceExecution permits operation even if the node level memory limit is breached
     * @return Releasable to mark the completion of operation and release the accounted bytes
     */
    public Releasable markCoordinatingOperationStarted(ShardId shardId, LongSupplier bytes, boolean forceExecution) {
        if (isShardIndexingPressureEnabled()) {
            return shardIndexingPressure.markCoordinatingOperationStarted(shardId, bytes.getAsLong(), forceExecution);
        } else {
            return () -> {};
        }
    }

    /**
     * Marks the beginning of primary operation for an indexing request. Rejects the operation if memory limit is breached.
     * Performs the node level accounting only if shard indexing pressure is not disabled. Else shard level accounting
     * is performed.
     * @param shardId Shard ID for which the current indexing operation is targeted for
     * @param bytes memory bytes to be tracked for the current operation
     * @param forceExecution permits operation even if the memory limit is breached
     * @return Releasable to mark the completion of operation and release the accounted bytes
     */
    public Releasable markPrimaryOperationStarted(ShardId shardId, long bytes, boolean forceExecution) {
        if (isShardIndexingPressureEnabled()) {
            return shardIndexingPressure.markPrimaryOperationStarted(shardId, bytes, forceExecution);
        } else {
            return shardIndexingPressure.markPrimaryOperationStarted(bytes, forceExecution);
        }
    }

    /**
     * Marks the beginning of primary operation for an indexing request, when primary shard is local to the coordinator node.
     * Rejects the operation if memory limit is breached.
     * Performs the node level accounting only if shard indexing pressure is not disabled. Else shard level accounting
     * is performed.
     * @param shardId Shard ID for which the current indexing operation is targeted for
     * @param bytes memory bytes to be tracked for the current operation
     * @return Releasable to mark the completion of operation and release the accounted bytes
     */
    public Releasable markPrimaryOperationLocalToCoordinatingNodeStarted(ShardId shardId, long bytes) {
        if (isShardIndexingPressureEnabled()) {
            return shardIndexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(shardId, bytes);
        } else {
            return shardIndexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(bytes);
        }
    }

    /**
     * Marks the beginning of replication operation for an indexing request. Rejects the operation if memory limit is breached.
     * Performs the node level accounting only if shard indexing pressure is not disabled. Else shard level accounting
     * is performed.
     * @param shardId Shard ID for which the current indexing operation is targeted for
     * @param bytes memory bytes to be tracked for the current operation
     * @param forceExecution permits operation even if the memory limit is breached
     * @return Releasable to mark the completion of operation and release the accounted bytes
     */
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

    // visible for testing
    ShardIndexingPressure getShardIndexingPressure() {
        return shardIndexingPressure;
    }
}
