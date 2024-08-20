/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.tiering.TieringRequests;
import org.opensearch.action.admin.indices.tiering.TieringRequestContext;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RunningTieringStateProcessor extends AbstractTieringStateProcessor {
    private static final Logger logger = LogManager.getLogger(RunningTieringStateProcessor.class);

    public RunningTieringStateProcessor(AllocationService allocationService) {
        super(IndexTieringState.PENDING_COMPLETION, allocationService);
    }

    /**
     * Checks if the shard is in the warm tier.
     * @param shard shard routing
     * @param clusterState current cluster state
     * @return true if shard is started on the search node, false otherwise
     */
    boolean isShardInWarmTier(final ShardRouting shard, final ClusterState clusterState) {
        if (shard.unassigned()) {
            return false;
        }
        final boolean isShardFoundOnSearchNode = clusterState.getNodes().get(shard.currentNodeId()).isSearchNode();
        return shard.started() && isShardFoundOnSearchNode;
    }

    @Override
    public void process(ClusterState clusterState, ClusterService clusterService, TieringRequests tieringRequests) {
        final Map<Index, TieringRequestContext> tieredIndices = new HashMap<>();
        for (TieringRequestContext tieringRequestContext : tieringRequests.getAcceptedTieringRequestContexts()) {
            List<ShardRouting> shardRoutings;
            for (Index index : tieringRequestContext.filterIndicesByState(IndexTieringState.IN_PROGRESS)) {
                if (clusterState.routingTable().hasIndex(index)) {
                    // Ensure index is not deleted
                    shardRoutings = clusterState.routingTable().allShards(index.getName());
                } else {
                    // Index already deleted nothing to do
                    logger.warn("[HotToWarmTiering] Index [{}] deleted before shard relocation finished", index.getName());
                    tieringRequestContext.markIndexFailed(index, "index not found");
                    continue;
                }

                boolean relocationCompleted = true;
                for (ShardRouting shard : shardRoutings) {
                    if (!isShardInWarmTier(shard, clusterState)) {
                        relocationCompleted = false;
                        break;
                    }
                }
                if (relocationCompleted) {
                    logger.debug("[HotToWarmTiering] Shard relocation completed for index [{}]", index.getName());
                    tieringRequestContext.markIndexAsPendingComplete(index);
                    tieredIndices.put(index, tieringRequestContext);
                }
            }
        }
        assert nextStateProcessor != null;
        nextStateProcessor.process(clusterState, clusterService, tieringRequests);
    }
}
