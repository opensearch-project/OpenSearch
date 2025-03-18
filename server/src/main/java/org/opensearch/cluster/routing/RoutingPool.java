/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.util.FeatureFlags;

import static org.opensearch.action.admin.indices.tiering.TieringUtils.isWarmIndex;

/**
 *  {@link RoutingPool} defines the different node types based on the assigned capabilities. The methods
 *  help decide the capabilities of a specific node as well as an index or shard based on the index configuration.
 *  These methods help with allocation decisions and determining shard classification with the allocation process.
 *
 * @opensearch.internal
 */
public enum RoutingPool {
    LOCAL_ONLY,
    REMOTE_CAPABLE;

    /**
     * Helps to determine the appropriate {@link RoutingPool} for a given node from the {@link RoutingNode}
     */
    public static RoutingPool getNodePool(RoutingNode node) {
        return getNodePool(node.node());
    }

    /**
     * Helps to determine the appropriate {@link RoutingPool} for a given node from the {@link DiscoveryNode}
     */
    public static RoutingPool getNodePool(DiscoveryNode node) {
        if (node.isSearchNode()) {
            return REMOTE_CAPABLE;
        }
        return LOCAL_ONLY;
    }

    /**
     * Can determine the appropriate {@link RoutingPool} for a given shard using the {@link IndexMetadata} for the
     * index using the {@link RoutingAllocation}.
     * @param shard the shard routing for which {@link RoutingPool} has to be determined.
     * @param allocation the current allocation of the cluster
     * @return {@link RoutingPool} for the given shard.
     */
    public static RoutingPool getShardPool(ShardRouting shard, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shard.index());
        return getIndexPool(indexMetadata);
    }

    /**
     * Can determine the appropriate {@link RoutingPool} for a given index using the {@link IndexMetadata}.
     * @param indexMetadata the index metadata object for which {@link RoutingPool} has to be determined.
     * @return {@link RoutingPool} for the given index.
     */
    public static RoutingPool getIndexPool(IndexMetadata indexMetadata) {
        return indexMetadata.isRemoteSnapshot()
            || (FeatureFlags.isEnabled(FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG) && isWarmIndex(indexMetadata))
                ? REMOTE_CAPABLE
                : LOCAL_ONLY;

    }
}
