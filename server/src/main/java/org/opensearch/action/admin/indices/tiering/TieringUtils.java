/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.index.IndexModule;

/**
 * Utility class for tiering operations
 *
 * @opensearch.internal
 */
public class TieringUtils {

    /**
     *  Checks if the specified shard is a partial shard by
     *  checking the WARM_INDEX_ENABLED_SETTING for its index.
     *  see {@link #isWarmIndex(IndexMetadata)} (IndexMetadata)}
     * @param shard ShardRouting object representing the shard
     * @param allocation RoutingAllocation object representing the allocation
     * @return true if the shard is a partial shard, false otherwise
     */
    public static boolean isPartialShard(ShardRouting shard, RoutingAllocation allocation) {
        IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shard.index());
        return isWarmIndex(indexMetadata);
    }

    /**
     * Checks if the specified index is a warm index by
     * checking the WARM_INDEX_ENABLED_SETTING for the index.
     *
     * @param indexMetadata the metadata of the index
     * @return true if the index is a warm index, false otherwise
     */
    public static boolean isWarmIndex(final IndexMetadata indexMetadata) {
        return indexMetadata.getSettings().getAsBoolean(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false);
    }
}
