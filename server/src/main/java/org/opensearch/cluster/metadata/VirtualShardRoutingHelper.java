/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Resolves virtual shard routing to physical shard IDs.
 */

public final class VirtualShardRoutingHelper {

    private VirtualShardRoutingHelper() {}

    private static final Logger logger = LogManager.getLogger(VirtualShardRoutingHelper.class);

    /**
     * Custom Metadata key for storing virtual shard routing overrides.
     */
    public static final String VIRTUAL_SHARDS_CUSTOM_METADATA_KEY = "virtual_shards_routing";

    /**
     * Resolves the Physical Shard ID for a given Virtual Shard ID.
     *
     * @param indexMetadata The index metadata.
     * @param vShardId The virtual shard ID.
     * @return The physical shard ID.
     */
    public static int resolvePhysicalShardId(IndexMetadata indexMetadata, int vShardId) {
        Map<String, String> overrides = indexMetadata.getCustomData(VIRTUAL_SHARDS_CUSTOM_METADATA_KEY);
        if (overrides != null) {
            String pShardIdStr = overrides.get(String.valueOf(vShardId));
            if (pShardIdStr != null) {
                try {
                    int pShardId = Integer.parseInt(pShardIdStr);
                    if (pShardId >= 0 && pShardId < indexMetadata.getNumberOfShards()) {
                        return pShardId;
                    }
                    logger.trace("Invalid override value [{}] for vShard [{}]: out of bounds", pShardId, vShardId);
                } catch (NumberFormatException e) {
                    logger.trace("Invalid override value [{}] for vShard [{}]: not a number", pShardIdStr, vShardId);
                }
            }
        }

        int numVirtualShards = indexMetadata.getNumberOfVirtualShards();
        int numPhysicalShards = indexMetadata.getNumberOfShards();

        if (numVirtualShards < numPhysicalShards || numVirtualShards % numPhysicalShards != 0) {
            throw new IllegalArgumentException(
                "Virtual shards must be enabled and be a multiple of the number of physical shards to resolve routing."
            );
        }

        int virtualShardsPerPhysical = numVirtualShards / numPhysicalShards;
        return vShardId / virtualShardsPerPhysical;
    }
}
