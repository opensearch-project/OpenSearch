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
import org.opensearch.cluster.routing.Murmur3HashFunction;

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
     * Computes the virtual shard id for a document given its id and optional routing.
     * Replicates the hash chain used by {@code OperationRouting.generateShardId},
     * including partition offset for routing-partitioned indices.
     *
     * @throws IllegalStateException if virtual shards are not enabled on the index
     */
    public static int computeVirtualShardId(IndexMetadata indexMetadata, String id, String routing) {
        int numVirtualShards = indexMetadata.getNumberOfVirtualShards();
        if (numVirtualShards <= 0) {
            throw new IllegalStateException("virtual shards are not enabled on index [" + indexMetadata.getIndex() + "]");
        }
        String effectiveRouting = (routing != null) ? routing : id;
        int partitionOffset = indexMetadata.isRoutingPartitionedIndex()
            ? Math.floorMod(Murmur3HashFunction.hash(id), indexMetadata.getRoutingPartitionSize())
            : 0;
        int hash = Murmur3HashFunction.hash(effectiveRouting) + partitionOffset;
        return Math.floorMod(hash, numVirtualShards);
    }

    /**
     * Resolves the physical shard for a virtual shard id.
     */
    public static int resolvePhysicalShardId(IndexMetadata indexMetadata, int vShardId) {
        int numVirtualShards = indexMetadata.getNumberOfVirtualShards();
        int numPhysicalShards = indexMetadata.getNumberOfShards();

        if (numVirtualShards < numPhysicalShards || numVirtualShards % numPhysicalShards != 0) {
            throw new IllegalArgumentException(
                "Virtual shards must be enabled and be a multiple of the number of physical shards to resolve routing."
            );
        }

        vShardId = Math.floorMod(vShardId, numVirtualShards);

        Map<String, String> overrides = indexMetadata.getCustomData(VIRTUAL_SHARDS_CUSTOM_METADATA_KEY);
        if (overrides != null) {
            String pShardIdStr = overrides.get(String.valueOf(vShardId));
            if (pShardIdStr != null) {
                try {
                    int pShardId = Integer.parseInt(pShardIdStr);
                    if (pShardId >= 0 && pShardId < numPhysicalShards) {
                        return pShardId;
                    }
                    logger.trace("Invalid override value [{}] for vShard [{}]: out of bounds", pShardId, vShardId);
                } catch (NumberFormatException e) {
                    logger.trace("Invalid override value [{}] for vShard [{}]: not a number", pShardIdStr, vShardId);
                }
            }
        }

        int virtualShardsPerPhysical = numVirtualShards / numPhysicalShards;
        return vShardId / virtualShardsPerPhysical;
    }
}
