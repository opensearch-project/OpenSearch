/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.repositories.IndexId;

import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;

import static org.opensearch.cluster.block.ClusterBlockLevel.WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_SEARCH_ONLY_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_SEARCH_ONLY_BLOCK_ID;

/**
 * Utility class responsible for constructing new cluster states during scale operations.
 * <p>
 * This builder constructs cluster states for both scale-up and scale-down operations, handling
 * the transition of indices between normal operation and search-only mode. It manages:
 * <ul>
 *   <li>Adding temporary write blocks during scale-down preparation</li>
 *   <li>Updating index metadata and settings when finalizing scale operations</li>
 *   <li>Modifying routing tables to add/remove shards based on scaling direction</li>
 * </ul>
 * <p>
 * The cluster state modifications ensure proper synchronization of operations and maintain
 * data integrity throughout the scaling process.
 */
class ScaleIndexClusterStateBuilder {

    /**
     * Builds the new cluster state by adding a temporary scale-down block on the target index.
     * <p>
     * This temporary block prevents writes to the index during the preparation phase of scaling down,
     * allowing existing operations to complete before transitioning to search-only mode.
     *
     * @param currentState   the current cluster state
     * @param index          the name of the index being scaled down
     * @param blockedIndices map to store the association between indices and their scale blocks
     * @return the modified cluster state with temporary write blocks applied
     */
    ClusterState buildScaleDownState(ClusterState currentState, String index, Map<Index, ClusterBlock> blockedIndices) {
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());

        IndexMetadata indexMetadata = currentState.metadata().index(index);
        Index idx = indexMetadata.getIndex();
        ClusterBlock scaleBlock = createScaleDownBlock();

        blocksBuilder.addIndexBlock(index, scaleBlock);
        blockedIndices.put(idx, scaleBlock);

        return ClusterState.builder(currentState)
            .metadata(metadataBuilder)
            .blocks(blocksBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
    }

    /**
     * Builds the final cluster state for completing a scale-down operation.
     * <p>
     * This state modification:
     * <ul>
     *   <li>Removes the temporary scale-down block</li>
     *   <li>Updates index metadata to mark it as search-only</li>
     *   <li>Adds a permanent search-only block</li>
     *   <li>Updates the routing table to maintain only search replicas</li>
     * </ul>
     *
     * @param currentState the current cluster state
     * @param index        the name of the index being scaled down
     * @return the modified cluster state with finalized search-only configuration
     * @throws IllegalStateException if the specified index is not found
     */
    ClusterState buildFinalScaleDownState(ClusterState currentState, String index) {
        ClusterBlocks.Builder blocksBuilder = ClusterBlocks.builder().blocks(currentState.blocks());
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());

        IndexMetadata indexMetadata = currentState.metadata().index(index);
        if (indexMetadata == null) {
            throw new IllegalStateException("Index " + index + " not found");
        }

        blocksBuilder.removeIndexBlockWithId(index, INDEX_SEARCH_ONLY_BLOCK_ID);

        Settings updatedSettings = Settings.builder()
            .put(indexMetadata.getSettings())
            .put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
            .build();

        metadataBuilder.put(
            IndexMetadata.builder(indexMetadata).settings(updatedSettings).settingsVersion(indexMetadata.getSettingsVersion() + 1)
        );

        blocksBuilder.addIndexBlock(index, INDEX_SEARCH_ONLY_BLOCK);

        updateRoutingTableForScaleDown(routingTableBuilder, currentState, index);

        return ClusterState.builder(currentState)
            .metadata(metadataBuilder)
            .blocks(blocksBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
    }

    /**
     * Updates the routing table for a scale-down operation, removing non-search-only shards.
     * <p>
     * This method preserves only the search-only replica shards in the routing table,
     * effectively removing primary shards and standard replicas from the allocation.
     *
     * @param routingTableBuilder the routing table builder to modify
     * @param currentState        the current cluster state
     * @param index               the name of the index being scaled down
     */
    private void updateRoutingTableForScaleDown(RoutingTable.Builder routingTableBuilder, ClusterState currentState, String index) {
        IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
        if (indexRoutingTable != null) {
            IndexRoutingTable.Builder indexBuilder = new IndexRoutingTable.Builder(indexRoutingTable.getIndex());
            for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardTable.shardId());
                for (ShardRouting shardRouting : shardTable) {
                    if (shardRouting.isSearchOnly()) {
                        shardBuilder.addShard(shardRouting);
                    }
                }
                indexBuilder.addIndexShard(shardBuilder.build());
            }
            routingTableBuilder.add(indexBuilder.build());
        }
    }

    /**
     * Builds a new routing table for scaling up an index from search-only mode to normal operation.
     * <p>
     * This method:
     * <ul>
     *   <li>Preserves existing search-only replicas</li>
     *   <li>Creates new unassigned primary shards with remote store recovery source</li>
     *   <li>Creates new unassigned replica shards configured for peer recovery</li>
     * </ul>
     * <p>
     * The resulting routing table allows the cluster allocator to restore normal index operation
     * by recovering shards from remote storage.
     *
     * @param currentState the current cluster state
     * @param index        the name of the index being scaled up
     * @return the modified routing table with newly added primary and replica shards
     */
    RoutingTable buildScaleUpRoutingTable(ClusterState currentState, String index) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        IndexRoutingTable indexRoutingTable = currentState.routingTable().index(index);
        IndexMetadata indexMetadata = currentState.metadata().index(index);

        if (indexRoutingTable != null && indexMetadata != null) {
            IndexRoutingTable.Builder indexBuilder = new IndexRoutingTable.Builder(indexRoutingTable.getIndex());

            for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                indexBuilder.addIndexShard(buildShardTableForScaleUp(shardTable, indexMetadata));
            }
            routingTableBuilder.add(indexBuilder.build());
        }

        return routingTableBuilder.build();
    }

    /**
     * Builds a shard routing table for a scale-up operation.
     * <p>
     * For each shard, this method:
     * <ul>
     *   <li>Preserves all existing search-only replicas</li>
     *   <li>Creates a new unassigned primary shard configured to recover from remote store</li>
     *   <li>Creates a new unassigned replica shard configured to recover from peers</li>
     * </ul>
     *
     * @param shardTable the current shard routing table
     * @return a new shard routing table with both search replicas and newly added shards
     */

    private IndexShardRoutingTable buildShardTableForScaleUp(IndexShardRoutingTable shardTable, IndexMetadata indexMetadata) {
        IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shardTable.shardId());

        // Keep existing search-only shards
        for (ShardRouting shardRouting : shardTable) {
            if (shardRouting.isSearchOnly()) {
                shardBuilder.addShard(shardRouting);
            }
        }

        RecoverySource.RemoteStoreRecoverySource remoteStoreRecoverySource = new RecoverySource.RemoteStoreRecoverySource(
            UUID.randomUUID().toString(),
            Version.CURRENT,
            new IndexId(shardTable.shardId().getIndex().getName(), shardTable.shardId().getIndex().getUUID())
        );

        // Get replica settings from index metadata
        int numberOfReplicas = indexMetadata.getNumberOfReplicas();

        // Create primary shard
        ShardRouting primaryShard = ShardRouting.newUnassigned(
            shardTable.shardId(),
            true,
            remoteStoreRecoverySource,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "Restoring primary shard")
        );
        shardBuilder.addShard(primaryShard);

        // Create the correct number of replica shards
        for (int i = 0; i < numberOfReplicas; i++) {
            ShardRouting replicaShard = ShardRouting.newUnassigned(
                shardTable.shardId(),
                false,
                RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "Restoring replica shard")
            );
            shardBuilder.addShard(replicaShard);
        }

        return shardBuilder.build();
    }

    /**
     * Creates a temporary cluster block used during scale-down preparation.
     * <p>
     * This block:
     * <ul>
     *   <li>Prevents write operations to the index</li>
     *   <li>Uses a unique ID to track the block through the scaling process</li>
     *   <li>Returns a 403 Forbidden status for write attempts</li>
     *   <li>Includes a descriptive message</li>
     * </ul>
     *
     * @return a cluster block for temporary use during scale-down
     */
    static ClusterBlock createScaleDownBlock() {
        return new ClusterBlock(
            INDEX_SEARCH_ONLY_BLOCK_ID,
            UUIDs.randomBase64UUID(),
            "index preparing to scale down",
            false,
            false,
            false,
            RestStatus.FORBIDDEN,
            EnumSet.of(WRITE)
        );
    }
}
