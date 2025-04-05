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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_SEARCH_ONLY_BLOCK;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_SEARCH_ONLY_BLOCK_ID;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;

public class ScaleIndexClusterStateBuilderTests extends OpenSearchTestCase {

    private ScaleIndexClusterStateBuilder builder;
    private ClusterState initialState;
    private String testIndex;
    private IndexMetadata indexMetadata;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        builder = new ScaleIndexClusterStateBuilder();
        testIndex = "test_index";

        // Create basic index metadata with segment replication enabled
        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_INDEX_UUID, randomAlphaOfLength(8))
            .put(SETTING_NUMBER_OF_SHARDS, 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 1)
            .put(SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)  // Add search replicas
            .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)  // Enable segment replication
            .put(SETTING_REMOTE_STORE_ENABLED, true)  // Enable remote store
            .put(SETTING_CREATION_DATE, System.currentTimeMillis())
            .build();

        indexMetadata = IndexMetadata.builder(testIndex).settings(indexSettings).build();

        // Create initial cluster state with routing table
        Metadata metadata = Metadata.builder().put(indexMetadata, true).build();

        initialState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .build();
    }

    public void testBuildScaleDownState() {
        Map<Index, ClusterBlock> blockedIndices = new HashMap<>();

        // Execute scale down state build
        ClusterState newState = builder.buildScaleDownState(initialState, testIndex, blockedIndices);

        // Verify block was added
        assertTrue("Scale down block should be present", newState.blocks().hasIndexBlockWithId(testIndex, INDEX_SEARCH_ONLY_BLOCK_ID));

        // Verify blocked indices map was updated
        assertFalse("Blocked indices map should not be empty", blockedIndices.isEmpty());
        assertEquals("Should have one blocked index", 1, blockedIndices.size());
        assertTrue("Index should be in blocked indices map", blockedIndices.containsKey(indexMetadata.getIndex()));
    }

    public void testBuildFinalScaleDownState() {
        Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        ClusterState stateWithBlock = builder.buildScaleDownState(initialState, testIndex, blockedIndices);

        ClusterState finalState = builder.buildFinalScaleDownState(stateWithBlock, testIndex);

        // Verify blocks
        assertFalse(
            "Temporary block should be removed",
            finalState.blocks().hasIndexBlock(testIndex, blockedIndices.get(indexMetadata.getIndex()))
        );
        assertTrue("Search-only block should be present", finalState.blocks().hasIndexBlock(testIndex, INDEX_SEARCH_ONLY_BLOCK));

        // Verify metadata was updated
        IndexMetadata updatedMetadata = finalState.metadata().index(testIndex);
        assertTrue(
            "Index should be marked as search-only",
            updatedMetadata.getSettings().getAsBoolean(INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false)
        );
    }

    public void testBuildScaleUpRoutingTable() {
        // Prepare a proper search-only state
        Settings scaleUpSettings = Settings.builder()
            .put(indexMetadata.getSettings())
            .put(INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true)
            .build();

        IndexMetadata searchOnlyMetadata = IndexMetadata.builder(indexMetadata).settings(scaleUpSettings).build();

        // Create search-only shard routing
        ShardRouting searchOnlyShard = ShardRouting.newUnassigned(
            new ShardId(searchOnlyMetadata.getIndex(), 0),
            false,  // not primary
            true, // search only
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        );

        // Build routing table with search-only shard
        IndexRoutingTable.Builder routingTableBuilder = new IndexRoutingTable.Builder(searchOnlyMetadata.getIndex()).addShard(
            searchOnlyShard
        );

        ClusterState searchOnlyState = ClusterState.builder(initialState)
            .metadata(Metadata.builder(initialState.metadata()).put(searchOnlyMetadata, true))
            .routingTable(RoutingTable.builder().add(routingTableBuilder.build()).build())
            .build();

        // Execute scale up
        RoutingTable newRoutingTable = builder.buildScaleUpRoutingTable(searchOnlyState, testIndex);

        // Verify routing table
        IndexRoutingTable indexRoutingTable = newRoutingTable.index(testIndex);
        assertNotNull("Index routing table should exist", indexRoutingTable);

        // Verify primary shard was added
        boolean hasPrimary = indexRoutingTable.shardsWithState(UNASSIGNED).stream().anyMatch(ShardRouting::primary);
        assertTrue("Should have an unassigned primary shard", hasPrimary);

        // Verify regular replicas were added (excluding search replicas)
        long replicaCount = indexRoutingTable.shardsWithState(UNASSIGNED)
            .stream()
            .filter(shard -> !shard.primary() && !shard.isSearchOnly())
            .count();
        assertEquals("Should have correct number of replica shards", indexMetadata.getNumberOfReplicas(), replicaCount);

        // Verify search replicas were preserved
        long searchReplicaCount = indexRoutingTable.shardsWithState(UNASSIGNED).stream().filter(ShardRouting::isSearchOnly).count();
        assertEquals("Should preserve search replicas", indexMetadata.getNumberOfSearchOnlyReplicas(), searchReplicaCount);
    }

    public void testBuildFinalScaleDownStateWithInvalidIndex() {
        expectThrows(IllegalStateException.class, () -> builder.buildFinalScaleDownState(initialState, "nonexistent_index"));
    }
}
