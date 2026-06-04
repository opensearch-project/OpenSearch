/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceSplitShardClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class MetadataInPlaceSplitShardServiceTests extends OpenSearchTestCase {

    // --- applySplitShardRequest: success cases ---

    public void testApplySplitShardRequestUpdatesMetadata() {
        ClusterState state = createClusterState("test-index", 3, 1);
        InPlaceSplitShardClusterStateUpdateRequest request = newRequest("test-index", 0, 2);

        ClusterState updatedState = applyRequest(state, request);

        SplitShardsMetadata splitMetadata = updatedState.metadata().index("test-index").getSplitShardsMetadata();
        assertTrue(splitMetadata.isSplitOfShardInProgress(0));
        assertEquals(2, splitMetadata.getChildShardIdsOfParent(0).size());
    }

    public void testApplySplitShardRequestCallsReroute() {
        ClusterState state = createClusterState("test-index", 3, 1);
        InPlaceSplitShardClusterStateUpdateRequest request = newRequest("test-index", 1, 3);

        boolean[] rerouteCalled = { false };
        MetadataInPlaceSplitShardService.applySplitShardRequest(state, request, (cs, reason) -> {
            rerouteCalled[0] = true;
            assertTrue(reason.contains("shard [1]"));
            assertTrue(reason.contains("test-index"));
            return cs;
        });

        assertTrue(rerouteCalled[0]);
    }

    public void testApplySplitShardRequestBumpsVersion() {
        ClusterState state = createClusterState("test-index", 3, 0);
        long versionBefore = state.metadata().index("test-index").getVersion();

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0, 2));

        long versionAfter = updatedState.metadata().index("test-index").getVersion();
        assertTrue(versionAfter > versionBefore);
    }

    public void testApplySplitShardRequestMultipleShardsIndependently() {
        ClusterState state = createClusterState("test-index", 5, 0);

        ClusterState afterSplit0 = applyRequest(state, newRequest("test-index", 0, 2));
        ClusterState afterSplit2 = applyRequest(afterSplit0, newRequest("test-index", 2, 3));

        SplitShardsMetadata splitMetadata = afterSplit2.metadata().index("test-index").getSplitShardsMetadata();
        assertTrue(splitMetadata.isSplitOfShardInProgress(0));
        assertTrue(splitMetadata.isSplitOfShardInProgress(2));
        assertEquals(2, splitMetadata.getChildShardIdsOfParent(0).size());
        assertEquals(3, splitMetadata.getChildShardIdsOfParent(2).size());
        assertFalse(splitMetadata.isSplitOfShardInProgress(1));
    }

    public void testApplySplitShardRequestPreservesExistingMetadata() {
        ClusterState state = createClusterState("test-index", 3, 1);
        IndexMetadata originalMetadata = state.metadata().index("test-index");

        ClusterState updatedState = applyRequest(state, newRequest("test-index", 0, 2));

        IndexMetadata updatedMetadata = updatedState.metadata().index("test-index");
        assertEquals(originalMetadata.getNumberOfShards(), updatedMetadata.getNumberOfShards());
        assertEquals(originalMetadata.getNumberOfReplicas(), updatedMetadata.getNumberOfReplicas());
        assertEquals(originalMetadata.getIndex(), updatedMetadata.getIndex());
    }

    public void testApplySplitShardRequestChildShardIdsAreUnique() {
        ClusterState state = createClusterState("test-index", 5, 0);

        ClusterState afterSplit0 = applyRequest(state, newRequest("test-index", 0, 3));
        ClusterState afterSplit1 = applyRequest(afterSplit0, newRequest("test-index", 1, 2));

        SplitShardsMetadata splitMetadata = afterSplit1.metadata().index("test-index").getSplitShardsMetadata();
        Set<Integer> allChildIds = new HashSet<>();
        allChildIds.addAll(splitMetadata.getChildShardIdsOfParent(0));
        allChildIds.addAll(splitMetadata.getChildShardIdsOfParent(1));
        assertEquals(5, allChildIds.size()); // 3 + 2, all unique
    }

    // --- applySplitShardRequest: error cases ---

    public void testApplySplitShardRequestThrowsIfAlreadyInProgress() {
        ClusterState state = createClusterState("test-index", 3, 1);
        ClusterState afterFirstSplit = applyRequest(state, newRequest("test-index", 0, 2));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(afterFirstSplit, newRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage().contains("already in progress"));
    }

    public void testApplySplitShardRequestThrowsIfAlreadySplit() {
        ClusterState state = createClusterState("test-index", 3, 1);

        IndexMetadata indexMetadata = state.metadata().index("test-index");
        SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(indexMetadata.getSplitShardsMetadata());
        var childShards = splitBuilder.splitShard(0, 2);
        Set<Integer> childIds = new HashSet<>();
        childShards.forEach(s -> childIds.add(s.shardId()));
        splitBuilder.updateSplitMetadataForChildShards(0, childIds);

        IndexMetadata.Builder imBuilder = IndexMetadata.builder(indexMetadata);
        imBuilder.splitShardsMetadata(splitBuilder.build());
        Metadata.Builder metaBuilder = Metadata.builder(state.metadata()).put(imBuilder);
        ClusterState stateWithCompletedSplit = ClusterState.builder(state).metadata(metaBuilder).build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(stateWithCompletedSplit, newRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage().contains("already been split"));
    }

    public void testApplySplitShardRequestThrowsForNonExistentIndex() {
        ClusterState state = createClusterState("test-index", 3, 1);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(state, newRequest("non-existent-index", 0, 2))
        );
        assertTrue(e.getMessage().contains("not found"));
    }

    public void testApplySplitShardRequestThrowsForInvalidShardId() {
        ClusterState state = createClusterState("test-index", 3, 0);

        expectThrows(Exception.class, () -> applyRequest(state, newRequest("test-index", 99, 2)));
    }

    public void testApplySplitShardRequestThrowsForSplitIntoZero() {
        ClusterState state = createClusterState("test-index", 3, 0);

        expectThrows(ArithmeticException.class, () -> applyRequest(state, newRequest("test-index", 0, 0)));
    }

    public void testApplySplitShardRequestThrowsIfVirtualShardsEnabled() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, 6)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings).build();
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(state, newRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage().contains("virtual shards"));
    }

    public void testApplySplitShardRequestThrowsInMixedVersionCluster() {
        ClusterState state = createClusterState("test-index", 3, 0);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.V_3_6_0));
        nodesBuilder.add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.V_3_5_0));
        state = ClusterState.builder(state).nodes(nodesBuilder).build();

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(finalState, newRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage().contains("same version"));
    }

    public void testApplySplitShardRequestThrowsIfNodeBelowSplitVersion() {
        ClusterState state = createClusterState("test-index", 3, 0);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        nodesBuilder.add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.V_3_5_0));
        nodesBuilder.add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.V_3_5_0));
        state = ClusterState.builder(state).nodes(nodesBuilder).build();

        ClusterState finalState = state;
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(finalState, newRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage().contains(Version.V_3_7_0.toString()));
    }

    public void testApplySplitShardRequestThrowsIfShardIsRelocating() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings).build();
        Index index = indexMetadata.getIndex();

        // Build a routing table where shard 0 is relocating
        ShardRouting relocatingShard = TestShardRouting.newShardRouting(
            new ShardId(index, 0),
            "node1",
            "node2",
            true,
            ShardRoutingState.RELOCATING
        );
        IndexShardRoutingTable.Builder shardTable0 = new IndexShardRoutingTable.Builder(new ShardId(index, 0)).addShard(relocatingShard);
        IndexShardRoutingTable.Builder shardTable1 = new IndexShardRoutingTable.Builder(new ShardId(index, 1)).addShard(
            TestShardRouting.newShardRouting(new ShardId(index, 1), "node1", true, ShardRoutingState.STARTED)
        );
        IndexShardRoutingTable.Builder shardTable2 = new IndexShardRoutingTable.Builder(new ShardId(index, 2)).addShard(
            TestShardRouting.newShardRouting(new ShardId(index, 2), "node1", true, ShardRoutingState.STARTED)
        );
        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index).addIndexShard(shardTable0.build())
            .addIndexShard(shardTable1.build())
            .addIndexShard(shardTable2.build());

        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(state, newRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage().contains("relocating"));
    }

    public void testApplySplitShardRequestThrowsIfPrimaryNotStarted() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(indexSettings).build();
        // addAsNew creates UNASSIGNED shards
        ClusterState state = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> applyRequest(state, newRequest("test-index", 0, 2))
        );
        assertTrue(e.getMessage().contains("primary shard is not started"));
    }

    // --- helpers ---

    private static ClusterState applyRequest(ClusterState state, InPlaceSplitShardClusterStateUpdateRequest request) {
        return MetadataInPlaceSplitShardService.applySplitShardRequest(state, request, (cs, reason) -> cs);
    }

    private static InPlaceSplitShardClusterStateUpdateRequest newRequest(String index, int shardId, int splitInto) {
        return new InPlaceSplitShardClusterStateUpdateRequest("test", index, shardId, splitInto);
    }

    private ClusterState createClusterState(String indexName, int numShards, int numReplicas) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings).build();
        Index index = indexMetadata.getIndex();

        IndexRoutingTable.Builder indexRoutingBuilder = new IndexRoutingTable.Builder(index);
        for (int i = 0; i < numShards; i++) {
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(new ShardId(index, i));
            shardBuilder.addShard(TestShardRouting.newShardRouting(new ShardId(index, i), "node1", true, ShardRoutingState.STARTED));
            for (int r = 0; r < numReplicas; r++) {
                shardBuilder.addShard(
                    TestShardRouting.newShardRouting(new ShardId(index, i), "node" + (r + 2), false, ShardRoutingState.STARTED)
                );
            }
            indexRoutingBuilder.addIndexShard(shardBuilder.build());
        }

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(indexRoutingBuilder).build())
            .build();
    }
}
