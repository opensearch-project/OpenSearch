/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.tiering;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsTests;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.StoreStats;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tiering.TieringServiceValidator;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.emptyList;

public class TieringRequestValidatorTests extends OpenSearchTestCase {

    private TieringServiceValidator validator = new TieringServiceValidator();

    public void testValidateSearchNodes() {

        ClusterState clusterStateWithSearchNodes = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(2, 0, 0))
            .build();

        // throws no errors
        validator.validateSearchNodes(clusterStateWithSearchNodes, "test_index");
    }

    public void testValidateSearchNodesThrowError() {

        ClusterState clusterStateWithNoSearchNodes = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(0, 1, 1))
            .build();
        // throws error
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validator.validateSearchNodes(clusterStateWithNoSearchNodes, "test")
        );
    }

    public void testValidRemoteStoreIndex() {

        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";

        ClusterState clusterState1 = buildClusterState(
            indexName,
            indexUuid,
            Settings.builder()
                .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
                .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
                .build()
        );

        assertTrue(validator.validateRemoteStoreIndex(clusterState1, new Index(indexName, indexUuid)));
    }

    public void testNotRemoteStoreIndex() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        assertFalse(
            validator.validateRemoteStoreIndex(buildClusterState(indexName, indexUuid, Settings.EMPTY), new Index(indexName, indexUuid))
        );
    }

    public void testValidHotIndex() {

        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        assertTrue(validator.validateHotIndex(buildClusterState(indexName, indexUuid, Settings.EMPTY), new Index(indexName, indexUuid)));
    }

    public void testNotHotIndex() {

        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";

        IndexModule.TieringState tieringState = randomBoolean() ? IndexModule.TieringState.HOT_TO_WARM : IndexModule.TieringState.WARM;

        ClusterState clusterState = buildClusterState(
            indexName,
            indexUuid,
            Settings.builder().put(IndexModule.INDEX_TIERING_STATE.getKey(), tieringState).build()
        );
        assertFalse(validator.validateHotIndex(clusterState, new Index(indexName, indexUuid)));
    }

    public void testValidateIndexHealth() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        ClusterState clusterState = buildClusterState(indexName, indexUuid, Settings.EMPTY);
        assertTrue(validator.validateIndexHealth(clusterState, new Index(indexName, indexUuid)));
    }

    public void testValidOpenIndex() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        assertTrue(validator.validateOpenIndex(buildClusterState(indexName, indexUuid, Settings.EMPTY), new Index(indexName, indexUuid)));
    }

    public void testNotValidOpenIndex() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        assertFalse(
            validator.validateOpenIndex(
                buildClusterState(indexName, indexUuid, Settings.EMPTY, IndexMetadata.State.CLOSE),
                new Index(indexName, indexUuid)
            )
        );
    }

    public void testValidateDiskThresholdWaterMarkNotBreached() {
        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE),
            Version.CURRENT
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE),
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .build();

        NodesStatsResponse nodesStatsResponse = createNodesStatsResponse(
            List.of(node1, node2),
            clusterState.getClusterName(),
            100,
            90,
            100
        );
        // throws no error
        // validator.validateDiskThresholdWaterMarkNotBreached(clusterState, nodesStatsResponse, new ByteSizeValue(20));
    }

    public void testValidateDiskThresholdWaterMarkNotBreachedThrowsError() {
        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE),
            Version.CURRENT
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE),
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .build();

        NodesStatsResponse nodesStatsResponse = createNodesStatsResponse(List.of(node1, node2), clusterState.getClusterName(), 100, 90, 20);
        // throws error
        // expectThrows(IllegalArgumentException.class, () -> validator.validateDiskThresholdWaterMarkNotBreached(clusterState,
        // nodesStatsResponse, new ByteSizeValue(100)));
    }

    // public void testValidateEligibleNodesCapacity() {
    // String indexUuid = UUID.randomUUID().toString();
    // String indexName = "test_index";
    //
    // DiscoveryNode node1 = new DiscoveryNode("node1", buildNewFakeTransportAddress(),
    // Collections.emptyMap(), Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE), Version.CURRENT);
    // DiscoveryNode node2 = new DiscoveryNode("node2", buildNewFakeTransportAddress(),
    // Collections.emptyMap(), Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE), Version.CURRENT);
    // ClusterState clusterState = ClusterState.builder(buildClusterState(indexName, indexUuid, Settings.EMPTY))
    // .nodes(DiscoveryNodes.builder()
    // .add(node1)
    // .add(node2)
    // .build())
    // .build();
    //
    // NodesStatsResponse nodesStatsResponse = createNodesStatsResponse(List.of(node1, node2), clusterState.getClusterName(), 100, 90, 100);
    // Index index = new Index(indexName, indexUuid);
    // // Setting shard per size as 10 b
    // long sizePerShard = 10;
    // IndicesStatsResponse indicesStatsResponse = createIndicesStatsResponse(index, clusterState, sizePerShard);
    // Index[] indices = new Index[]{index};
    // List<Index> acceptedIndices = validator.validateEligibleNodesCapacity(nodesStatsResponse, indicesStatsResponse, clusterState,
    // indices, 0);
    //
    // assertEquals(indices.length, acceptedIndices.size());
    // assertEquals(Arrays.asList(indices), acceptedIndices);
    //
    // nodesStatsResponse = createNodesStatsResponse(List.of(node1, node2), clusterState.getClusterName(), 100, 90, 100);
    // // Setting shard per size as 10 mb
    // sizePerShard = 10 * 1024;
    // indicesStatsResponse = createIndicesStatsResponse(index, clusterState, sizePerShard);
    // acceptedIndices = validator.validateEligibleNodesCapacity(nodesStatsResponse, indicesStatsResponse, clusterState, indices, 0);
    //
    // assertEquals(0, acceptedIndices.size());
    // }

    public void testGetTotalAvailableBytesInWarmTier() {
        DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE),
            Version.CURRENT
        );
        DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE),
            Version.CURRENT
        );
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).build())
            .build();
        NodesStatsResponse nodesStatsResponse = createNodesStatsResponse(
            List.of(node1, node2),
            clusterState.getClusterName(),
            100,
            90,
            100
        );
        // assertEquals(200, validator.getTotalAvailableBytesInWarmTier(nodesStatsResponse, Set.of(node1, node2), 0));
    }

    public void testEligibleNodes() {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(2, 0, 0))
            .build();

        assertEquals(2, validator.getEligibleNodes(clusterState).size());

        clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(0, 1, 1))
            .build();
        assertEquals(0, validator.getEligibleNodes(clusterState).size());
    }

    private static ClusterState buildClusterState(String indexName, String indexUuid, Settings settings) {
        return buildClusterState(indexName, indexUuid, settings, IndexMetadata.State.OPEN);
    }

    private static ClusterState buildClusterState(String indexName, String indexUuid, Settings settings, IndexMetadata.State state) {
        Settings combinedSettings = Settings.builder().put(settings).put(createDefaultIndexSettings(indexUuid)).build();

        Metadata metadata = Metadata.builder().put(IndexMetadata.builder(indexName).settings(combinedSettings).state(state)).build();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();

        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
    }

    private static Settings createDefaultIndexSettings(String indexUuid) {
        return Settings.builder()
            .put("index.version.created", Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();
    }

    private DiscoveryNodes createNodes(int numOfSearchNodes, int numOfDataNodes, int numOfIngestNodes) {
        DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numOfSearchNodes; i++) {
            discoveryNodesBuilder.add(
                new DiscoveryNode(
                    "node-s" + i,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE),
                    Version.CURRENT
                )
            );
        }
        for (int i = 0; i < numOfDataNodes; i++) {
            discoveryNodesBuilder.add(
                new DiscoveryNode(
                    "node-d" + i,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
                    Version.CURRENT
                )
            );
        }
        for (int i = 0; i < numOfIngestNodes; i++) {
            discoveryNodesBuilder.add(
                new DiscoveryNode(
                    "node-i" + i,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.INGEST_ROLE),
                    Version.CURRENT
                )
            );
        }
        return discoveryNodesBuilder.build();
    }

    private ShardStats[] createShardStats(ShardRouting[] shardRoutings, long sizePerShard, Index index) {
        ShardStats[] shardStats = new ShardStats[shardRoutings.length];
        for (int i = 1; i <= shardRoutings.length; i++) {
            ShardRouting shardRouting = shardRoutings[i - 1];
            Path path = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(shardRouting.id()));
            CommonStats commonStats = new CommonStats();
            commonStats.store = new StoreStats(sizePerShard, 0);
            shardStats[i - 1] = new ShardStats(
                shardRouting,
                new ShardPath(false, path, path, shardRouting.shardId()),
                commonStats,
                null,
                null,
                null
            );
        }
        return shardStats;
    }

    private IndicesStatsResponse createIndicesStatsResponse(Index index, ClusterState clusterState, long sizePerShard) {
        ShardRouting[] shardRoutings = clusterState.routingTable()
            .allShards(index.getName())
            .stream()
            .filter(ShardRouting::primary)
            .toArray(ShardRouting[]::new);

        ShardStats[] shardStats = createShardStats(shardRoutings, sizePerShard, index);
        return IndicesStatsTests.newIndicesStatsResponse(shardStats, shardStats.length, shardStats.length, 0, emptyList());
    }

    private NodesStatsResponse createNodesStatsResponse(
        List<DiscoveryNode> nodes,
        ClusterName clusterName,
        long total,
        long free,
        long available
    ) {
        FsInfo fsInfo = new FsInfo(0, null, new FsInfo.Path[] { new FsInfo.Path("/path", "/dev/sda", total, free, available) });
        List<NodeStats> nodeStats = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            nodeStats.add(
                new NodeStats(
                    node,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    fsInfo,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            );
        }
        return new NodesStatsResponse(clusterName, nodeStats, new ArrayList<>());
    }
}
