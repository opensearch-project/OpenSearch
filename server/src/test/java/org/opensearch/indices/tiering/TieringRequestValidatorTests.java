/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.tiering;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.tiering.TieringValidationResult;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.opensearch.indices.tiering.TieringRequestValidator.getEligibleNodes;
import static org.opensearch.indices.tiering.TieringRequestValidator.getIndexPrimaryStoreSize;
import static org.opensearch.indices.tiering.TieringRequestValidator.getTotalAvailableBytesInWarmTier;
import static org.opensearch.indices.tiering.TieringRequestValidator.validateDiskThresholdWaterMarkNotBreached;
import static org.opensearch.indices.tiering.TieringRequestValidator.validateEligibleNodesCapacity;
import static org.opensearch.indices.tiering.TieringRequestValidator.validateIndexHealth;
import static org.opensearch.indices.tiering.TieringRequestValidator.validateOpenIndex;
import static org.opensearch.indices.tiering.TieringRequestValidator.validateRemoteStoreIndex;
import static org.opensearch.indices.tiering.TieringRequestValidator.validateSearchNodes;

public class TieringRequestValidatorTests extends OpenSearchTestCase {

    public void testValidateSearchNodes() {
        ClusterState clusterStateWithSearchNodes = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(2, 0, 0))
            .build();

        // throws no errors
        validateSearchNodes(clusterStateWithSearchNodes, "test_index");
    }

    public void testWithNoSearchNodesInCluster() {
        ClusterState clusterStateWithNoSearchNodes = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(0, 1, 1))
            .build();
        // throws error
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> validateSearchNodes(clusterStateWithNoSearchNodes, "test")
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

        assertTrue(validateRemoteStoreIndex(clusterState1, new Index(indexName, indexUuid)));
    }

    public void testDocRepIndex() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        assertFalse(validateRemoteStoreIndex(buildClusterState(indexName, indexUuid, Settings.EMPTY), new Index(indexName, indexUuid)));
    }

    public void testValidateIndexHealth() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        ClusterState clusterState = buildClusterState(indexName, indexUuid, Settings.EMPTY);
        assertTrue(validateIndexHealth(clusterState, new Index(indexName, indexUuid)));
    }

    public void testValidOpenIndex() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        assertTrue(validateOpenIndex(buildClusterState(indexName, indexUuid, Settings.EMPTY), new Index(indexName, indexUuid)));
    }

    public void testCloseIndex() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        assertFalse(
            validateOpenIndex(
                buildClusterState(indexName, indexUuid, Settings.EMPTY, IndexMetadata.State.CLOSE),
                new Index(indexName, indexUuid)
            )
        );
    }

    public void testValidateDiskThresholdWaterMarkNotBreached() {
        int noOfNodes = 2;
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(noOfNodes, 0, 0))
            .build();

        ClusterInfo clusterInfo = clusterInfo(noOfNodes, 100, 20);
        DiskThresholdSettings diskThresholdSettings = diskThresholdSettings("10b", "10b", "5b");
        // throws no error
        validateDiskThresholdWaterMarkNotBreached(clusterState, clusterInfo, diskThresholdSettings, "test");
    }

    public void testValidateDiskThresholdWaterMarkNotBreachedThrowsError() {
        int noOfNodes = 2;
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(noOfNodes, 0, 0))
            .build();
        ClusterInfo clusterInfo = clusterInfo(noOfNodes, 100, 5);
        DiskThresholdSettings diskThresholdSettings = diskThresholdSettings("10b", "10b", "5b");
        // throws error
        expectThrows(
            IllegalArgumentException.class,
            () -> validateDiskThresholdWaterMarkNotBreached(clusterState, clusterInfo, diskThresholdSettings, "test")
        );
    }

    public void testGetTotalIndexSize() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        ClusterState clusterState = ClusterState.builder(buildClusterState(indexName, indexUuid, Settings.EMPTY))
            .nodes(createNodes(1, 0, 0))
            .build();
        Map<String, DiskUsage> diskUsages = diskUsages(1, 100, 50);
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test_index][0][p]", 10L); // 10 bytes
        ClusterInfo clusterInfo = new ClusterInfo(diskUsages, null, shardSizes, null, Map.of(), Map.of());
        assertEquals(10, getIndexPrimaryStoreSize(clusterState, clusterInfo, indexName));
    }

    public void testValidateEligibleNodesCapacityWithAllAccepted() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        Set<Index> indices = Set.of(new Index(indexName, indexUuid));
        ClusterState clusterState = ClusterState.builder(buildClusterState(indexName, indexUuid, Settings.EMPTY))
            .nodes(createNodes(1, 0, 0))
            .build();
        Map<String, DiskUsage> diskUsages = diskUsages(1, 100, 50);
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test_index][0][p]", 10L); // 10 bytes
        ClusterInfo clusterInfo = new ClusterInfo(diskUsages, null, shardSizes, null, Map.of(), Map.of());
        TieringValidationResult tieringValidationResult = new TieringValidationResult(indices);
        validateEligibleNodesCapacity(clusterInfo, clusterState, tieringValidationResult);
        assertEquals(indices, tieringValidationResult.getAcceptedIndices());
        assertTrue(tieringValidationResult.getRejectedIndices().isEmpty());
    }

    public void testValidateEligibleNodesCapacityWithAllRejected() {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        Set<Index> indices = Set.of(new Index(indexName, indexUuid));
        ClusterState clusterState = ClusterState.builder(buildClusterState(indexName, indexUuid, Settings.EMPTY))
            .nodes(createNodes(1, 0, 0))
            .build();
        Map<String, DiskUsage> diskUsages = diskUsages(1, 100, 10);
        final Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test_index][0][p]", 20L); // 20 bytes
        ClusterInfo clusterInfo = new ClusterInfo(diskUsages, null, shardSizes, null, Map.of(), Map.of());
        TieringValidationResult tieringValidationResult = new TieringValidationResult(indices);
        validateEligibleNodesCapacity(clusterInfo, clusterState, tieringValidationResult);
        assertEquals(indices.size(), tieringValidationResult.getRejectedIndices().size());
        assertEquals(indices, tieringValidationResult.getRejectedIndices().keySet());
        assertTrue(tieringValidationResult.getAcceptedIndices().isEmpty());
    }

    public void testGetTotalAvailableBytesInWarmTier() {
        Map<String, DiskUsage> diskUsages = diskUsages(2, 500, 100);
        assertEquals(200, getTotalAvailableBytesInWarmTier(diskUsages, Set.of("node-s0", "node-s1")));
    }

    public void testEligibleNodes() {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(2, 0, 0))
            .build();

        assertEquals(2, getEligibleNodes(clusterState).size());

        clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(createNodes(0, 1, 1))
            .build();
        assertEquals(0, getEligibleNodes(clusterState).size());
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

    private static DiskThresholdSettings diskThresholdSettings(String low, String high, String flood) {
        return new DiskThresholdSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), low)
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), high)
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), flood)
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    private static ClusterInfo clusterInfo(int noOfNodes, long totalBytes, long freeBytes) {
        final Map<String, DiskUsage> diskUsages = diskUsages(noOfNodes, totalBytes, freeBytes);
        return new ClusterInfo(diskUsages, null, null, null, Map.of(), Map.of());
    }

    private static Map<String, DiskUsage> diskUsages(int noOfSearchNodes, long totalBytes, long freeBytes) {
        final Map<String, DiskUsage> diskUsages = new HashMap<>();
        for (int i = 0; i < noOfSearchNodes; i++) {
            diskUsages.put("node-s" + i, new DiskUsage("node-s" + i, "node-s" + i, "/foo/bar", totalBytes, freeBytes));
        }
        return diskUsages;
    }
}
