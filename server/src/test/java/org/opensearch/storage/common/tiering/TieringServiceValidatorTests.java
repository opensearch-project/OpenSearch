/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common.tiering;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.WarmNodeDiskThresholdEvaluator;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.remote.filecache.FileCacheSettings;
import org.opensearch.indices.ShardLimitValidator;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.opensearch.index.IndexModule.INDEX_TIERING_STATE;
import static org.opensearch.index.IndexModule.TieringState.HOT_TO_WARM;
import static org.opensearch.index.IndexModule.TieringState.WARM;
import static org.opensearch.storage.common.tiering.TieringServiceValidator.validateEligibleHotNodesCapacity;
import static org.opensearch.storage.common.tiering.TieringUtils.Tier.HOT;
import static org.mockito.Mockito.mock;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class TieringServiceValidatorTests extends OpenSearchTestCase {

    private ClusterState clusterState;
    private Index testIndex;
    private IndexMetadata indexMetadata;
    private ClusterInfo clusterInfo;
    private ShardLimitValidator shardLimitValidator;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        testIndex = new Index("test-index", "uuid");
        shardLimitValidator = mock(ShardLimitValidator.class);
    }

    public void testValidateCommon_SuccessfulHotToWarmTransition() {
        setupClusterState(1, HOT.toString(), true);
        setupClusterInfo(1, 100, 50, 10L, 50, 20);
        TieringServiceValidator.validateCommon(clusterState, clusterInfo, testIndex, 2, 3, 99, WARM, HOT_TO_WARM, shardLimitValidator);
    }

    public void testValidateCommon_FailsWithNoWarmNodes() {
        setupClusterState(0, HOT.toString(), true);
        setupClusterInfo(1, 100, 50, 10L, 50, 20);
        TieringRejectionException e = expectThrows(
            TieringRejectionException.class,
            () -> TieringServiceValidator.validateCommon(
                clusterState,
                clusterInfo,
                testIndex,
                1,
                3,
                90,
                WARM,
                HOT_TO_WARM,
                shardLimitValidator
            )
        );
        assertTrue(e.getMessage().contains("no nodes found with the warm role"));
    }

    public void testValidateCommon_FailsWithIncorrectInitialState() {
        setupClusterState(1, WARM.toString(), true);
        setupClusterInfo(1, 100, 50, 10L, 50, 20);
        expectThrows(
            TieringRejectionException.class,
            () -> TieringServiceValidator.validateCommon(
                clusterState,
                clusterInfo,
                testIndex,
                1,
                3,
                90,
                WARM,
                HOT_TO_WARM,
                shardLimitValidator
            )
        );
    }

    public void testValidateCommon_FailsWithMaxConcurrentRequestsBreached() {
        setupClusterState(1, HOT.toString(), true);
        setupClusterInfo(1, 100, 50, 10L, 50, 20);
        expectThrows(
            TieringRejectionException.class,
            () -> TieringServiceValidator.validateCommon(
                clusterState,
                clusterInfo,
                testIndex,
                4,
                4,
                90,
                WARM,
                HOT_TO_WARM,
                shardLimitValidator
            )
        );
    }

    public void testValidateCommon_FailsWithRemoteStoreDisabled() {
        setupClusterStateWithRemoteStore(1, HOT.toString(), true, false);
        setupClusterInfo(1, 100, 50, 10L, 50, 20);
        expectThrows(
            TieringRejectionException.class,
            () -> TieringServiceValidator.validateCommon(
                clusterState,
                clusterInfo,
                testIndex,
                1,
                3,
                90,
                WARM,
                HOT_TO_WARM,
                shardLimitValidator
            )
        );
    }

    public void testValidateCommon_FailsWithHighJVMUtilization() {
        setupClusterState(1, HOT.toString(), true);
        setupClusterInfo(1, 100, 50, 10L, 99, 20);
        expectThrows(
            TieringRejectionException.class,
            () -> TieringServiceValidator.validateCommon(
                clusterState,
                clusterInfo,
                testIndex,
                1,
                3,
                90,
                WARM,
                HOT_TO_WARM,
                shardLimitValidator
            )
        );
    }

    public void testValidateCommon_SucceedsWithLowJVMUtilization() {
        setupClusterState(1, HOT.toString(), true);
        setupClusterInfo(1, 100, 50, 10L, 40, 20);
        TieringServiceValidator.validateCommon(clusterState, clusterInfo, testIndex, 2, 5, 90, WARM, HOT_TO_WARM, shardLimitValidator);
    }

    public void testValidateEligibleNodesCapacityW2H_Success() {
        validateCapacity(100, 50, 20L, IndexModule.TieringState.HOT);
    }

    public void testValidateEligibleNodesCapacityW2H_Failure() {
        expectThrows(TieringRejectionException.class, () -> validateCapacity(100, 5, 20L, IndexModule.TieringState.HOT));
    }

    public void testValidateSpaceForLargestShard_Success() {
        setupClusterState(2, HOT.toString(), true);
        setupClusterInfo(2, 1000, 500, 10L, 50, 20);
        TieringServiceValidator.validateSpaceForLargestShard(clusterState, clusterInfo, testIndex, WARM, 20L * 1024 * 1024 * 1024L);
    }

    public void testValidateSpaceForLargestShard_Failure() {
        setupClusterState(1, HOT.toString(), true);
        setupClusterInfo(1, 100, 5, 50L, 50, 20);
        expectThrows(
            TieringRejectionException.class,
            () -> TieringServiceValidator.validateSpaceForLargestShard(
                clusterState,
                clusterInfo,
                testIndex,
                WARM,
                20L * 1024 * 1024 * 1024L
            )
        );
    }

    public void testValidateWarmNodeDiskThresholdWaterMarkLow_NoBreached() {
        DiskThresholdSettings diskThresholdSettings = diskThresholdSettings("150b", "100b", "50b");

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 100L);

        Map<String, DiskUsage> diskUsages = new HashMap<>();
        diskUsages.put("warm-0", new DiskUsage("warm-0", "warm-0", "/foo/bar", 2000, 1500));
        diskUsages.put("warm-1", new DiskUsage("warm-1", "warm-1", "/foo/bar", 2000, 1500));

        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test";
        Index idx = new Index(indexName, indexUuid);

        Map<Index, String> indicesNodeMap = new HashMap<>();
        indicesNodeMap.put(idx, "warm-0");

        ClusterState state = ClusterState.builder(buildClusterState(indexName, indexUuid, Settings.EMPTY, null))
            .nodes(createNodes(2, 0, 0))
            .build();

        ClusterInfo info = new ClusterInfo(diskUsages, diskUsages, shardSizes, null, Map.of(), Map.of(), null);

        TieringServiceValidator.validateWarmNodeDiskThresholdWaterMarkLow(
            state,
            info,
            new HashSet<>(),
            idx,
            new WarmNodeDiskThresholdEvaluator(diskThresholdSettings, fileCacheSettings(2.0)::getRemoteDataRatio)
        );
    }

    public void testValidateWarmNodeDiskThresholdWaterMarkLow_Breached() {
        DiskThresholdSettings diskThresholdSettings = diskThresholdSettings("10b", "10b", "5b");

        Map<String, Long> shardSizes = new HashMap<>();
        shardSizes.put("[test][0][p]", 2000L);

        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test";
        Index idx = new Index(indexName, indexUuid);

        ClusterState state = ClusterState.builder(buildClusterState(indexName, indexUuid, Settings.EMPTY, null))
            .nodes(createNodes(2, 0, 0))
            .build();

        ClusterInfo info = new ClusterInfo(Map.of(), Map.of(), shardSizes, null, Map.of(), Map.of(), Map.of());

        expectThrows(
            TieringRejectionException.class,
            () -> TieringServiceValidator.validateWarmNodeDiskThresholdWaterMarkLow(
                state,
                info,
                new HashSet<>(),
                idx,
                new WarmNodeDiskThresholdEvaluator(diskThresholdSettings, fileCacheSettings(1.0)::getRemoteDataRatio)
            )
        );
    }

    // --- Helper methods ---

    private void setupClusterState(int warmNodes, String tierState, boolean isHealthy) {
        setupClusterStateWithRemoteStore(warmNodes, tierState, isHealthy, true);
    }

    private void setupClusterStateWithRemoteStore(int warmNodes, String tierState, boolean isHealthy, boolean remoteStoreEnabled) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, testIndex.getUUID())
            .put(INDEX_TIERING_STATE.getKey(), tierState)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, remoteStoreEnabled)
            .build();

        indexMetadata = IndexMetadata.builder(testIndex.getName()).settings(indexSettings).numberOfShards(1).numberOfReplicas(1).build();

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.add(createRoutingTable(testIndex, "warm-0", isHealthy));

        clusterState = ClusterState.builder(buildClusterState("test-index", "na", Settings.EMPTY, null))
            .nodes(createNodes(warmNodes, 0, 0))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(routingTableBuilder.build())
            .build();
    }

    private void setupClusterInfo(
        int warmNodeCount,
        long totalBytes,
        long freeBytes,
        long shardSize,
        int jvmUtilPercent,
        int fileCachePercent
    ) {
        Map<String, Long> shardSizes = Collections.singletonMap("[test-index][0][p]", shardSize);
        clusterInfo = new ClusterInfo(
            diskUsages(warmNodeCount, 0, totalBytes, freeBytes),
            null,
            shardSizes,
            null,
            Map.of(),
            Map.of(),
            nodeResourceUsageStatsMap(warmNodeCount, 0, jvmUtilPercent)
        );
    }

    private void validateCapacity(long totalBytes, long freeBytes, long shardSize, IndexModule.TieringState targetTier) {
        String indexUuid = UUID.randomUUID().toString();
        String indexName = "test_index";
        Index idx = new Index(indexName, indexUuid);
        Set<Index> tieringIndices = Collections.singleton(idx);

        ClusterState state = ClusterState.builder(buildClusterState(indexName, indexUuid, Settings.EMPTY, null))
            .nodes(createNodes(2, 2, 0))
            .build();

        Map<String, Long> shardSizes = Collections.singletonMap("[test_index][0][p]", shardSize);
        ClusterInfo info = new ClusterInfo(
            diskUsages(2, 2, totalBytes, freeBytes),
            diskUsages(2, 2, totalBytes, freeBytes),
            shardSizes,
            null,
            Map.of(),
            Map.of(),
            null
        );

        validateEligibleHotNodesCapacity(state, info, tieringIndices, idx);
    }

    private IndexRoutingTable createRoutingTable(Index index, String nodeId, boolean isHealthy) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(index);
        ShardRouting primaryShard = TestShardRouting.newShardRouting(
            new ShardId(index, 0),
            isHealthy ? nodeId : null,
            true,
            isHealthy ? ShardRoutingState.STARTED : ShardRoutingState.UNASSIGNED
        );
        builder.addShard(primaryShard);
        return builder.build();
    }

    private static ClusterState buildClusterState(String indexName, String indexUuid, Settings settings, RoutingTable routingTable) {
        Settings combinedSettings = Settings.builder()
            .put(settings)
            .put("index.version.created", Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .build();

        Metadata metadata = Metadata.builder().put(IndexMetadata.builder(indexName).settings(combinedSettings)).build();
        if (routingTable == null) {
            routingTable = RoutingTable.builder().addAsNew(metadata.index(indexName)).build();
        }
        return ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(settings))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
    }

    private DiscoveryNodes createNodes(int numWarm, int numData, int numIngest) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < numWarm; i++) {
            builder.add(
                new DiscoveryNode(
                    "warm-" + i,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.WARM_ROLE),
                    Version.CURRENT
                )
            );
        }
        for (int i = 0; i < numData; i++) {
            builder.add(
                new DiscoveryNode(
                    "data-" + i,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
                    Version.CURRENT
                )
            );
        }
        return builder.build();
    }

    private static Map<String, DiskUsage> diskUsages(int warmNodes, int hotNodes, long totalBytes, long freeBytes) {
        Map<String, DiskUsage> usages = new HashMap<>();
        for (int i = 0; i < warmNodes; i++) {
            usages.put("warm-" + i, new DiskUsage("warm-" + i, "warm-" + i, "/foo/bar", totalBytes, freeBytes));
        }
        for (int i = 0; i < hotNodes; i++) {
            usages.put("data-" + i, new DiskUsage("data-" + i, "data-" + i, "/foo/bar", totalBytes, freeBytes));
        }
        return usages;
    }

    private static Map<String, NodeResourceUsageStats> nodeResourceUsageStatsMap(int warmNodes, int hotNodes, double jvmPercent) {
        Map<String, NodeResourceUsageStats> map = new HashMap<>();
        for (int i = 0; i < warmNodes; i++) {
            map.put("warm-" + i, new NodeResourceUsageStats("warm-" + i, System.currentTimeMillis(), jvmPercent, 0, null));
        }
        for (int i = 0; i < hotNodes; i++) {
            map.put("data-" + i, new NodeResourceUsageStats("data-" + i, System.currentTimeMillis(), jvmPercent, 0, null));
        }
        return map;
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

    private static FileCacheSettings fileCacheSettings(double ratio) {
        HashSet<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING);
        return new FileCacheSettings(
            Settings.builder().put(FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), ratio).build(),
            new ClusterSettings(Settings.EMPTY, settings)
        );
    }
}
