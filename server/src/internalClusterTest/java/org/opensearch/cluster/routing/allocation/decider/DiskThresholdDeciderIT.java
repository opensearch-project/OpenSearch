/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.apache.lucene.tests.mockfile.FilterFileStore;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.lucene.util.Constants;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexRequestBuilder;

import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.InternalClusterInfoService;
import org.opensearch.cluster.MockInternalClusterInfoService;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.Rebalance;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.snapshots.RestoreInfo;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;

import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.opensearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.opensearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskThresholdDeciderIT extends OpenSearchIntegTestCase {

    private static TestFileSystemProvider fileSystemProvider;

    private FileSystem defaultFileSystem;

    @Before
    public void installFilesystemProvider() {
        assertNull(defaultFileSystem);
        defaultFileSystem = PathUtils.getDefaultFileSystem();
        assertNull(fileSystemProvider);
        fileSystemProvider = new TestFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @After
    public void removeFilesystemProvider() {
        fileSystemProvider = null;
        assertNotNull(defaultFileSystem);
        PathUtilsForTesting.installMock(defaultFileSystem); // set the default filesystem back
        defaultFileSystem = null;
    }

    private static final long WATERMARK_BYTES = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();
    private static final long TOTAL_SPACE_BYTES = new ByteSizeValue(100, ByteSizeUnit.KB).getBytes();
    private static final String INDEX_ROUTING_ALLOCATION_NODE_SETTING = "index.routing.allocation.include._name";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Path dataPath = fileSystemProvider.getRootDir().resolve("node-" + nodeOrdinal);
        try {
            Files.createDirectories(dataPath);
        } catch (IOException e) {
            throw new AssertionError("unexpected", e);
        }
        fileSystemProvider.addTrackedPath(dataPath);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Environment.PATH_DATA_SETTING.getKey(), dataPath)
            .put(FsService.ALWAYS_REFRESH_SETTING.getKey(), true)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class, MockInternalClusterInfoService.TestPlugin.class);
    }

    public void testHighWatermarkNotExceeded() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        final InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) internalCluster()
            .getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
        internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class).addListener(event -> clusterInfoService.refresh());

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();
        final Path dataNode0Path = internalCluster().getInstance(Environment.class, dataNodeName).dataFiles()[0];

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final long minShardSize = createAndPopulateIndex(indexName, null);

        // reduce disk size of node 0 so that no shards fit below the high watermark, forcing all shards onto the other
        // data node (subtract the translog size since the disk threshold decider ignores this and may therefore move
        // the shard back again).
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, empty());

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, hasSize(1));
    }

    public void testIndexCreateBlockWhenAllNodesExceededHighWatermark() throws Exception {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNodes(2, settings);
        ensureStableCluster(3);
        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        // Reduce disk space of all node until all of them is breaching high disk watermark.
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, WATERMARK_BYTES - 1)
        );
        assertBusy(() -> {
            ClusterState state1 = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertFalse(state1.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);
    }

    public void testIndexCreateBlockNotAppliedWhenAnyNodesBelowHighWatermark() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        ensureStableCluster(3);

        final InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) internalCluster()
            .getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
        internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class).addListener(event -> clusterInfoService.refresh());

        // Validate cluster block is not applied on the cluster
        ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
        assertFalse(state.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
    }

    public void testIndexCreateBlockIsRemovedWhenAnyNodesNotExceedHighWatermarkWithAutoReleaseEnabled() throws Exception {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNodes(2, settings);
        ensureStableCluster(3);

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        // Reduce disk space of all node until all of them is breaching high disk watermark.
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, WATERMARK_BYTES - 1)
        );

        // Validate if cluster block is applied on the cluster
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);

        // Free all the space
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, TOTAL_SPACE_BYTES)
        );

        // Validate if index create block is removed on the cluster
        assertBusy(() -> {
            ClusterState state1 = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertFalse(state1.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);
    }

    public void testIndexCreateBlockIsRemovedWhenAnyNodesNotExceedHighWatermarkWithAutoReleaseDisabled() throws Exception {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE.getKey(), false)
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNodes(2, settings);
        ensureStableCluster(3);

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        // Reduce disk space of all node until all of them is breaching high disk watermark
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, WATERMARK_BYTES - 1)
        );

        // Validate if cluster block is applied on the cluster
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);

        // Free all the space
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, TOTAL_SPACE_BYTES)
        );

        // Validate index create block is not removed on the cluster
        assertBusy(() -> {
            ClusterState state1 = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state1.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);
    }

    public void testDiskMonitorAppliesBlockBackWhenUserRemovesIndexCreateBlock() throws Exception {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE.getKey(), false)
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNodes(2, settings);
        ensureStableCluster(3);

        // User applies index create block.
        Settings createBlockSetting = Settings.builder().put(Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING.getKey(), "true").build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(createBlockSetting).get());
        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        // Reduce disk space of all node until all of them is breaching high disk watermark and DiskMonitor applies block.
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, WATERMARK_BYTES - 1)
        );

        // Validate if cluster block is applied on the cluster
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);

        // User removes the block.
        Settings removeBlockSetting = Settings.builder().put(Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING.getKey(), false).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(removeBlockSetting).get());

        // Refresh so that DiskThresholdMonitor kicks in and applies block.
        getMockInternalClusterInfoService().refresh();
        // Validate index create block is not removed on the cluster
        assertBusy(() -> {
            ClusterState state1 = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state1.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);
    }

    public void testIndexCreateBlockWithAReadOnlyBlock() throws Exception {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        final List<String> dataNodeNames = internalCluster().startDataOnlyNodes(2, settings);
        ensureStableCluster(3);

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        // Create one of the index.
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(indexName, dataNodeNames.get(0));
        // Apply a read_only_allow_delete_block on one of the index
        // (can happen if the corresponding node has breached flood stage watermark).
        final Settings readOnlySettings = Settings.builder()
            .put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, Boolean.TRUE.toString())
            .build();
        client().admin().indices().prepareUpdateSettings(indexName).setSettings(readOnlySettings).get();

        // Reduce disk space of all node until all of them is breaching high disk watermark.
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, WATERMARK_BYTES - 1)
        );

        // Validate index create block is applied on the cluster
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);
    }

    public void testRestoreSnapshotAllocationDoesNotExceedWatermark() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository("repo")
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()))
        );

        final InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) internalCluster()
            .getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
        internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class).addListener(event -> clusterInfoService.refresh());

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();
        final Path dataNode0Path = internalCluster().getInstance(Environment.class, dataNodeName).dataFiles()[0];

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final long minShardSize = createAndPopulateIndex(indexName, null);

        final CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot("repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertAcked(client().admin().indices().prepareDelete(indexName).get());

        // reduce disk size of node 0 so that no shards fit below the low watermark, forcing shards to be assigned to the other data node
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        refreshDiskUsage();

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString())
                        .build()
                )
                .get()
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot("repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), is(0));

        assertBusy(() -> assertThat(getShardRoutings(dataNode0Id, indexName), empty()));

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey()).build()
                )
                .get()
        );

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        fileSystemProvider.getTestFileStore(dataNode0Path).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, hasSize(1));
    }

    public void testDiskMonitorResetLastRuntimeMilliSecOnlyInFirstCall() throws Exception {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_CREATE_INDEX_BLOCK_AUTO_RELEASE.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        internalCluster().startDataOnlyNodes(2, settings);
        ensureStableCluster(3);

        final MockInternalClusterInfoService clusterInfoService = getMockInternalClusterInfoService();
        // Reduce disk space of all node.
        clusterInfoService.setDiskUsageFunctionAndRefresh((discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, 0));

        // Validate if cluster block is applied on the cluster
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertTrue(state.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);

        // User removes index create block.
        Settings removeBlockSetting = Settings.builder().put(Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING.getKey(), "false").build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(removeBlockSetting).get());
        // Free all the space
        clusterInfoService.setDiskUsageFunctionAndRefresh(
            (discoveryNode, fsInfoPath) -> setDiskUsage(fsInfoPath, TOTAL_SPACE_BYTES, TOTAL_SPACE_BYTES)
        );

        // Validate index create block is removed on the cluster
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().setLocal(true).get().getState();
            assertFalse(state.blocks().hasGlobalBlockWithId(Metadata.CLUSTER_CREATE_INDEX_BLOCK.id()));
        }, 30L, TimeUnit.SECONDS);
    }

    private String populateNode(final String dataNodeName) throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(indexName, dataNodeName);
        return indexName;
    }

    private long createAndPopulateIndex(final String indexName, final String nodeName) throws Exception {

        final Settings.Builder indexSettingBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
            .put(IndexSettings.INDEX_MERGE_ON_FLUSH_ENABLED.getKey(), false);

        // Depending on node name specified or not, we determine whether to enable node name based shard routing for index
        // and whether reallocation is disabled on that index or not.
        if (nodeName != null) {
            indexSettingBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(INDEX_ROUTING_ALLOCATION_NODE_SETTING, nodeName);
            createIndex(indexName, indexSettingBuilder.build());
            assertAcked(
                client().admin()
                    .indices()
                    .updateSettings(
                        new UpdateSettingsRequest(indexName).settings(
                            Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none")
                        )
                    )
                    .get()
            );

            ensureGreen(indexName);
        } else {
            indexSettingBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6);
            createIndex(indexName, indexSettingBuilder.build());
        }

        return createReasonableSizedShards(indexName);
    }

    private Set<ShardRouting> getShardRoutings(final String nodeId, final String indexName) {
        final Set<ShardRouting> shardRoutings = new HashSet<>();
        for (IndexShardRoutingTable indexShardRoutingTable : client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setRoutingTable(true)
            .get()
            .getState()
            .getRoutingTable()
            .index(indexName)) {
            for (ShardRouting shard : indexShardRoutingTable.shards()) {
                assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
                if (shard.currentNodeId().equals(nodeId)) {
                    shardRoutings.add(shard);
                }
            }
        }
        return shardRoutings;
    }

    /**
     * Index documents until all the shards are at least WATERMARK_BYTES in size, and return the size of the smallest shard.
     */
    private long createReasonableSizedShards(final String indexName) throws InterruptedException {
        while (true) {
            final IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[scaledRandomIntBetween(100, 10000)];
            for (int i = 0; i < indexRequestBuilders.length; i++) {
                indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", randomAlphaOfLength(10));
            }
            indexRandom(true, indexRequestBuilders);
            forceMerge();
            refresh();

            final ShardStats[] shardStatses = client().admin()
                .indices()
                .prepareStats(indexName)
                .clear()
                .setStore(true)
                .setTranslog(true)
                .get()
                .getShards();
            final long[] shardSizes = new long[shardStatses.length];
            for (ShardStats shardStats : shardStatses) {
                shardSizes[shardStats.getShardRouting().id()] = shardStats.getStats().getStore().sizeInBytes();
            }

            final long minShardSize = Arrays.stream(shardSizes).min().orElseThrow(() -> new AssertionError("no shards"));
            if (minShardSize > WATERMARK_BYTES) {
                return minShardSize;
            }
        }
    }

    private static FsInfo.Path setDiskUsage(FsInfo.Path original, long totalBytes, long freeBytes) {
        return new FsInfo.Path(original.getPath(), original.getMount(), totalBytes, freeBytes, freeBytes);
    }

    private void refreshDiskUsage() {
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
        ((InternalClusterInfoService) clusterInfoService).refresh();
        // if the nodes were all under the low watermark already (but unbalanced) then a change in the disk usage doesn't trigger a reroute
        // even though it's now possible to achieve better balance, so we have to do an explicit reroute. TODO fix this?
        if (StreamSupport.stream(clusterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().values().spliterator(), false)
            .allMatch(cur -> cur.value.getFreeBytes() > WATERMARK_BYTES)) {
            assertAcked(client().admin().cluster().prepareReroute());
        }

        assertFalse(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .setWaitForNoInitializingShards(true)
                .get()
                .isTimedOut()
        );
    }

    private void assertBusyWithDiskUsageRefresh(String nodeName, String indexName, Matcher<? super Set<ShardRouting>> matcher)
        throws Exception {
        assertBusy(() -> {
            // refresh the cluster-manager's ClusterInfoService before checking the assigned shards because DiskThresholdMonitor might still
            // be processing a previous ClusterInfo update and will skip the new one (see DiskThresholdMonitor#onNewInfo(ClusterInfo)
            // and its internal checkInProgress flag)
            refreshDiskUsage();

            final Set<ShardRouting> shardRoutings = getShardRoutings(nodeName, indexName);
            assertThat("Mismatching shard routings: " + shardRoutings, shardRoutings, matcher);
        }, 30L, TimeUnit.SECONDS);
    }

    private MockInternalClusterInfoService getMockInternalClusterInfoService() {
        return (MockInternalClusterInfoService) internalCluster().getCurrentClusterManagerNodeInstance(ClusterInfoService.class);
    }

    private static class TestFileStore extends FilterFileStore {

        private final Path path;

        private volatile long totalSpace = -1;

        TestFileStore(FileStore delegate, String scheme, Path path) {
            super(delegate, scheme);
            this.path = path;
        }

        @Override
        public String name() {
            return "fake"; // Lucene's is-spinning-disk check expects the device name here
        }

        @Override
        public long getTotalSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getTotalSpace();
            } else {
                return totalSpace;
            }
        }

        public void setTotalSpace(long totalSpace) {
            assertThat(totalSpace, anyOf(is(-1L), greaterThan(0L)));
            this.totalSpace = totalSpace;
        }

        @Override
        public long getUsableSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getUsableSpace();
            } else {
                return Math.max(0L, totalSpace - getTotalFileSize(path));
            }
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            final long totalSpace = this.totalSpace;
            if (totalSpace == -1) {
                return super.getUnallocatedSpace();
            } else {
                return Math.max(0L, totalSpace - getTotalFileSize(path));
            }
        }

        private static long getTotalFileSize(Path path) throws IOException {
            if (Files.isRegularFile(path)) {
                try {
                    return Files.size(path);
                } catch (NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            } else if (path.getFileName().toString().equals("_state") || path.getFileName().toString().equals("translog")) {
                // ignore metadata and translog, since the disk threshold decider only cares about the store size
                return 0L;
            } else {
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path)) {
                    long total = 0L;
                    for (Path subpath : directoryStream) {
                        total += getTotalFileSize(subpath);
                    }
                    return total;
                } catch (NotDirectoryException | NoSuchFileException | FileNotFoundException e) {
                    // probably removed
                    return 0L;
                }
            }
        }
    }

    private static class TestFileSystemProvider extends FilterFileSystemProvider {
        private final Map<Path, TestFileStore> trackedPaths = newConcurrentMap();
        private final Path rootDir;

        TestFileSystemProvider(FileSystem delegateInstance, Path rootDir) {
            super("diskthreshold://", delegateInstance);
            this.rootDir = new FilterPath(rootDir, fileSystem);
        }

        Path getRootDir() {
            return rootDir;
        }

        void addTrackedPath(Path path) {
            assertTrue(path + " starts with " + rootDir, path.startsWith(rootDir));
            final FileStore fileStore;
            try {
                fileStore = super.getFileStore(path);
            } catch (IOException e) {
                throw new AssertionError("unexpected", e);
            }
            assertNull(trackedPaths.put(path, new TestFileStore(fileStore, getScheme(), path)));
        }

        @Override
        public FileStore getFileStore(Path path) {
            return getTestFileStore(path);
        }

        TestFileStore getTestFileStore(Path path) {
            if (path.endsWith(path.getFileSystem().getPath("nodes", "0"))) {
                path = path.getParent().getParent();
            }
            final TestFileStore fileStore = trackedPaths.get(path);
            if (fileStore != null) {
                return fileStore;
            }

            // On Linux, and only Linux, Lucene obtains a filestore for the index in order to determine whether it's on a spinning disk or
            // not so it can configure the merge scheduler accordingly
            assertTrue(path + " not tracked and not on Linux", Constants.LINUX);
            final Set<Path> containingPaths = trackedPaths.keySet().stream().filter(path::startsWith).collect(Collectors.toSet());
            assertThat(path + " not contained in a unique tracked path", containingPaths, hasSize(1));
            return trackedPaths.get(containingPaths.iterator().next());
        }
    }
}
