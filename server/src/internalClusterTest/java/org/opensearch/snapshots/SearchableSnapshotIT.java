/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.snapshots;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsGroup;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCacheStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.node.Node;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.transport.client.Client;
import org.hamcrest.MatcherAssert;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.FS;
import static org.opensearch.core.common.util.CollectionUtils.iterableAsArrayList;
import static org.opensearch.index.store.remote.filecache.FileCacheSettings.DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING;
import static org.opensearch.test.NodeRoles.clusterManagerOnlyNode;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.NodeRoles.onlyRole;
import static org.opensearch.test.NodeRoles.onlyRoles;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public final class SearchableSnapshotIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings.Builder randomRepositorySettings() {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", randomRepoPath()).put("compress", randomBoolean());
        settings.put(FsRepository.BASE_PATH_SETTING.getKey(), "my_base_path");
        return settings;
    }

    private Settings.Builder chunkedRepositorySettings(long chunkSize) {
        final Settings.Builder settings = Settings.builder();
        settings.put("location", randomRepoPath()).put("compress", randomBoolean());
        settings.put("chunk_size", chunkSize, ByteSizeUnit.BYTES);
        return settings;
    }

    /**
     * Tests a happy path scenario for searchable snapshots by creating 2 indices,
     * taking a snapshot, restoring them as searchable snapshots.
     * Ensures availability of sufficient data nodes and warm capable nodes.
     */
    public void testCreateSearchableSnapshot() throws Exception {
        final String snapshotName = "test-snap";
        final String repoName = "test-repo";
        final String indexName1 = "test-idx-1";
        final String restoredIndexName1 = indexName1 + "-copy";
        final String indexName2 = "test-idx-2";
        final String restoredIndexName2 = indexName2 + "-copy";
        final int numReplicasIndex1 = randomIntBetween(1, 4);
        final int numReplicasIndex2 = randomIntBetween(0, 2);
        final Client client = client();

        internalCluster().ensureAtLeastNumDataNodes(Math.max(numReplicasIndex1, numReplicasIndex2) + 1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex1, 100, indexName1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex2, 100, indexName2);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName1, indexName2);
        deleteIndicesAndEnsureGreen(client, indexName1, indexName2);

        internalCluster().ensureAtLeastNumWarmNodes(Math.max(numReplicasIndex1, numReplicasIndex2) + 1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName1, restoredIndexName2);

        assertDocCount(restoredIndexName1, 100L);
        assertDocCount(restoredIndexName2, 100L);
        assertIndexDirectoryDoesNotExist(restoredIndexName1, restoredIndexName2);
    }

    public void testSnapshottingSearchableSnapshots() throws Exception {
        final String repoName = "test-repo";
        final String initSnapName = "initial-snapshot";
        final String indexName = "test-idx";
        final String repeatSnapNamePrefix = "test-repeated-snap-";
        final String repeatIndexNamePrefix = indexName + "-copy-";
        final Client client = client();

        // create an index, add data, snapshot it, then delete it
        internalCluster().ensureAtLeastNumDataNodes(1);
        createIndexWithDocsAndEnsureGreen(0, 100, indexName);
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, initSnapName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        // restore the index as a searchable snapshot
        internalCluster().ensureAtLeastNumWarmNodes(1);
        client.admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, initSnapName)
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy-0")
            .setStorageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        ensureGreen();
        assertDocCount(indexName + "-copy-0", 100L);
        assertIndexDirectoryDoesNotExist(indexName + "-copy-0");

        // Test that the searchable snapshot index can continue to be snapshotted and restored
        for (int i = 0; i < 4; i++) {
            final String repeatedSnapshotName = repeatSnapNamePrefix + i;
            takeSnapshot(client, repeatedSnapshotName, repoName);
            deleteIndicesAndEnsureGreen(client, "_all");
            client.admin()
                .cluster()
                .prepareRestoreSnapshot(repoName, repeatedSnapshotName)
                .setRenamePattern("([a-z-]+).*")
                .setRenameReplacement("$1" + (i + 1))
                .setWaitForCompletion(true)
                .execute()
                .actionGet();
            ensureGreen();
            final String restoredIndexName = indexName + "-copy-" + (i + 1);
            assertDocCount(restoredIndexName, 100L);
            assertIndexDirectoryDoesNotExist(restoredIndexName);
        }
        // Assert all the snapshots exist. Note that AbstractSnapshotIntegTestCase::assertRepoConsistency
        // will run after this test (and all others) and assert on the consistency of the data in the repo.
        final GetSnapshotsResponse response = client.admin().cluster().prepareGetSnapshots(repoName).execute().actionGet();
        final Map<String, List<String>> snapshotInfoMap = response.getSnapshots()
            .stream()
            .collect(Collectors.toMap(s -> s.snapshotId().getName(), SnapshotInfo::indices));
        final Map<String, List<String>> expect = new HashMap<>();
        expect.put(initSnapName, List.of(indexName));
        IntStream.range(0, 4).forEach(i -> expect.put(repeatSnapNamePrefix + i, List.of(repeatIndexNamePrefix + i)));
        assertEquals(expect, snapshotInfoMap);

        String[] snapNames = new String[5];
        IntStream.range(0, 4).forEach(i -> snapNames[i] = repeatSnapNamePrefix + i);
        snapNames[4] = initSnapName;
        SnapshotsStatusResponse snapshotsStatusResponse = client.admin()
            .cluster()
            .prepareSnapshotStatus(repoName)
            .addSnapshots(snapNames)
            .execute()
            .actionGet();
        snapshotsStatusResponse.getSnapshots().forEach(s -> {
            String snapName = s.getSnapshot().getSnapshotId().getName();
            assertEquals(1, s.getIndices().size());
            assertEquals(1, s.getShards().size());
            if (snapName.equals("initial-snapshot")) {
                assertNotNull(s.getIndices().get("test-idx"));
                assertTrue(s.getShards().get(0).getStats().getTotalFileCount() > 0);
            } else {
                assertTrue(snapName.startsWith(repeatSnapNamePrefix));
                assertEquals(1, s.getIndices().size());
                assertNotNull(s.getIndices().get(repeatIndexNamePrefix + snapName.substring(repeatSnapNamePrefix.length())));
                assertEquals(0L, s.getShards().get(0).getStats().getTotalFileCount());
            }
        });
    }

    /**
     * Tests a default 8mib chunked repository scenario for searchable snapshots by creating an index,
     * taking a snapshot, restoring it as a searchable snapshot index.
     */
    public void testCreateSearchableSnapshotWithDefaultChunks() throws Exception {
        final int numReplicasIndex = randomIntBetween(1, 4);
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        Settings.Builder repositorySettings = chunkedRepositorySettings(2 << 23);

        internalCluster().ensureAtLeastNumWarmAndDataNodes(numReplicasIndex + 1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex, 1000, indexName);
        createRepositoryWithSettings(repositorySettings, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);

        deleteIndicesAndEnsureGreen(client, indexName);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        assertDocCount(restoredIndexName, 1000L);
    }

    /**
     * Tests a small 1000 bytes chunked repository scenario for searchable snapshots by creating an index,
     * taking a snapshot, restoring it as a searchable snapshot index.
     */
    public void testCreateSearchableSnapshotWithSmallChunks() throws Exception {
        final int numReplicasIndex = randomIntBetween(1, 4);
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        Settings.Builder repositorySettings = chunkedRepositorySettings(1000);

        internalCluster().ensureAtLeastNumWarmAndDataNodes(numReplicasIndex + 1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex, 1000, indexName);
        createRepositoryWithSettings(repositorySettings, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);

        deleteIndicesAndEnsureGreen(client, indexName);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        assertDocCount(restoredIndexName, 1000L);
    }

    /**
     * Tests the functionality of remote shard allocation to
     * ensure it can assign remote shards to a node with local shards given it has the
     * warm role capabilities.
     */
    public void testSearchableSnapshotAllocationForLocalAndRemoteShardsOnSameNode() throws Exception {
        final int numReplicasIndex = randomIntBetween(1, 4);
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        internalCluster().ensureAtLeastNumWarmAndDataNodes(numReplicasIndex + 1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex, 100, indexName);
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);

        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        assertDocCount(restoredIndexName, 100L);
        assertDocCount(indexName, 100L);
    }

    public void testSearchableSnapshotAllocationFilterSettings() throws Exception {
        final int numShardsIndex = randomIntBetween(3, 6);
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        internalCluster().ensureAtLeastNumWarmAndDataNodes(numShardsIndex);
        createIndexWithDocsAndEnsureGreen(numShardsIndex, 1, 100, indexName);
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);

        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);
        final Set<String> warmNodes = StreamSupport.stream(clusterService().state().getNodes().spliterator(), false)
            .filter(DiscoveryNode::isWarmNode)
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());

        for (int i = warmNodes.size(); i > 2; --i) {
            String pickedNode = randomFrom(warmNodes);
            warmNodes.remove(pickedNode);
            assertIndexAssignedToNodeOrNot(restoredIndexName, pickedNode, true);
            assertTrue(
                client.admin()
                    .indices()
                    .prepareUpdateSettings(restoredIndexName)
                    .setSettings(Settings.builder().put("index.routing.allocation.exclude._id", pickedNode))
                    .execute()
                    .actionGet()
                    .isAcknowledged()
            );
            ClusterHealthResponse clusterHealthResponse = client.admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .setTimeout(new TimeValue(5, TimeUnit.MINUTES))
                .execute()
                .actionGet();
            assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
            assertIndexAssignedToNodeOrNot(restoredIndexName, pickedNode, false);
            assertIndexAssignedToNodeOrNot(indexName, pickedNode, true);
        }
    }

    private void assertIndexAssignedToNodeOrNot(String index, String node, boolean assigned) {
        final ClusterState state = clusterService().state();
        if (assigned) {
            assertTrue(state.getRoutingTable().allShards(index).stream().anyMatch(shard -> shard.currentNodeId().equals(node)));
        } else {
            assertTrue(state.getRoutingTable().allShards(index).stream().noneMatch(shard -> shard.currentNodeId().equals(node)));
        }
    }

    /**
     * Tests the functionality of remote shard allocation to
     * ensure it can handle node drops for failover scenarios and the cluster gets back to a healthy state when
     * nodes with warm capabilities are added back to the cluster.
     */
    public void testSearchableSnapshotAllocationForFailoverAndRecovery() throws Exception {
        final int numReplicasIndex = 1;
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        internalCluster().ensureAtLeastNumDataNodes(numReplicasIndex + 1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex, 100, indexName);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        internalCluster().ensureAtLeastNumWarmNodes(numReplicasIndex + 1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);
        assertDocCount(restoredIndexName, 100L);

        logger.info("--> stop a random warm node");
        internalCluster().stopRandomWarmNode();
        ensureYellow(restoredIndexName);
        assertDocCount(restoredIndexName, 100L);

        logger.info("--> stop the last warm node");
        internalCluster().stopRandomWarmNode();
        ensureRed(restoredIndexName);

        logger.info("--> add 3 new warm nodes");
        internalCluster().ensureAtLeastNumWarmNodes(numReplicasIndex + 2);
        ensureGreen(restoredIndexName);
        assertDocCount(restoredIndexName, 100);

        logger.info("--> stop a random warm node");
        internalCluster().stopRandomWarmNode();
        ensureGreen(restoredIndexName);
        assertDocCount(restoredIndexName, 100);
    }

    /**
     * Tests the functionality of index write block on a searchable snapshot index.
     */
    public void testSearchableSnapshotIndexIsReadOnly() throws Exception {
        final String indexName = "test-index";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        createIndexWithDocsAndEnsureGreen(0, 100, indexName);
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        internalCluster().ensureAtLeastNumWarmNodes(1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        assertIndexingBlocked(restoredIndexName);
        assertTrue(client.admin().indices().prepareDelete(restoredIndexName).get().isAcknowledged());
        assertThrows(
            "Expect index to not exist",
            IndexNotFoundException.class,
            () -> client.admin().indices().prepareGetIndex().setIndices(restoredIndexName).execute().actionGet()
        );
    }

    public void testDeleteSearchableSnapshotBackingIndexThrowsException() throws Exception {
        final String indexName = "test-index";
        final Client client = client();
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        createRepositoryWithSettings(null, repoName);
        createIndexWithDocsAndEnsureGreen(0, 100, indexName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        internalCluster().ensureAtLeastNumWarmNodes(1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertThrows(
            SnapshotInUseDeletionException.class,
            () -> client().admin().cluster().deleteSnapshot(new DeleteSnapshotRequest(repoName, snapshotName)).actionGet()
        );
    }

    public void testDeleteSearchableSnapshotBackingIndex() throws Exception {
        final String indexName1 = "test-index1";
        final String indexName2 = "test-index2";
        final Client client = client();
        final String repoName = "test-repo";
        final String snapshotName1 = "test-snapshot1";
        final String snapshotName2 = "test-snap";
        createRepositoryWithSettings(null, repoName);
        createIndexWithDocsAndEnsureGreen(0, 100, indexName1);
        createIndexWithDocsAndEnsureGreen(0, 100, indexName2);
        takeSnapshot(client, snapshotName1, repoName, indexName1);
        takeSnapshot(client, snapshotName2, repoName, indexName2);
        internalCluster().ensureAtLeastNumWarmNodes(1);
        restoreSnapshotAndEnsureGreen(client, snapshotName2, repoName);
        client().admin().cluster().deleteSnapshot(new DeleteSnapshotRequest(repoName, snapshotName1)).actionGet();
    }

    private void createIndexWithDocsAndEnsureGreen(int numReplicasIndex, int numOfDocs, String indexName) throws InterruptedException {
        createIndexWithDocsAndEnsureGreen(1, numReplicasIndex, numOfDocs, indexName);
    }

    private void createIndexWithDocsAndEnsureGreen(int numShardsIndex, int numReplicasIndex, int numOfDocs, String indexName)
        throws InterruptedException {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicasIndex)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsIndex)
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey())
                .build()
        );
        ensureGreen();

        indexRandomDocs(indexName, numOfDocs);
        ensureGreen();
    }

    private void takeSnapshot(Client client, String snapshotName, String repoName, String... indices) {
        logger.info("--> Take a snapshot");
        final CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indices)
            .get();

        MatcherAssert.assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        MatcherAssert.assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
    }

    private void createRepositoryWithSettings(Settings.Builder repositorySettings, String repoName) {
        logger.info("--> Create a repository");
        if (repositorySettings == null) {
            createRepository(repoName, FsRepository.TYPE);
        } else {
            createRepository(repoName, FsRepository.TYPE, repositorySettings);
        }
    }

    private void deleteIndicesAndEnsureGreen(Client client, String... indices) {
        assertTrue(client.admin().indices().prepareDelete(indices).get().isAcknowledged());
        ensureGreen();
    }

    private void restoreSnapshotAndEnsureGreen(Client client, String snapshotName, String repoName) {
        logger.info("--> restore indices as 'remote_snapshot'");
        client.admin()
            .cluster()
            .prepareRestoreSnapshot(repoName, snapshotName)
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy")
            .setStorageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        ensureGreen();
    }

    private void assertRemoteSnapshotIndexSettings(Client client, String... snapshotIndexNames) {
        GetSettingsResponse settingsResponse = client.admin()
            .indices()
            .getSettings(new GetSettingsRequest().indices(snapshotIndexNames))
            .actionGet();
        assertEquals(snapshotIndexNames.length, settingsResponse.getIndexToSettings().keySet().size());
        for (String snapshotIndexName : snapshotIndexNames) {
            assertEquals(
                IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey(),
                settingsResponse.getSetting(snapshotIndexName, IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
            );
        }
    }

    private void assertIndexingBlocked(String index) {
        try {
            final IndexRequestBuilder builder = client().prepareIndex(index);
            builder.setSource("foo", "bar");
            builder.execute().actionGet();
            fail("Expected operation to throw an exception");
        } catch (ClusterBlockException e) {
            MatcherAssert.assertThat(e.blocks(), contains(IndexMetadata.REMOTE_READ_ONLY_ALLOW_DELETE));
        }
    }

    public void testUpdateIndexSettings() throws InterruptedException {
        final String indexName = "test-index";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        createIndexWithDocsAndEnsureGreen(0, 100, indexName);
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        internalCluster().ensureAtLeastNumWarmNodes(1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        testUpdateIndexSettingsOnlyNotAllowedSettings(restoredIndexName);
        testUpdateIndexSettingsOnlyAllowedSettings(restoredIndexName);
        testUpdateIndexSettingsAtLeastOneNotAllowedSettings(restoredIndexName);
    }

    private void testUpdateIndexSettingsOnlyNotAllowedSettings(String index) {
        try {
            final UpdateSettingsRequestBuilder builder = client().admin().indices().prepareUpdateSettings(index);
            builder.setSettings(Map.of("index.refresh_interval", 10));
            builder.execute().actionGet();
            fail("Expected operation to throw an exception");
        } catch (ClusterBlockException e) {
            MatcherAssert.assertThat(e.blocks(), contains(IndexMetadata.REMOTE_READ_ONLY_ALLOW_DELETE));
        }
    }

    private void testUpdateIndexSettingsOnlyAllowedSettings(String index) {
        final UpdateSettingsRequestBuilder builder = client().admin().indices().prepareUpdateSettings(index);
        builder.setSettings(
            Map.of("index.max_result_window", 1000, "index.search.slowlog.threshold.query.warn", "10s", "index.number_of_replicas", 0)
        );
        AcknowledgedResponse settingsResponse = builder.execute().actionGet();
        assertThat(settingsResponse, notNullValue());
    }

    private void testUpdateIndexSettingsAtLeastOneNotAllowedSettings(String index) {
        try {
            final UpdateSettingsRequestBuilder builder = client().admin().indices().prepareUpdateSettings(index);
            builder.setSettings(
                Map.of("index.max_result_window", 5000, "index.search.slowlog.threshold.query.warn", "15s", "index.refresh_interval", 10)
            );
            builder.execute().actionGet();
            fail("Expected operation to throw an exception");
        } catch (ClusterBlockException e) {
            MatcherAssert.assertThat(e.blocks(), contains(IndexMetadata.REMOTE_READ_ONLY_ALLOW_DELETE));
        }
    }

    public void testFileCacheStats() throws Exception {
        final String snapshotName = "test-snap";
        final String repoName = "test-repo";
        final String indexName1 = "test-idx-1";
        final Client client = client();
        final int numNodes = 2;

        internalCluster().ensureAtLeastNumDataNodes(numNodes);
        createIndexWithDocsAndEnsureGreen(1, 100, indexName1);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName1);
        deleteIndicesAndEnsureGreen(client, indexName1);
        assertAllNodesFileCacheEmpty();

        internalCluster().ensureAtLeastNumWarmNodes(numNodes);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertNodesFileCacheNonEmpty(numNodes);
    }

    /**
     * Tests file cache restore scenario for searchable snapshots by creating an index,
     * taking a snapshot, restoring it as a searchable snapshot.
     * It ensures file cache is restored post node restart.
     */
    public void testFileCacheRestore() throws Exception {
        final String snapshotName = "test-snap";
        final String repoName = "test-repo";
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        // Keeping the replicas to 0 for reproducible cache results as shards can get reassigned otherwise
        final int numReplicasIndex = 0;
        final Client client = client();

        internalCluster().ensureAtLeastNumDataNodes(numReplicasIndex + 1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex, 100, indexName);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        internalCluster().ensureAtLeastNumWarmNodes(numReplicasIndex + 1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        assertDocCount(restoredIndexName, 100L);
        assertIndexDirectoryDoesNotExist(restoredIndexName);

        NodesStatsResponse preRestoreStats = client().admin().cluster().nodesStats(new NodesStatsRequest().all()).actionGet();
        for (NodeStats nodeStats : preRestoreStats.getNodes()) {
            if (nodeStats.getNode().isWarmNode()) {
                internalCluster().restartNode(nodeStats.getNode().getName());
            }
        }

        NodesStatsResponse postRestoreStats = client().admin().cluster().nodesStats(new NodesStatsRequest().all()).actionGet();
        Map<String, NodeStats> preRestoreStatsMap = preRestoreStats.getNodesMap();
        Map<String, NodeStats> postRestoreStatsMap = postRestoreStats.getNodesMap();
        for (String node : postRestoreStatsMap.keySet()) {
            NodeStats preRestoreStat = preRestoreStatsMap.get(node);
            NodeStats postRestoreStat = postRestoreStatsMap.get(node);
            if (preRestoreStat.getNode().isWarmNode()) {
                assertEquals(preRestoreStat.getFileCacheStats().getUsed(), postRestoreStat.getFileCacheStats().getUsed());
            }
        }
    }

    /**
     * Picks a shard out of the cluster state for each given index and asserts
     * that the 'index' directory does not exist in the node's file system.
     * This assertion is digging a bit into the implementation details to
     * verify that the Lucene segment files are not copied from the snapshot
     * repository to the node's local disk for a remote snapshot index.
     */
    private void assertIndexDirectoryDoesNotExist(String... indexNames) {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        for (String indexName : indexNames) {
            final Index index = state.metadata().index(indexName).getIndex();
            // Get the primary shards for the given index
            final GroupShardsIterator<ShardIterator> shardIterators = state.getRoutingTable()
                .activePrimaryShardsGrouped(new String[] { indexName }, false);
            // Randomly pick one of the shards
            final List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
            final ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
            final ShardRouting shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertTrue(shardRouting.primary());
            assertTrue(shardRouting.assignedToNode());
            // Get the file system stats for the assigned node
            final String nodeId = shardRouting.currentNodeId();
            final NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats(nodeId).addMetric(FS.metricName()).get();
            for (FsInfo.Path info : nodeStats.getNodes().get(0).getFs()) {
                // Build the expected path for the index data for a "normal"
                // index and assert it does not exist
                final String path = info.getPath();
                final Path file = PathUtils.get(path)
                    .resolve("indices")
                    .resolve(index.getUUID())
                    .resolve(Integer.toString(shardRouting.getId()))
                    .resolve("index");
                MatcherAssert.assertThat("Expect file not to exist: " + file, Files.exists(file), is(false));
            }
        }
    }

    private void assertAllNodesFileCacheEmpty() {
        NodesStatsResponse response = client().admin().cluster().nodesStats(new NodesStatsRequest().all()).actionGet();
        for (NodeStats stats : response.getNodes()) {
            FileCacheStats fcstats = stats.getFileCacheStats();
            if (fcstats != null) {
                assertTrue(isFileCacheEmpty(fcstats));
            }
        }
    }

    private void assertNodesFileCacheNonEmpty(int numNodes) {
        NodesStatsResponse response = client().admin().cluster().nodesStats(new NodesStatsRequest().all()).actionGet();
        int nonEmptyFileCacheNodes = 0;
        for (NodeStats stats : response.getNodes()) {
            FileCacheStats fcStats = stats.getFileCacheStats();
            if (stats.getNode().isWarmNode()) {
                if (!isFileCacheEmpty(fcStats)) {
                    nonEmptyFileCacheNodes++;
                }
            } else {
                assertNull(fcStats);
            }

        }
        assertEquals(numNodes, nonEmptyFileCacheNodes);
    }

    private boolean isFileCacheEmpty(FileCacheStats stats) {
        return stats.getUsed().getBytes() == 0L && stats.getActive().getBytes() == 0L;
    }

    public void testPruneFileCacheOnIndexDeletion() throws Exception {
        final String snapshotName = "test-snap";
        final String repoName = "test-repo";
        final String indexName1 = "test-idx-1";
        final String restoredIndexName1 = indexName1 + "-copy";
        final Client client = client();
        final int numNodes = 2;

        internalCluster().ensureAtLeastNumWarmAndDataNodes(numNodes);
        createIndexWithDocsAndEnsureGreen(1, 100, indexName1);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName1);
        deleteIndicesAndEnsureGreen(client, indexName1);

        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName1);
        assertNodesFileCacheNonEmpty(numNodes);

        deleteIndicesAndEnsureGreen(client, restoredIndexName1);
        assertAllNodesFileCacheEmpty();
    }

    /**
     * Test scenario that checks the cache folder location on warm nodes for the restored index on snapshot restoration
     * and ensures the index folder is cleared on all nodes post index deletion
     */
    public void testCacheIndexFilesClearedOnDelete() throws Exception {
        final int numReplicas = randomIntBetween(1, 4);
        final int numShards = numReplicas + 1;
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        internalCluster().ensureAtLeastNumWarmAndDataNodes(numShards);
        createIndexWithDocsAndEnsureGreen(numReplicas, 100, indexName);
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertDocCount(restoredIndexName, 100L);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        // The index count will be 1 since there is only a single restored index "test-idx-copy"
        assertCacheDirectoryReplicaAndIndexCount(numShards, 1);

        // The local cache files should be closed by deleting the restored index
        deleteIndicesAndEnsureGreen(client, restoredIndexName);

        logger.info("--> validate cache file path is deleted");
        // The index count will be 0 since the only restored index "test-idx-copy" was deleted
        assertCacheDirectoryReplicaAndIndexCount(numShards, 0);
        logger.info("--> validated that the cache file path doesn't exist");
    }

    /**
     * Test scenario that validates that the default warm preference for searchable snapshot
     * is primary shards
     */
    public void testDefaultShardPreference() throws Exception {
        final int numReplicas = 1;
        final String indexName = "test-idx";
        final String restoredIndexName = indexName + "-copy";
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final Client client = client();

        // Create an index, snapshot and restore as a searchable snapshot index
        internalCluster().ensureAtLeastNumWarmAndDataNodes(numReplicas + 1);
        createIndexWithDocsAndEnsureGreen(numReplicas, 100, indexName);
        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertDocCount(restoredIndexName, 100L);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        // ClusterSearchShards API returns a list of shards that will be used
        // when querying a particular index
        ClusterSearchShardsGroup[] shardGroups = client.admin()
            .cluster()
            .searchShards(new ClusterSearchShardsRequest(restoredIndexName))
            .actionGet()
            .getGroups();

        // Ensure when no preferences are set (default preference), the only compatible shards are primary
        for (ClusterSearchShardsGroup shardsGroup : shardGroups) {
            assertEquals(1, shardsGroup.getShards().length);
            assertTrue(shardsGroup.getShards()[0].primary());
        }

        // Ensure when preferences are set, all the compatible shards are returned
        shardGroups = client.admin()
            .cluster()
            .searchShards(new ClusterSearchShardsRequest(restoredIndexName).preference("foo"))
            .actionGet()
            .getGroups();

        // Ensures that the compatible shards are not just primaries
        for (ClusterSearchShardsGroup shardsGroup : shardGroups) {
            assertTrue(shardsGroup.getShards().length > 1);
            boolean containsReplica = Arrays.stream(shardsGroup.getShards())
                .map(shardRouting -> !shardRouting.primary())
                .reduce(false, (s1, s2) -> s1 || s2);
            assertTrue(containsReplica);
        }
    }

    public void testRestoreSearchableSnapshotWithIndexStoreTypeThrowsException() throws Exception {
        final String snapshotName = "test-snap";
        final String repoName = "test-repo";
        final String indexName1 = "test-idx-1";
        final int numReplicasIndex1 = randomIntBetween(1, 4);
        final Client client = client();

        internalCluster().ensureAtLeastNumDataNodes(numReplicasIndex1 + 1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex1, 100, indexName1);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName1);
        deleteIndicesAndEnsureGreen(client, indexName1);

        internalCluster().ensureAtLeastNumWarmNodes(numReplicasIndex1 + 1);

        // set "index.store.type" to "remote_snapshot" in index settings of restore API and assert appropriate exception with error message
        // is thrown.
        final SnapshotRestoreException error = expectThrows(
            SnapshotRestoreException.class,
            () -> client.admin()
                .cluster()
                .prepareRestoreSnapshot(repoName, snapshotName)
                .setRenamePattern("(.+)")
                .setRenameReplacement("$1-copy")
                .setIndexSettings(
                    Settings.builder()
                        .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
                )
                .setWaitForCompletion(true)
                .execute()
                .actionGet()
        );
        assertThat(
            error.getMessage(),
            containsString(
                "cannot restore remote snapshot with index settings \"index.store.type\" set to \"remote_snapshot\". Instead use \"storage_type\": \"remote_snapshot\" as argument to restore."
            )
        );
    }

    /**
     * Asserts the cache folder count to match the number of shards and the number of indices within the cache folder
     * as provided.
     * @param numCacheFolderCount total number of cache folders that should exist for the test case
     * @param numIndexCount total number of index folder locations that should exist within the cache folder
     */
    private void assertCacheDirectoryReplicaAndIndexCount(int numCacheFolderCount, int numIndexCount) throws IOException {
        // Get the available NodeEnvironment instances
        Iterable<Node> nodes = internalCluster().getInstances(Node.class);

        // Filter out warm NodeEnvironment(s) since FileCache is initialized only on warm nodes and
        // collect the path for all the cache locations on warm nodes.
        List<Path> searchNodeFileCachePaths = StreamSupport.stream(nodes.spliterator(), false)
            .filter(node -> node.fileCache() != null)
            .map(node -> node.getNodeEnvironment().fileCacheNodePath().fileCachePath)
            .collect(Collectors.toList());

        // Walk through the cache directory on nodes
        for (Path fileCachePath : searchNodeFileCachePaths) {
            assertTrue(Files.exists(fileCachePath));
            assertTrue(Files.isDirectory(fileCachePath));
            try (Stream<Path> dataPathStream = Files.list(fileCachePath)) {
                assertEquals(numIndexCount, dataPathStream.count());
            }
        }
        // Verifies if all the shards (primary and replica) have been deleted
        assertEquals(numCacheFolderCount, searchNodeFileCachePaths.size());
    }

    public void testRelocateSearchableSnapshotIndex() throws Exception {
        final String snapshotName = "test-snap";
        final String repoName = "test-repo";
        final String indexName = "test-idx-1";
        final String restoredIndexName = indexName + "-copy";
        final Client client = client();

        internalCluster().ensureAtLeastNumDataNodes(1);
        createIndexWithDocsAndEnsureGreen(0, 100, indexName);

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName);
        deleteIndicesAndEnsureGreen(client, indexName);

        String searchNode1 = internalCluster().startWarmOnlyNodes(1).get(0);
        internalCluster().validateClusterFormed();
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);
        assertRemoteSnapshotIndexSettings(client, restoredIndexName);

        String searchNode2 = internalCluster().startWarmOnlyNodes(1).get(0);
        internalCluster().validateClusterFormed();

        final Index index = resolveIndex(restoredIndexName);
        assertSearchableSnapshotIndexDirectoryExistence(searchNode1, index, true);
        assertSearchableSnapshotIndexDirectoryExistence(searchNode2, index, false);

        // relocate the shard from node1 to node2
        client.admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(restoredIndexName, 0, searchNode1, searchNode2))
            .execute()
            .actionGet();
        ClusterHealthResponse clusterHealthResponse = client.admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(new TimeValue(5, TimeUnit.MINUTES))
            .execute()
            .actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));
        assertDocCount(restoredIndexName, 100L);

        assertSearchableSnapshotIndexDirectoryExistence(searchNode1, index, false);
        assertSearchableSnapshotIndexDirectoryExistence(searchNode2, index, true);
        deleteIndicesAndEnsureGreen(client, restoredIndexName);
        assertSearchableSnapshotIndexDirectoryExistence(searchNode2, index, false);
    }

    public void testCreateSearchableSnapshotWithSpecifiedRemoteDataRatio() throws Exception {
        final String snapshotName = "test-snap";
        final String repoName = "test-repo";
        final String indexName1 = "test-idx-1";
        final String restoredIndexName1 = indexName1 + "-copy";
        final String indexName2 = "test-idx-2";
        final String restoredIndexName2 = indexName2 + "-copy";
        final int numReplicasIndex1 = 1;
        final int numReplicasIndex2 = 1;

        Settings clusterManagerNodeSettings = clusterManagerOnlyNode();
        internalCluster().startNodes(2, clusterManagerNodeSettings);
        Settings dateNodeSettings = dataNode();
        internalCluster().startNodes(2, dateNodeSettings);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex1, 100, indexName1);
        createIndexWithDocsAndEnsureGreen(numReplicasIndex2, 100, indexName2);

        final Client client = client();
        assertAcked(
            client.admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey(), 5))
        );

        createRepositoryWithSettings(null, repoName);
        takeSnapshot(client, snapshotName, repoName, indexName1, indexName2);

        internalCluster().ensureAtLeastNumWarmNodes(Math.max(numReplicasIndex1, numReplicasIndex2) + 1);
        restoreSnapshotAndEnsureGreen(client, snapshotName, repoName);

        assertDocCount(restoredIndexName1, 100L);
        assertDocCount(restoredIndexName2, 100L);
        assertIndexDirectoryDoesNotExist(restoredIndexName1, restoredIndexName2);
    }

    @After
    public void cleanup() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(DATA_TO_FILE_CACHE_SIZE_RATIO_SETTING.getKey()))
        );
    }

    public void testStartSearchNode() throws Exception {
        // test start dedicated warm node
        internalCluster().startNode(Settings.builder().put(onlyRole(DiscoveryNodeRole.WARM_ROLE)));
        // test start node without warm role
        internalCluster().startNode(Settings.builder().put(onlyRole(DiscoveryNodeRole.DATA_ROLE)));
        // test start non-dedicated warm node, if the user doesn't configure the cache size, it fails
        assertThrows(
            SettingsException.class,
            () -> internalCluster().startNode(
                Settings.builder().put(onlyRoles(Set.of(DiscoveryNodeRole.WARM_ROLE, DiscoveryNodeRole.DATA_ROLE)))
            )
        );
        // test start non-dedicated warm node
        assertThrows(
            SettingsException.class,
            () -> internalCluster().startNode(
                Settings.builder().put(onlyRoles(Set.of(DiscoveryNodeRole.WARM_ROLE, DiscoveryNodeRole.DATA_ROLE)))
            )
        );
    }

    private void assertSearchableSnapshotIndexDirectoryExistence(String nodeName, Index index, boolean exists) throws Exception {
        final Node node = internalCluster().getInstance(Node.class, nodeName);
        final ShardId shardId = new ShardId(index, 0);
        final ShardPath shardPath = ShardPath.loadFileCachePath(node.getNodeEnvironment(), shardId);

        assertBusy(() -> {
            assertTrue(
                "shard state path should " + (exists ? "exist" : "not exist"),
                Files.exists(shardPath.getShardStatePath()) == exists
            );
            assertTrue("shard cache path should " + (exists ? "exist" : "not exist"), Files.exists(shardPath.getDataPath()) == exists);
        }, 30, TimeUnit.SECONDS);

        final Path indexDataPath = node.getNodeEnvironment().fileCacheNodePath().fileCachePath.resolve(index.getUUID());
        final Path indexPath = node.getNodeEnvironment().fileCacheNodePath().indicesPath.resolve(index.getUUID());
        assertBusy(() -> {
            assertTrue("index path should " + (exists ? "exist" : "not exist"), Files.exists(indexDataPath) == exists);
            assertTrue("index cache path should " + (exists ? "exist" : "not exist"), Files.exists(indexPath) == exists);
        }, 30, TimeUnit.SECONDS);
    }
}
