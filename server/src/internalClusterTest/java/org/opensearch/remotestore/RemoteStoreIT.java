/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.translog.Translog.Durability;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.recovery.PeerRecoveryTargetService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.multipart.mocks.MockFsRepositoryPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.SEGMENTS;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
import static org.opensearch.test.OpenSearchTestCase.getShardLevelBlobPath;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreIT extends RemoteStoreBaseIntegTestCase {

    protected final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockFsRepositoryPlugin.class);
    }

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    private void testPeerRecovery(int numberOfIterations, boolean invokeFlush) throws Exception {
        internalCluster().startNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, INDEX_NAME);

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        String replicaNodeName = replicaNodeName(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client(replicaNodeName).prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );

        RecoveryResponse recoveryResponse = client(replicaNodeName).admin().indices().prepareRecoveries().get();

        Optional<RecoveryState> recoverySource = recoveryResponse.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(rs -> rs.getRecoverySource().getType() == RecoverySource.Type.PEER)
            .findFirst();
        assertFalse(recoverySource.isEmpty());
        // segments_N file is copied to new replica
        assertEquals(1, recoverySource.get().getIndex().recoveredFileCount());

        IndexResponse response = indexSingleDoc(INDEX_NAME);
        assertEquals(indexStats.get(MAX_SEQ_NO_TOTAL) + 1, response.getSeqNo());
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client(replicaNodeName).prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1),
            30,
            TimeUnit.SECONDS
        );
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogNoDataFlush() throws Exception {
        testPeerRecovery(1, true);
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogFlush() throws Exception {
        testPeerRecovery(randomIntBetween(2, 5), true);
    }

    public void testPeerRecoveryWithLowActivityTimeout() throws Exception {
        ClusterUpdateSettingsRequest req = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder()
                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "20kb")
                .put(RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.getKey(), "1s")
        );
        internalCluster().client().admin().cluster().updateSettings(req).get();
        testPeerRecovery(randomIntBetween(2, 5), true);
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogNoDataRefresh() throws Exception {
        testPeerRecovery(1, false);
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogRefresh() throws Exception {
        testPeerRecovery(randomIntBetween(2, 5), false);
    }

    private void verifyRemoteStoreCleanup() throws Exception {
        internalCluster().startNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));

        indexData(5, randomBoolean(), INDEX_NAME);
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(segmentRepoPath), indexUUID);
        assertTrue(getFileCount(indexPath) > 0);
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
        // Delete is async. Give time for it
        assertBusy(() -> {
            try {
                assertThat(getFileCount(indexPath), comparesEqualTo(0));
            } catch (Exception e) {}
        }, 30, TimeUnit.SECONDS);
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9327")
    public void testRemoteTranslogCleanup() throws Exception {
        verifyRemoteStoreCleanup();
    }

    public void testStaleCommitDeletionWithInvokeFlush() throws Exception {
        String dataNode = internalCluster().startNode();
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(5, 15);
        indexData(numberOfIterations, true, INDEX_NAME);
        String shardPath = getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", SEGMENTS, METADATA).buildAsString();
        Path indexPath = Path.of(segmentRepoPath + "/" + shardPath);
        ;
        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        int lastNMetadataFilesToKeep = indexShard.getRemoteStoreSettings().getMinRemoteSegmentMetadataFiles();
        // Delete is async.
        assertBusy(() -> {
            int actualFileCount = getFileCount(indexPath);
            if (numberOfIterations <= lastNMetadataFilesToKeep) {
                MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations - 1, numberOfIterations, numberOfIterations + 1)));
            } else {
                // As delete is async its possible that the file gets created before the deletion or after
                // deletion.
                MatcherAssert.assertThat(
                    actualFileCount,
                    is(oneOf(lastNMetadataFilesToKeep - 1, lastNMetadataFilesToKeep, lastNMetadataFilesToKeep + 1))
                );
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testStaleCommitDeletionWithoutInvokeFlush() throws Exception {
        internalCluster().startNode();
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(5, 15);
        indexData(numberOfIterations, false, INDEX_NAME);
        String shardPath = getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", SEGMENTS, METADATA).buildAsString();
        Path indexPath = Path.of(segmentRepoPath + "/" + shardPath);
        int actualFileCount = getFileCount(indexPath);
        // We also allow (numberOfIterations + 1) as index creation also triggers refresh.
        MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations - 1, numberOfIterations, numberOfIterations + 1)));
    }

    public void testStaleCommitDeletionWithMinSegmentFiles_3() throws Exception {
        Settings.Builder settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), "3");
        internalCluster().startNode(settings);

        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(5, 15);
        indexData(numberOfIterations, true, INDEX_NAME);
        String shardPath = getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", SEGMENTS, METADATA).buildAsString();
        Path indexPath = Path.of(segmentRepoPath + "/" + shardPath);
        int actualFileCount = getFileCount(indexPath);
        // We also allow (numberOfIterations + 1) as index creation also triggers refresh.
        MatcherAssert.assertThat(actualFileCount, is(oneOf(4)));
    }

    public void testStaleCommitDeletionWithMinSegmentFiles_Disabled() throws Exception {
        Settings.Builder settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_INDEX_SEGMENT_METADATA_RETENTION_MAX_COUNT_SETTING.getKey(), "-1");
        internalCluster().startNode(settings);

        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(12, 18);
        indexData(numberOfIterations, true, INDEX_NAME);
        String shardPath = getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", SEGMENTS, METADATA).buildAsString();
        Path indexPath = Path.of(segmentRepoPath + "/" + shardPath);
        ;
        int actualFileCount = getFileCount(indexPath);
        // We also allow (numberOfIterations + 1) as index creation also triggers refresh.
        MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations + 1)));
    }

    /**
     * Tests that when the index setting is not passed during index creation, the buffer interval picked up is the cluster
     * default.
     */
    public void testDefaultBufferInterval() throws ExecutionException, InterruptedException {
        internalCluster().startClusterManagerOnlyNode();
        String clusterManagerName = internalCluster().getClusterManagerName();
        String dataNode = internalCluster().startDataOnlyNodes(1).get(0);
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertClusterRemoteBufferInterval(IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL, dataNode);

        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        assertTrue(indexShard.getTranslogSyncProcessor() instanceof BufferedAsyncIOProcessor);
        assertBufferInterval(IndexSettings.DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL, indexShard);

        // Next, we change the default buffer interval and the same should reflect in the buffer interval of the index created
        TimeValue clusterBufferInterval = TimeValue.timeValueSeconds(randomIntBetween(100, 200));
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), clusterBufferInterval))
            .get();
        assertBufferInterval(clusterBufferInterval, indexShard);
        clearClusterBufferIntervalSetting(clusterManagerName);
    }

    /**
     * This tests multiple cases where the index setting is passed during the index creation with multiple combinations
     * with and without cluster default.
     */
    public void testOverriddenBufferInterval() throws ExecutionException, InterruptedException {
        internalCluster().startClusterManagerOnlyNode();
        String clusterManagerName = internalCluster().getClusterManagerName();
        String dataNode = internalCluster().startDataOnlyNodes(1).get(0);

        TimeValue bufferInterval = TimeValue.timeValueSeconds(randomIntBetween(0, 100));
        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), bufferInterval)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        assertTrue(indexShard.getTranslogSyncProcessor() instanceof BufferedAsyncIOProcessor);
        assertBufferInterval(bufferInterval, indexShard);

        // Set the cluster default with a different value, validate that the buffer interval is still the overridden value
        TimeValue clusterBufferInterval = TimeValue.timeValueSeconds(randomIntBetween(100, 200));
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), clusterBufferInterval))
            .get();
        assertBufferInterval(bufferInterval, indexShard);

        // Set the index setting (index.remote_store.translog.buffer_interval) with a different value and validate that
        // the buffer interval is updated
        bufferInterval = TimeValue.timeValueSeconds(bufferInterval.seconds() + randomIntBetween(1, 100));
        client(clusterManagerName).admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(INDEX_NAME).settings(
                    Settings.builder().put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), bufferInterval)
                )
            )
            .get();
        assertBufferInterval(bufferInterval, indexShard);

        // Set the index setting (index.remote_store.translog.buffer_interval) with null and validate the buffer interval
        // which will be the cluster default now.
        client(clusterManagerName).admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(INDEX_NAME).settings(
                    Settings.builder().putNull(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey())
                )
            )
            .get();
        assertBufferInterval(clusterBufferInterval, indexShard);
        clearClusterBufferIntervalSetting(clusterManagerName);
    }

    /**
     * This tests validation which kicks in during index creation failing creation if the value is less than minimum allowed value.
     */
    public void testOverriddenBufferIntervalValidation() {
        internalCluster().startClusterManagerOnlyNode();
        TimeValue bufferInterval = TimeValue.timeValueSeconds(-1);
        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), bufferInterval)
            .build();
        IllegalArgumentException exceptionDuringCreateIndex = assertThrows(
            IllegalArgumentException.class,
            () -> createIndex(INDEX_NAME, indexSettings)
        );
        assertEquals(
            "failed to parse value [-1] for setting [index.remote_store.translog.buffer_interval], must be >= [0ms]",
            exceptionDuringCreateIndex.getMessage()
        );
    }

    /**
     * This tests validation of the cluster setting when being set.
     */
    public void testClusterBufferIntervalValidation() {
        String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> client(clusterManagerName).admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(-1))
                )
                .get()
        );
        assertEquals(
            "failed to parse value [-1] for setting [cluster.remote_store.translog.buffer_interval], must be >= [0ms]",
            exception.getMessage()
        );
    }

    public void testRequestDurabilityWhenRestrictSettingExplicitFalse() throws ExecutionException, InterruptedException {
        // Explicit node settings and request durability
        testRestrictSettingFalse(true, Durability.REQUEST);
    }

    public void testAsyncDurabilityWhenRestrictSettingExplicitFalse() throws ExecutionException, InterruptedException {
        // Explicit node settings and async durability
        testRestrictSettingFalse(true, Durability.ASYNC);
    }

    public void testRequestDurabilityWhenRestrictSettingImplicitFalse() throws ExecutionException, InterruptedException {
        // No node settings and request durability
        testRestrictSettingFalse(false, Durability.REQUEST);
    }

    public void testAsyncDurabilityWhenRestrictSettingImplicitFalse() throws ExecutionException, InterruptedException {
        // No node settings and async durability
        testRestrictSettingFalse(false, Durability.ASYNC);
    }

    private void testRestrictSettingFalse(boolean setRestrictFalse, Durability durability) throws ExecutionException, InterruptedException {
        String clusterManagerName;
        if (setRestrictFalse) {
            clusterManagerName = internalCluster().startClusterManagerOnlyNode(
                Settings.builder().put(IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING.getKey(), false).build()
            );
        } else {
            clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        }
        String dataNode = internalCluster().startDataOnlyNodes(1).get(0);
        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability)
            .build();
        createIndex(INDEX_NAME, indexSettings);
        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        assertEquals(durability, indexShard.indexSettings().getTranslogDurability());

        durability = randomFrom(Durability.values());
        client(clusterManagerName).admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(INDEX_NAME).settings(
                    Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability)
                )
            )
            .get();
        assertEquals(durability, indexShard.indexSettings().getTranslogDurability());
    }

    public void testAsyncDurabilityThrowsExceptionWhenRestrictSettingTrue() throws ExecutionException, InterruptedException {
        String expectedExceptionMsg =
            "index setting [index.translog.durability=async] is not allowed as cluster setting [cluster.remote_store.index.restrict.async-durability=true]";
        String clusterManagerName = internalCluster().startClusterManagerOnlyNode(
            Settings.builder().put(IndicesService.CLUSTER_REMOTE_INDEX_RESTRICT_ASYNC_DURABILITY_SETTING.getKey(), true).build()
        );
        String dataNode = internalCluster().startDataOnlyNodes(1).get(0);

        // Case 1 - Test create index fails
        Settings indexSettings = Settings.builder()
            .put(indexSettings())
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Durability.ASYNC)
            .build();
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> createIndex(INDEX_NAME, indexSettings));
        assertEquals(expectedExceptionMsg, exception.getMessage());

        // Case 2 - Test update index fails
        createIndex(INDEX_NAME);
        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        assertEquals(Durability.REQUEST, indexShard.indexSettings().getTranslogDurability());
        exception = assertThrows(
            IllegalArgumentException.class,
            () -> client(clusterManagerName).admin()
                .indices()
                .updateSettings(new UpdateSettingsRequest(INDEX_NAME).settings(indexSettings))
                .actionGet()
        );
        assertEquals(expectedExceptionMsg, exception.getMessage());
    }

    private void assertClusterRemoteBufferInterval(TimeValue expectedBufferInterval, String dataNode) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNode);
        assertEquals(expectedBufferInterval, indicesService.getRemoteStoreSettings().getClusterRemoteTranslogBufferInterval());
    }

    private void assertBufferInterval(TimeValue expectedBufferInterval, IndexShard indexShard) {
        assertEquals(
            expectedBufferInterval,
            ((BufferedAsyncIOProcessor<?>) indexShard.getTranslogSyncProcessor()).getBufferIntervalSupplier().get()
        );
    }

    private void clearClusterBufferIntervalSetting(String clusterManagerName) {
        client(clusterManagerName).admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey()))
            .get();
    }

    public void testRestoreSnapshotToIndexWithSameNameDifferentUUID() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);

        Path absolutePath = randomRepoPath().toAbsolutePath();
        assertAcked(
            clusterAdmin().preparePutRepository("test-repo").setType("fs").setSettings(Settings.builder().put("location", absolutePath))
        );

        logger.info("--> Create index and ingest 50 docs");
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));
        indexBulk(INDEX_NAME, 50);
        flushAndRefresh(INDEX_NAME);

        String originalIndexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        assertNotNull(originalIndexUUID);
        assertNotEquals(IndexMetadata.INDEX_UUID_NA_VALUE, originalIndexUUID);

        ensureGreen();

        logger.info("--> take a snapshot");
        client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setIndices(INDEX_NAME).setWaitForCompletion(true).get();

        logger.info("--> wipe all indices");
        cluster().wipeIndices(INDEX_NAME);

        logger.info("--> Create index with the same name, different UUID");
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1))
        );

        ensureGreen(TimeValue.timeValueSeconds(30), INDEX_NAME);

        String newIndexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        assertNotNull(newIndexUUID);
        assertNotEquals(IndexMetadata.INDEX_UUID_NA_VALUE, newIndexUUID);
        assertNotEquals(newIndexUUID, originalIndexUUID);

        logger.info("--> close index");
        client().admin().indices().prepareClose(INDEX_NAME).get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        flushAndRefresh(INDEX_NAME);

        ensureGreen(INDEX_NAME);
        assertBusy(() -> {
            assertHitCount(client(dataNodes.get(0)).prepareSearch(INDEX_NAME).setSize(0).get(), 50);
            assertHitCount(client(dataNodes.get(1)).prepareSearch(INDEX_NAME).setSize(0).get(), 50);
        });
    }

    public void testNoSearchIdleForAnyReplicaCount() throws ExecutionException, InterruptedException {
        internalCluster().startClusterManagerOnlyNode();
        String primaryShardNode = internalCluster().startDataOnlyNodes(1).get(0);

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureGreen(INDEX_NAME);
        IndexShard indexShard = getIndexShard(primaryShardNode, INDEX_NAME);
        assertFalse(indexShard.isSearchIdleSupported());

        String replicaShardNode = internalCluster().startDataOnlyNodes(1).get(0);
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        ensureGreen(INDEX_NAME);
        assertFalse(indexShard.isSearchIdleSupported());

        indexShard = getIndexShard(replicaShardNode, INDEX_NAME);
        assertFalse(indexShard.isSearchIdleSupported());
    }

    public void testFallbackToNodeToNodeSegmentCopy() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        List<String> dataNodes = internalCluster().startDataOnlyNodes(2);

        // 1. Create index with 0 replica
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 10000L, -1));
        ensureGreen(INDEX_NAME);

        // 2. Index docs
        indexBulk(INDEX_NAME, 50);
        flushAndRefresh(INDEX_NAME);

        // 3. Delete data from remote segment store
        String shardPath = getShardLevelBlobPath(client(), INDEX_NAME, BlobPath.cleanPath(), "0", SEGMENTS, DATA).buildAsString();
        Path segmentDataPath = Path.of(segmentRepoPath + "/" + shardPath);

        try (Stream<Path> files = Files.list(segmentDataPath)) {
            files.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    // Ignore
                }
            });
        }

        // 4. Start recovery by changing number of replicas to 1
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

        // 5. Ensure green and verify number of docs
        ensureGreen(INDEX_NAME);
        assertBusy(() -> {
            assertHitCount(client(dataNodes.get(0)).prepareSearch(INDEX_NAME).setSize(0).get(), 50);
            assertHitCount(client(dataNodes.get(1)).prepareSearch(INDEX_NAME).setSize(0).get(), 50);
        });
    }

    public void testNoMultipleWriterDuringPrimaryRelocation() throws ExecutionException, InterruptedException {
        // In this test, we trigger a force flush on existing primary while the primary mode on new primary has been
        // activated. There was a bug in primary relocation of remote store enabled indexes where the new primary
        // starts uploading translog and segments even before the cluster manager has started this shard. With this test,
        // we check that we do not overwrite any file on remote store. Here we will also increase the replica count to
        // check that there are no duplicate metadata files for translog or upload.

        internalCluster().startClusterManagerOnlyNode();
        String oldPrimary = internalCluster().startDataOnlyNodes(1).get(0);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureGreen(INDEX_NAME);
        indexBulk(INDEX_NAME, randomIntBetween(5, 10));
        String newPrimary = internalCluster().startDataOnlyNodes(1).get(0);
        ensureStableCluster(3);

        IndexShard oldPrimaryIndexShard = getIndexShard(oldPrimary, INDEX_NAME);
        CountDownLatch flushLatch = new CountDownLatch(1);

        MockTransportService mockTargetTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            oldPrimary
        ));
        mockTargetTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT.equals(action)) {
                flushLatch.countDown();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("--> relocate the shard");
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, oldPrimary, newPrimary))
            .execute()
            .actionGet();

        CountDownLatch flushDone = new CountDownLatch(1);
        Thread flushThread = new Thread(() -> {
            try {
                flushLatch.await(2, TimeUnit.SECONDS);
                oldPrimaryIndexShard.flush(new FlushRequest().waitIfOngoing(true).force(true));
                // newPrimaryTranslogRepo.setSleepSeconds(0);
            } catch (IndexShardClosedException e) {
                // this is fine
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } finally {
                flushDone.countDown();
            }
        });
        flushThread.start();
        flushDone.await(5, TimeUnit.SECONDS);
        flushThread.join();

        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForStatus(ClusterHealthStatus.GREEN)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(TimeValue.timeValueSeconds(5))
            .execute()
            .actionGet();
        assertFalse(clusterHealthResponse.isTimedOut());

        client().admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(INDEX_NAME).settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            )
            .get();

        clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForStatus(ClusterHealthStatus.GREEN)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(TimeValue.timeValueSeconds(5))
            .execute()
            .actionGet();
        assertFalse(clusterHealthResponse.isTimedOut());
    }

    public void testResumeUploadAfterFailedPrimaryRelocation() throws ExecutionException, InterruptedException, IOException {
        // In this test, we fail the hand off during the primary relocation. This will undo the drainRefreshes and
        // drainSync performed as part of relocation handoff (before performing the handoff transport action).
        // We validate the same here by failing the peer recovery and ensuring we can index afterward as well.

        internalCluster().startClusterManagerOnlyNode();
        String oldPrimary = internalCluster().startDataOnlyNodes(1).get(0);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureGreen(INDEX_NAME);
        int docs = randomIntBetween(5, 10);
        indexBulk(INDEX_NAME, docs);
        flushAndRefresh(INDEX_NAME);
        assertHitCount(client(oldPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs);
        String newPrimary = internalCluster().startDataOnlyNodes(1).get(0);
        ensureStableCluster(3);

        IndexShard oldPrimaryIndexShard = getIndexShard(oldPrimary, INDEX_NAME);
        CountDownLatch handOffLatch = new CountDownLatch(1);

        MockTransportService mockTargetTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            oldPrimary
        ));
        mockTargetTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT.equals(action)) {
                handOffLatch.countDown();
                throw new OpenSearchException("failing recovery for test purposes");
            }
            connection.sendRequest(requestId, action, request, options);
        });

        logger.info("--> relocate the shard");
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, oldPrimary, newPrimary))
            .execute()
            .actionGet();

        handOffLatch.await(30, TimeUnit.SECONDS);

        assertTrue(oldPrimaryIndexShard.isStartedPrimary());
        assertEquals(oldPrimary, primaryNodeName(INDEX_NAME));
        assertHitCount(client(oldPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs);

        SearchPhaseExecutionException ex = assertThrows(
            SearchPhaseExecutionException.class,
            () -> client(newPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get()
        );
        assertEquals("all shards failed", ex.getMessage());

        int moreDocs = randomIntBetween(5, 10);
        indexBulk(INDEX_NAME, moreDocs);
        flushAndRefresh(INDEX_NAME);
        int uncommittedOps = randomIntBetween(5, 10);
        indexBulk(INDEX_NAME, uncommittedOps);
        assertHitCount(client(oldPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), docs + moreDocs);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));

        restore(true, INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(
            client(newPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(),
            docs + moreDocs + uncommittedOps
        );

        String newNode = internalCluster().startDataOnlyNodes(1).get(0);
        ensureStableCluster(3);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, newPrimary, newNode))
            .execute()
            .actionGet();

        ClusterHealthResponse clusterHealthResponse = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForStatus(ClusterHealthStatus.GREEN)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(TimeValue.timeValueSeconds(10))
            .execute()
            .actionGet();
        assertFalse(clusterHealthResponse.isTimedOut());

        ex = assertThrows(
            SearchPhaseExecutionException.class,
            () -> client(newPrimary).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get()
        );
        assertEquals("all shards failed", ex.getMessage());
        assertHitCount(
            client(newNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(),
            docs + moreDocs + uncommittedOps
        );
    }

    // Test local only translog files which are not uploaded to remote store (no metadata present in remote)
    // Without the cleanup change in RemoteFsTranslog.createEmptyTranslog, this test fails with NPE.
    public void testLocalOnlyTranslogCleanupOnNodeRestart() throws Exception {
        clusterSettingsSuppliedByTest = true;

        // Overriding settings to use AsyncMultiStreamBlobContainer
        Settings settings = Settings.builder()
            .put(super.nodeSettings(1))
            .put(
                remoteStoreClusterSettings(
                    REPOSITORY_NAME,
                    segmentRepoPath,
                    MockFsRepositoryPlugin.TYPE,
                    REPOSITORY_2_NAME,
                    translogRepoPath,
                    MockFsRepositoryPlugin.TYPE
                )
            )
            .build();

        internalCluster().startClusterManagerOnlyNode(settings);
        String dataNode = internalCluster().startDataOnlyNode(settings);

        // 1. Create index with 0 replica
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 10000L, -1));
        ensureGreen(INDEX_NAME);

        // 2. Index docs
        int searchableDocs = 0;
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            indexBulk(INDEX_NAME, 15);
            refresh(INDEX_NAME);
            searchableDocs += 15;
        }
        indexBulk(INDEX_NAME, 15);

        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs);

        // 3. Delete metadata from remote translog
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);

        Path translogMetaDataPath = Path.of(String.valueOf(translogRepoPath), indexUUID, "/0/translog/metadata");

        try (Stream<Path> files = Files.list(translogMetaDataPath)) {
            files.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    // Ignore
                }
            });
        }

        internalCluster().restartNode(dataNode);

        ensureGreen(INDEX_NAME);

        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs);
        indexBulk(INDEX_NAME, 15);
        refresh(INDEX_NAME);
        assertHitCount(client(dataNode).prepareSearch(INDEX_NAME).setSize(0).get(), searchableDocs + 15);
    }
}
