/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog.Durability;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.hamcrest.MatcherAssert;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
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
        return Arrays.asList(MockTransportService.TestPlugin.class);
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
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(segmentRepoPath), indexUUID, "/0/segments/metadata");

        IndexShard indexShard = getIndexShard(dataNode);
        int lastNMetadataFilesToKeep = indexShard.getRecoverySettings().getMinRemoteSegmentMetadataFiles();
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
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(segmentRepoPath), indexUUID, "/0/segments/metadata");
        int actualFileCount = getFileCount(indexPath);
        // We also allow (numberOfIterations + 1) as index creation also triggers refresh.
        MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations - 1, numberOfIterations, numberOfIterations + 1)));
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

        IndexShard indexShard = getIndexShard(dataNode);
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

        IndexShard indexShard = getIndexShard(dataNode);
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
        IndexShard indexShard = getIndexShard(dataNode);
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
        IndexShard indexShard = getIndexShard(dataNode);
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

    private IndexShard getIndexShard(String dataNode) throws ExecutionException, InterruptedException {
        String clusterManagerName = internalCluster().getClusterManagerName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNode);
        GetIndexResponse getIndexResponse = client(clusterManagerName).admin().indices().getIndex(new GetIndexRequest()).get();
        String uuid = getIndexResponse.getSettings().get(INDEX_NAME).get(IndexMetadata.SETTING_INDEX_UUID);
        IndexService indexService = indicesService.indexService(new Index(INDEX_NAME, uuid));
        return indexService.getShard(0);
    }

    private void assertClusterRemoteBufferInterval(TimeValue expectedBufferInterval, String dataNode) {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, dataNode);
        assertEquals(expectedBufferInterval, indicesService.getClusterRemoteTranslogBufferInterval());
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
        IndexShard indexShard = getIndexShard(primaryShardNode);
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

        indexShard = getIndexShard(replicaShardNode);
        assertFalse(indexShard.isSearchIdleSupported());
    }
}
