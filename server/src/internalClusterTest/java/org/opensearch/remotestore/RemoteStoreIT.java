/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

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
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.shard.RemoteStoreRefreshListener.LAST_N_METADATA_FILES_TO_KEEP;
import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoteStoreIT extends RemoteStoreBaseIntegTestCase {

    protected final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Before
    public void setup() {
        setupRepo();
    }

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    private void testPeerRecovery(int numberOfIterations, boolean invokeFlush) throws Exception {
        internalCluster().startDataOnlyNodes(3);
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

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogNoDataRefresh() throws Exception {
        testPeerRecovery(1, false);
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogRefresh() throws Exception {
        testPeerRecovery(randomIntBetween(2, 5), false);
    }

    private void verifyRemoteStoreCleanup() throws Exception {
        internalCluster().startDataOnlyNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));

        indexData(5, randomBoolean(), INDEX_NAME);
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(absolutePath), indexUUID);
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
        internalCluster().startDataOnlyNodes(1);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(5, 15);
        indexData(numberOfIterations, true, INDEX_NAME);
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(absolutePath), indexUUID, "/0/segments/metadata");
        // Delete is async.
        assertBusy(() -> {
            int actualFileCount = getFileCount(indexPath);
            if (numberOfIterations <= LAST_N_METADATA_FILES_TO_KEEP) {
                MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations - 1, numberOfIterations, numberOfIterations + 1)));
            } else {
                // As delete is async its possible that the file gets created before the deletion or after
                // deletion.
                MatcherAssert.assertThat(
                    actualFileCount,
                    is(oneOf(LAST_N_METADATA_FILES_TO_KEEP - 1, LAST_N_METADATA_FILES_TO_KEEP, LAST_N_METADATA_FILES_TO_KEEP + 1))
                );
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testStaleCommitDeletionWithoutInvokeFlush() throws Exception {
        internalCluster().startDataOnlyNodes(1);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(5, 15);
        indexData(numberOfIterations, false, INDEX_NAME);
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(absolutePath), indexUUID, "/0/segments/metadata");
        int actualFileCount = getFileCount(indexPath);
        // We also allow (numberOfIterations + 1) as index creation also triggers refresh.
        MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations - 1, numberOfIterations, numberOfIterations + 1)));
    }

    /**
     * Tests that when the index setting is not passed during index creation, the buffer interval picked up is the cluster
     * default.
     */
    public void testDefaultBufferInterval() throws ExecutionException, InterruptedException {
        setupRepo();
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
        setupRepo();
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
        setupRepo();
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
        setupRepo(false);
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
}
