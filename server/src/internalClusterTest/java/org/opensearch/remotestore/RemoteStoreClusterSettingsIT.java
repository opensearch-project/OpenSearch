/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.get.GetIndexRequest;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.ExecutionException;

import static org.opensearch.indices.IndicesService.CLUSTER_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreClusterSettingsIT extends RemoteStoreBaseIntegTestCase {

    private final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
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

}
