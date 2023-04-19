/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteRefreshSegmentPressureServiceTests extends OpenSearchTestCase {

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private ShardId shardId;

    private RemoteRefreshSegmentPressureService pressureService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_refresh_segment_pressure_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        shardId = new ShardId("index", "uuid", 0);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testIsSegmentsUploadBackpressureEnabled() {
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        assertFalse(pressureService.isSegmentsUploadBackpressureEnabled());

        Settings newSettings = Settings.builder()
            .put(RemoteRefreshSegmentPressureSettings.REMOTE_REFRESH_SEGMENT_PRESSURE_ENABLED.getKey(), "true")
            .build();
        clusterService.getClusterSettings().applySettings(newSettings);

        assertTrue(pressureService.isSegmentsUploadBackpressureEnabled());
    }

    public void testAfterIndexShardCreatedForRemoteBackedIndex() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, "true").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        ShardId testShardId = new ShardId("index", "uuid", 0);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(indexShard.shardId()).thenReturn(testShardId);
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        pressureService.afterIndexShardCreated(indexShard);
        assertNotNull(pressureService.getPressureTracker(testShardId));
    }

    public void testAfterIndexShardCreatedForNonRemoteBackedIndex() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, "false").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        ShardId testShardId = new ShardId("index", "uuid", 0);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(indexShard.shardId()).thenReturn(testShardId);
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        pressureService.afterIndexShardCreated(indexShard);
        assertNull(pressureService.getPressureTracker(testShardId));
    }

    public void testBeforeIndexShardClosed() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, "true").build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        ShardId testShardId = new ShardId("index", "uuid", 0);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(indexShard.shardId()).thenReturn(testShardId);
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        pressureService.afterIndexShardCreated(indexShard);
        assertNotNull(pressureService.getPressureTracker(testShardId));

        pressureService.beforeIndexShardClosed(testShardId, indexShard, settings);
        assertNull(pressureService.getPressureTracker(testShardId));
    }

}
