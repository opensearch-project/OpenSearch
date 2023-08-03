/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.index.store.Store;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

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
        IndexShard indexShard = createIndexShard(shardId, true);
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        pressureService.afterIndexShardCreated(indexShard);
        assertNotNull(pressureService.getRemoteRefreshSegmentTracker(indexShard.shardId()));
    }

    public void testAfterIndexShardCreatedForNonRemoteBackedIndex() {
        IndexShard indexShard = createIndexShard(shardId, false);
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        pressureService.afterIndexShardCreated(indexShard);
        assertNull(pressureService.getRemoteRefreshSegmentTracker(indexShard.shardId()));
    }

    public void testAfterIndexShardClosed() {
        IndexShard indexShard = createIndexShard(shardId, true);
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        pressureService.afterIndexShardCreated(indexShard);
        assertNotNull(pressureService.getRemoteRefreshSegmentTracker(shardId));

        pressureService.afterIndexShardClosed(shardId, indexShard, indexShard.indexSettings().getSettings());
        assertNull(pressureService.getRemoteRefreshSegmentTracker(shardId));
    }

    public void testValidateSegmentUploadLag() {
        // Create the pressure tracker
        IndexShard indexShard = createIndexShard(shardId, true);
        pressureService = new RemoteRefreshSegmentPressureService(clusterService, Settings.EMPTY);
        pressureService.afterIndexShardCreated(indexShard);

        RemoteSegmentTransferTracker pressureTracker = pressureService.getRemoteRefreshSegmentTracker(shardId);
        pressureTracker.updateLocalRefreshSeqNo(6);

        // 1. time lag more than dynamic threshold
        pressureTracker.updateRemoteRefreshSeqNo(3);
        AtomicLong sum = new AtomicLong();
        IntStream.range(0, 20).forEach(i -> {
            pressureTracker.addUploadTimeMs(i);
            sum.addAndGet(i);
        });
        double avg = (double) sum.get() / 20;
        long currentMs = System.nanoTime() / 1_000_000;
        pressureTracker.updateLocalRefreshTimeMs((long) (currentMs + 12 * avg));
        pressureTracker.updateRemoteRefreshTimeMs(currentMs);
        Exception e = assertThrows(OpenSearchRejectedExecutionException.class, () -> pressureService.validateSegmentsUploadLag(shardId));
        assertTrue(e.getMessage().contains("due to remote segments lagging behind local segments"));
        assertTrue(e.getMessage().contains("time_lag:114 ms dynamic_time_lag_threshold:95.0 ms"));

        pressureTracker.updateRemoteRefreshTimeMs((long) (currentMs + 2 * avg));
        pressureService.validateSegmentsUploadLag(shardId);

        // 2. bytes lag more than dynamic threshold
        sum.set(0);
        IntStream.range(0, 20).forEach(i -> {
            pressureTracker.addUploadBytes(i);
            sum.addAndGet(i);
        });
        avg = (double) sum.get() / 20;
        Map<String, Long> nameSizeMap = new HashMap<>();
        nameSizeMap.put("a", (long) (12 * avg));
        pressureTracker.setLatestLocalFileNameLengthMap(nameSizeMap);
        e = assertThrows(OpenSearchRejectedExecutionException.class, () -> pressureService.validateSegmentsUploadLag(shardId));
        assertTrue(e.getMessage().contains("due to remote segments lagging behind local segments"));
        assertTrue(e.getMessage().contains("bytes_lag:114 dynamic_bytes_lag_threshold:95.0"));

        nameSizeMap.put("a", (long) (2 * avg));
        pressureTracker.setLatestLocalFileNameLengthMap(nameSizeMap);
        pressureService.validateSegmentsUploadLag(shardId);

        // 3. Consecutive failures more than the limit
        IntStream.range(0, 5).forEach(ignore -> pressureTracker.incrementTotalUploadsFailed());
        pressureService.validateSegmentsUploadLag(shardId);
        pressureTracker.incrementTotalUploadsFailed();
        e = assertThrows(OpenSearchRejectedExecutionException.class, () -> pressureService.validateSegmentsUploadLag(shardId));
        assertTrue(e.getMessage().contains("due to remote segments lagging behind local segments"));
        assertTrue(e.getMessage().contains("failure_streak_count:6 min_consecutive_failure_threshold:5"));
        pressureTracker.incrementTotalUploadsSucceeded();
        pressureService.validateSegmentsUploadLag(shardId);
    }

    private static IndexShard createIndexShard(ShardId shardId, boolean remoteStoreEnabled) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, String.valueOf(remoteStoreEnabled))
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        Store store = mock(Store.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.store()).thenReturn(store);
        return indexShard;
    }
}
