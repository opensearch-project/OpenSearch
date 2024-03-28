/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexShard;

public class RemoteStoreStatsTrackerFactoryTests extends OpenSearchTestCase {
    private ThreadPool threadPool;
    private ClusterService clusterService;
    private Settings settings;
    private ShardId shardId;
    private IndexShard indexShard;
    private RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId("index", "uuid", 0);
        indexShard = createIndexShard(shardId, true);
        threadPool = new TestThreadPool(getTestName());
        settings = Settings.builder()
            .put(
                RemoteStoreStatsTrackerFactory.MOVING_AVERAGE_WINDOW_SIZE.getKey(),
                RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE
            )
            .build();
        clusterService = ClusterServiceUtils.createClusterService(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(clusterService, settings);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testAfterIndexShardCreatedForRemoteBackedIndex() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        assertNotNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId()));
    }

    public void testAfterIndexShardCreatedForNonRemoteBackedIndex() {
        indexShard = createIndexShard(shardId, false);
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        assertNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(indexShard.shardId()));
    }

    public void testAfterIndexShardClosed() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        assertNotNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId));
        remoteStoreStatsTrackerFactory.afterIndexShardClosed(shardId, indexShard, indexShard.indexSettings().getSettings());
        assertNull(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId));
    }

    public void testGetConfiguredSettings() {
        assertEquals(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            remoteStoreStatsTrackerFactory.getMovingAverageWindowSize()
        );
    }

    public void testInvalidMovingAverageWindowSize() {
        Settings settings = Settings.builder()
            .put(
                RemoteStoreStatsTrackerFactory.MOVING_AVERAGE_WINDOW_SIZE.getKey(),
                RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE - 1
            )
            .build();
        assertThrows(
            "Failed to parse value",
            IllegalArgumentException.class,
            () -> new RemoteStoreStatsTrackerFactory(
                ClusterServiceUtils.createClusterService(
                    settings,
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    threadPool
                ),
                settings
            )
        );
    }

    public void testUpdateAfterGetConfiguredSettings() {
        assertEquals(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
            remoteStoreStatsTrackerFactory.getMovingAverageWindowSize()
        );

        Settings newSettings = Settings.builder().put(RemoteStoreStatsTrackerFactory.MOVING_AVERAGE_WINDOW_SIZE.getKey(), 102).build();

        clusterService.getClusterSettings().applySettings(newSettings);

        // Check moving average window size updated
        assertEquals(102, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
    }

    public void testGetDefaultSettings() {
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(
            ClusterServiceUtils.createClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool
            ),
            Settings.EMPTY
        );
        // Check moving average window size updated
        assertEquals(
            RemoteStoreStatsTrackerFactory.Defaults.MOVING_AVERAGE_WINDOW_SIZE,
            remoteStoreStatsTrackerFactory.getMovingAverageWindowSize()
        );
    }
}
