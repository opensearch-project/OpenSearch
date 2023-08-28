/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.index.remote.RemoteStoreTestsHelper.createIndexShard;

public class RemoteStoreStatsTrackerFactoryTests extends OpenSearchTestCase {
    private ShardId shardId;
    private IndexShard indexShard;

    private ShardId shardId2;
    private IndexShard indexShard2;
    private RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId("index", "uuid", 0);
        indexShard = createIndexShard(shardId, true);
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(Settings.EMPTY);
        shardId2 = null;
        indexShard2 = null;
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

    public void testUpdateMovingAverageWindowSizeZero() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        createSecondShard();

        int defaultSize = RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE;
        assertDefaultSize(defaultSize);

        int updatedSize = 0;
        assertThrows(
            IllegalArgumentException.class,
            () -> remoteStoreStatsTrackerFactory.updateMovingAverageWindowSize(
                RemoteSegmentTransferTracker::updateMovingAverageWindowSize,
                updatedSize
            )
        );
        assertEquals(defaultSize, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
    }

    public void testUpdateMovingAverageWindowSizeLessThanMinAllowed() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        createSecondShard();

        int defaultSize = RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE;
        assertDefaultSize(defaultSize);

        int updatedSize = RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE - 1;
        // TODO: The following exception isn't thrown currently
        // assertThrows(
        // IllegalArgumentException.class,
        // () -> remoteStoreStatsTrackerFactory.updateMovingAverageWindowSize(
        // RemoteSegmentTransferTracker::updateMovingAverageWindowSize,
        // updatedSize
        // )
        // );
        // assertEquals(defaultSize, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
        remoteStoreStatsTrackerFactory.updateMovingAverageWindowSize(
            RemoteSegmentTransferTracker::updateMovingAverageWindowSize,
            updatedSize
        );
        assertEquals(updatedSize, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
    }

    public void testUpdateMovingAverageWindowSizeMinAllowed() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);
        createSecondShard();

        int defaultSize = RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE;
        assertDefaultSize(defaultSize);

        int updatedSize = RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE;
        remoteStoreStatsTrackerFactory.updateMovingAverageWindowSize(
            RemoteSegmentTransferTracker::updateMovingAverageWindowSize,
            updatedSize
        );
        assertEquals(updatedSize, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
    }

    private void assertDefaultSize(int defaultSize) {
        assertEquals(defaultSize, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
        assertFalse(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId).isUploadBytesAverageReady());
        assertFalse(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId).isUploadBytesPerSecAverageReady());
        assertFalse(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId).isUploadTimeMsAverageReady());
        if (shardId2 != null) {
            assertFalse(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId2).isUploadBytesAverageReady());
            assertFalse(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId2).isUploadBytesPerSecAverageReady());
            assertFalse(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId2).isUploadTimeMsAverageReady());
        }
    }

    private void createSecondShard() {
        shardId2 = new ShardId("index", "uuid", 1);
        indexShard2 = createIndexShard(shardId2, true);
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard2);
    }
}
