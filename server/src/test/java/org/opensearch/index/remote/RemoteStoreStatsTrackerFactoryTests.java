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
    private RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shardId = new ShardId("index", "uuid", 0);
        indexShard = createIndexShard(shardId, true);
        remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(Settings.EMPTY);
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

    public void testUpdateMovingAverageWindowSizeMinAllowed() {
        remoteStoreStatsTrackerFactory.afterIndexShardCreated(indexShard);

        int updatedSize = RemoteStorePressureSettings.Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE;
        remoteStoreStatsTrackerFactory.updateMovingAverageWindowSize(
            RemoteSegmentTransferTracker::updateMovingAverageWindowSize,
            updatedSize
        );
        assertEquals(updatedSize, remoteStoreStatsTrackerFactory.getMovingAverageWindowSize());
    }
}
