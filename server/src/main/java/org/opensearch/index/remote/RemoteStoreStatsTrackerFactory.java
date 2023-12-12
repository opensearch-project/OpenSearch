/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;

import java.util.Map;

/**
 * Factory to manage stats trackers for Remote Store operations
 *
 * @opensearch.api
 */
@PublicApi(since = "2.10.0")
public class RemoteStoreStatsTrackerFactory implements IndexEventListener {
    static class Defaults {
        static final int MOVING_AVERAGE_WINDOW_SIZE = 20;
        static final int MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE = 5;
    }

    public static final Setting<Integer> MOVING_AVERAGE_WINDOW_SIZE = Setting.intSetting(
        "remote_store.moving_average_window_size",
        Defaults.MOVING_AVERAGE_WINDOW_SIZE,
        Defaults.MOVING_AVERAGE_WINDOW_SIZE_MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(RemoteStoreStatsTrackerFactory.class);

    /**
     * Number of data points to consider for a moving average statistic
     */
    private volatile int movingAverageWindowSize;

    /**
     * Keeps map of remote-backed index shards and their corresponding stats tracker.
     */
    private final Map<ShardId, RemoteSegmentTransferTracker> remoteSegmentTrackerMap = ConcurrentCollections.newConcurrentMap();

    /**
     * Keeps map of remote-backed index shards and their corresponding stats tracker.
     */
    private final Map<ShardId, RemoteTranslogTransferTracker> remoteTranslogTrackerMap = ConcurrentCollections.newConcurrentMap();

    public RemoteStoreStatsTrackerFactory(ClusterService clusterService, Settings settings) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.movingAverageWindowSize = MOVING_AVERAGE_WINDOW_SIZE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MOVING_AVERAGE_WINDOW_SIZE, this::updateMovingAverageWindowSize);
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        if (indexShard.indexSettings().isRemoteStoreEnabled() == false) {
            return;
        }
        ShardId shardId = indexShard.shardId();
        remoteSegmentTrackerMap.put(
            shardId,
            new RemoteSegmentTransferTracker(shardId, indexShard.store().getDirectoryFileTransferTracker(), movingAverageWindowSize)
        );
        logger.trace("Created RemoteSegmentTransferTracker for shardId={}", shardId);
        remoteTranslogTrackerMap.put(shardId, new RemoteTranslogTransferTracker(shardId, movingAverageWindowSize));
        logger.trace("Created RemoteTranslogTransferTracker for shardId={}", shardId);
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        RemoteSegmentTransferTracker remoteSegmentTransferTracker = remoteSegmentTrackerMap.remove(shardId);
        if (remoteSegmentTransferTracker != null) {
            logger.trace("Deleted RemoteSegmentTransferTracker for shardId={}", shardId);
        }

        RemoteTranslogTransferTracker remoteTranslogTransferTracker = remoteTranslogTrackerMap.remove(shardId);
        if (remoteTranslogTransferTracker != null) {
            logger.trace("Deleted RemoteTranslogTransferTracker for shardId={}", shardId);
        }
    }

    private void updateMovingAverageWindowSize(int updatedSize) {
        remoteSegmentTrackerMap.values().forEach(tracker -> tracker.updateMovingAverageWindowSize(updatedSize));
        remoteTranslogTrackerMap.values().forEach(tracker -> tracker.updateMovingAverageWindowSize(updatedSize));

        // Update movingAverageWindowSize only if the trackers were successfully updated
        movingAverageWindowSize = updatedSize;
    }

    public RemoteSegmentTransferTracker getRemoteSegmentTransferTracker(ShardId shardId) {
        return remoteSegmentTrackerMap.get(shardId);
    }

    public RemoteTranslogTransferTracker getRemoteTranslogTransferTracker(ShardId shardId) {
        return remoteTranslogTrackerMap.get(shardId);
    }

    // visible for testing
    int getMovingAverageWindowSize() {
        return movingAverageWindowSize;
    }
}
