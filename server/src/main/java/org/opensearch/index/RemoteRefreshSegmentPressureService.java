/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;

import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Service used to validate if the incoming indexing request should be rejected based on the {@link RemoteRefreshSegmentPressureTracker}.
 */
public class RemoteRefreshSegmentPressureService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RemoteRefreshSegmentPressureService.class);

    /**
     * Keeps map of remote-backed index shards and their corresponding backpressure tracker.
     */
    private final Map<ShardId, RemoteRefreshSegmentPressureTracker> trackerMap = ConcurrentCollections.newConcurrentMap();

    /**
     * Remote refresh segment pressure settings which is used for creation of the backpressure tracker and as well as rejection.
     */
    private final RemoteRefreshSegmentPressureSettings pressureSettings;

    @Inject
    public RemoteRefreshSegmentPressureService(ClusterService clusterService, Settings settings) {
        pressureSettings = new RemoteRefreshSegmentPressureSettings(clusterService, settings, this);
    }

    /**
     * Get {@code RemoteRefreshSegmentPressureTracker} only if the underlying Index has remote segments integration enabled.
     *
     * @param shardId shard id
     * @return the tracker if index is remote-backed, else null.
     */
    public RemoteRefreshSegmentPressureTracker getStatsTracker(ShardId shardId) {
        return trackerMap.get(shardId);
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        if (indexShard.indexSettings().isRemoteStoreEnabled() == false) {
            return;
        }
        ShardId shardId = indexShard.shardId();
        trackerMap.put(shardId, new RemoteRefreshSegmentPressureTracker(shardId, pressureSettings));
        logger.trace("Created tracker for shardId={}", shardId);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        if (indexShard.indexSettings().isRemoteStoreEnabled() == false) {
            return;
        }
        trackerMap.remove(shardId);
        logger.trace("Deleted tracker for shardId={}", shardId);
    }

    /**
     * Check if remote refresh segments backpressure is enabled. This is backed by a cluster level setting.
     *
     * @return true if enabled, else false.
     */
    public boolean isSegmentsUploadBackpressureEnabled() {
        return pressureSettings.isRemoteRefreshSegmentPressureEnabled();
    }

    public void validateSegmentsUploadLag(ShardId shardId) {
        RemoteRefreshSegmentPressureTracker pressureTracker = getStatsTracker(shardId);
        // Check if refresh checkpoint (a.k.a. seq number) lag is 2 or below - this is to handle segment merges that can
        // increase the bytes to upload almost suddenly.
        if (pressureTracker.getSeqNoLag() <= 2) {
            return;
        }

        // Check if the remote store seq no lag is above the min seq no lag limit
        validateSeqNoLag(pressureTracker, shardId);

        // Check if the remote store is lagging more than the upload bytes average multiplied by a variance factor
        validateBytesLag(pressureTracker, shardId);

        // Check if the remote store is lagging more than the upload time average multiplied by a variance factor
        validateTimeLag(pressureTracker, shardId);

        // Check if consecutive failure limit has been breached
        validateConsecutiveFailureLimitBreached(pressureTracker, shardId);
    }

    private void validateSeqNoLag(RemoteRefreshSegmentPressureTracker pressureTracker, ShardId shardId) {
        // Check if the remote store seq no lag is above the min seq no lag limit
        if (pressureTracker.getSeqNoLag() > pressureSettings.getMinSeqNoLagLimit()) {
            pressureTracker.incrementRejectionCount();
            throw new OpenSearchRejectedExecutionException(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "remote_refresh_seq_no:%s local_refresh_seq_no:%s",
                    shardId,
                    pressureTracker.getRemoteRefreshSeqNo(),
                    pressureTracker.getLocalRefreshSeqNo()
                )
            );
        }
    }

    private void validateBytesLag(RemoteRefreshSegmentPressureTracker pressureTracker, ShardId shardId) {
        if (pressureTracker.isUploadBytesAverageReady() == false) {
            return;
        }
        double dynamicBytesLagThreshold = pressureTracker.getUploadBytesAverage() * pressureSettings.getBytesLagVarianceThreshold();
        long bytesLag = pressureTracker.getBytesLag();
        if (bytesLag > dynamicBytesLagThreshold) {
            pressureTracker.incrementRejectionCount();
            throw new OpenSearchRejectedExecutionException(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "bytes_lag:%s dynamic_bytes_lag_threshold:%s",
                    shardId,
                    bytesLag,
                    dynamicBytesLagThreshold
                )
            );
        }
    }

    private void validateTimeLag(RemoteRefreshSegmentPressureTracker pressureTracker, ShardId shardId) {
        if (pressureTracker.isUploadTimeMsAverageReady() == false) {
            return;
        }
        long timeLag = pressureTracker.getTimeMsLag();
        double dynamicTimeLagThreshold = pressureTracker.getUploadTimeMsAverage() * pressureSettings.getTimeLagVarianceThreshold();
        if (timeLag > dynamicTimeLagThreshold) {
            pressureTracker.incrementRejectionCount();
            throw new OpenSearchRejectedExecutionException(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "time_lag:%s ns dynamic_time_lag_threshold:%s ns",
                    shardId,
                    timeLag,
                    dynamicTimeLagThreshold
                )
            );
        }
    }

    private void validateConsecutiveFailureLimitBreached(RemoteRefreshSegmentPressureTracker pressureTracker, ShardId shardId) {
        int failureStreakCount = pressureTracker.getConsecutiveFailureCount();
        int minConsecutiveFailureThreshold = pressureSettings.getMinConsecutiveFailuresLimit();
        if (failureStreakCount > minConsecutiveFailureThreshold) {
            pressureTracker.incrementRejectionCount();
            throw new OpenSearchRejectedExecutionException(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "failure_streak_count:%s min_consecutive_failure_threshold:%s",
                    shardId,
                    failureStreakCount,
                    minConsecutiveFailureThreshold
                )
            );
        }
    }

    void updateUploadBytesMovingAverageWindowSize(int updatedSize) {
        updateMovingAverageWindowSize(RemoteRefreshSegmentPressureTracker::updateUploadBytesMovingAverageWindowSize, updatedSize);
    }

    void updateUploadBytesPerSecMovingAverageWindowSize(int updatedSize) {
        updateMovingAverageWindowSize(RemoteRefreshSegmentPressureTracker::updateUploadBytesPerSecMovingAverageWindowSize, updatedSize);
    }

    void updateUploadTimeMsMovingAverageWindowSize(int updatedSize) {
        updateMovingAverageWindowSize(RemoteRefreshSegmentPressureTracker::updateUploadTimeMsMovingAverageWindowSize, updatedSize);
    }

    void updateMovingAverageWindowSize(BiConsumer<RemoteRefreshSegmentPressureTracker, Integer> biConsumer, int updatedSize) {
        trackerMap.values().forEach(tracker -> biConsumer.accept(tracker, updatedSize));
    }
}
