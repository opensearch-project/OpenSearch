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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;

import java.util.Locale;
import java.util.Set;

/**
 * Remote upload back pressure service.
 *
 * @opensearch.internal
 */
public class RemoteUploadPressureService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RemoteUploadPressureService.class);

    private final RemoteUploadPressureSettings remoteUploadPressureSettings;

    @Inject
    public RemoteUploadPressureService(ClusterService clusterService, Settings settings) {
        remoteUploadPressureSettings = new RemoteUploadPressureSettings(clusterService, settings, this);
    }

    public RemoteSegmentUploadShardStatsTracker getStatsTracker(ShardId shardId) {
        return RemoteUploadStatsTracker.INSTANCE.getStatsTracker(shardId);
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        RemoteUploadStatsTracker.INSTANCE.createStatsTracker(
            indexShard.shardId(),
            remoteUploadPressureSettings.getUploadBytesMovingAverageWindowSize(),
            remoteUploadPressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
            remoteUploadPressureSettings.getUploadTimeMovingAverageWindowSize()
        );
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        RemoteUploadStatsTracker.INSTANCE.remove(shardId);
    }

    public boolean isSegmentsUploadBackpressureEnabled() {
        return remoteUploadPressureSettings.isRemoteSegmentUploadPressureEnabled();
    }

    public void validateSegmentsUploadLag(ShardId shardId) {
        RemoteSegmentUploadShardStatsTracker statsTracker = getStatsTracker(shardId);
        // Check if refresh checkpoint (a.k.a. seq number) lag is 2 or below - this is to handle segment merges that can
        // increase the bytes to upload almost suddenly.
        if (statsTracker.getSeqNoLag() <= 2) {
            return;
        }

        // Check if the remote store seq no lag is above the min seq no lag limit
        validateSeqNoLag(statsTracker, shardId);

        // Check if the remote store is lagging more than the upload bytes average multiplied by a variance factor
        validateBytesLag(statsTracker, shardId);

        // Check if the remote store is lagging more than the upload time average multiplied by a variance factor
        validateTimeLag(statsTracker, shardId);

        // Check if consecutive failure limit has been breached
        validateConsecutiveFailureLimitBreached(statsTracker, shardId);
    }

    private void validateSeqNoLag(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        // Check if the remote store seq no lag is above the min seq no lag limit
        if (statsTracker.getSeqNoLag() > remoteUploadPressureSettings.getMinSeqNoLagLimit()) {
            statsTracker.incrementRejectionCount();
            throw new OpenSearchRejectedExecutionException(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "remote_refresh_seq_no:%s local_refresh_seq_no:%s",
                    shardId,
                    statsTracker.getRemoteRefreshSeqNo(),
                    statsTracker.getLocalRefreshSeqNo()
                )
            );
        }
    }

    private void validateBytesLag(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        if (statsTracker.isUploadBytesAverageReady() == false) {
            return;
        }
        double dynamicBytesLagThreshold = statsTracker.getUploadBytesAverage() * remoteUploadPressureSettings
            .getBytesLagVarianceThreshold();
        long bytesLag = statsTracker.getBytesLag();
        if (bytesLag > dynamicBytesLagThreshold) {
            statsTracker.incrementRejectionCount();
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

    private void validateTimeLag(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        if (statsTracker.isUploadTimeAverageReady() == false) {
            return;
        }
        long timeLag = statsTracker.getTimeLag();
        double dynamicTimeLagThreshold = statsTracker.getUploadTimeAverage() * remoteUploadPressureSettings.getTimeLagVarianceThreshold();
        if (timeLag > dynamicTimeLagThreshold) {
            statsTracker.incrementRejectionCount();
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

    private void validateConsecutiveFailureLimitBreached(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        int failureStreakCount = statsTracker.getConsecutiveFailureCount();
        int minConsecutiveFailureThreshold = remoteUploadPressureSettings.getMinConsecutiveFailuresLimit();
        if (failureStreakCount > minConsecutiveFailureThreshold) {
            statsTracker.incrementRejectionCount();
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
        Set<ShardId> shardIds = RemoteUploadStatsTracker.INSTANCE.getAllShardIds();
        shardIds.forEach(shardId -> {
            RemoteUploadStatsTracker.INSTANCE.getStatsTracker(shardId).updateUploadBytesMovingAverageWindowSize(updatedSize);
        });
    }

    void updateUploadBytesPerSecMovingAverageWindowSize(int updatedSize) {
        Set<ShardId> shardIds = RemoteUploadStatsTracker.INSTANCE.getAllShardIds();
        shardIds.forEach(shardId -> {
            RemoteUploadStatsTracker.INSTANCE.getStatsTracker(shardId).updateUploadBytesPerSecMovingAverageWindowSize(updatedSize);
        });
    }

    void updateUploadTimeMovingAverageWindowSize(int updatedSize) {
        Set<ShardId> shardIds = RemoteUploadStatsTracker.INSTANCE.getAllShardIds();
        shardIds.forEach(shardId -> {
            RemoteUploadStatsTracker.INSTANCE.getStatsTracker(shardId).updateUploadTimeMovingAverageWindowSize(updatedSize);
        });
    }
}
