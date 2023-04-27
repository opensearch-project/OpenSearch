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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Service used to validate if the incoming indexing request should be rejected based on the {@link RemoteRefreshSegmentTracker}.
 *
 * @opensearch.internal
 */
public class RemoteRefreshSegmentPressureService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RemoteRefreshSegmentPressureService.class);

    /**
     * Keeps map of remote-backed index shards and their corresponding backpressure tracker.
     */
    private final Map<ShardId, RemoteRefreshSegmentTracker> trackerMap = ConcurrentCollections.newConcurrentMap();

    /**
     * Remote refresh segment pressure settings which is used for creation of the backpressure tracker and as well as rejection.
     */
    private final RemoteRefreshSegmentPressureSettings pressureSettings;

    private final List<LagValidator> lagValidators;

    @Inject
    public RemoteRefreshSegmentPressureService(ClusterService clusterService, Settings settings) {
        pressureSettings = new RemoteRefreshSegmentPressureSettings(clusterService, settings, this);
        lagValidators = Arrays.asList(
            new RefreshSeqNoLagValidator(pressureSettings),
            new BytesLagValidator(pressureSettings),
            new TimeLagValidator(pressureSettings),
            new ConsecutiveFailureValidator(pressureSettings)
        );
    }

    /**
     * Get {@code RemoteRefreshSegmentTracker} only if the underlying Index has remote segments integration enabled.
     *
     * @param shardId shard id
     * @return the tracker if index is remote-backed, else null.
     */
    public RemoteRefreshSegmentTracker getRemoteRefreshSegmentTracker(ShardId shardId) {
        return trackerMap.get(shardId);
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        if (indexShard.indexSettings().isRemoteStoreEnabled() == false) {
            return;
        }
        ShardId shardId = indexShard.shardId();
        trackerMap.put(
            shardId,
            new RemoteRefreshSegmentTracker(
                shardId,
                pressureSettings.getUploadBytesMovingAverageWindowSize(),
                pressureSettings.getUploadBytesPerSecMovingAverageWindowSize(),
                pressureSettings.getUploadTimeMovingAverageWindowSize()
            )
        );
        logger.trace("Created tracker for shardId={}", shardId);
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
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
        RemoteRefreshSegmentTracker remoteRefreshSegmentTracker = getRemoteRefreshSegmentTracker(shardId);
        // Check if refresh checkpoint (a.k.a. seq number) lag is 2 or below - this is to handle segment merges that can
        // increase the bytes to upload almost suddenly.
        if (remoteRefreshSegmentTracker.getSeqNoLag() <= 1) {
            return;
        }

        for (LagValidator lagValidator : lagValidators) {
            if (lagValidator.validate(remoteRefreshSegmentTracker, shardId) == false) {
                remoteRefreshSegmentTracker.incrementRejectionCount(lagValidator.name());
                throw new OpenSearchRejectedExecutionException(lagValidator.rejectionMessage(remoteRefreshSegmentTracker, shardId));
            }
        }
    }

    void updateUploadBytesMovingAverageWindowSize(int updatedSize) {
        updateMovingAverageWindowSize(RemoteRefreshSegmentTracker::updateUploadBytesMovingAverageWindowSize, updatedSize);
    }

    void updateUploadBytesPerSecMovingAverageWindowSize(int updatedSize) {
        updateMovingAverageWindowSize(RemoteRefreshSegmentTracker::updateUploadBytesPerSecMovingAverageWindowSize, updatedSize);
    }

    void updateUploadTimeMsMovingAverageWindowSize(int updatedSize) {
        updateMovingAverageWindowSize(RemoteRefreshSegmentTracker::updateUploadTimeMsMovingAverageWindowSize, updatedSize);
    }

    void updateMovingAverageWindowSize(BiConsumer<RemoteRefreshSegmentTracker, Integer> biConsumer, int updatedSize) {
        trackerMap.values().forEach(tracker -> biConsumer.accept(tracker, updatedSize));
    }

    /**
     * Abstract class for validating if lag is acceptable or not.
     *
     * @opensearch.internal
     */
    private static abstract class LagValidator {

        final RemoteRefreshSegmentPressureSettings pressureSettings;

        private LagValidator(RemoteRefreshSegmentPressureSettings pressureSettings) {
            this.pressureSettings = pressureSettings;
        }

        /**
         * Validates the lag and returns value accordingly.
         *
         * @param pressureTracker tracker which holds information about the shard.
         * @param shardId         shard id of the {@code IndexShard} currently being validated.
         * @return true if successfully validated that lag is acceptable.
         */
        abstract boolean validate(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId);

        /**
         * Returns the name of the lag validator.
         *
         * @return the name using class name.
         */
        final String name() {
            return this.getClass().getSimpleName();
        }

        abstract String rejectionMessage(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId);
    }

    /**
     * Check if the remote store seq no lag is above the min seq no lag limit
     *
     * @opensearch.internal
     */
    private static class RefreshSeqNoLagValidator extends LagValidator {

        private RefreshSeqNoLagValidator(RemoteRefreshSegmentPressureSettings pressureSettings) {
            super(pressureSettings);
        }

        @Override
        public boolean validate(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            // Check if the remote store seq no lag is above the min seq no lag limit
            return pressureTracker.getSeqNoLag() <= pressureSettings.getMinSeqNoLagLimit();
        }

        @Override
        String rejectionMessage(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            return String.format(
                Locale.ROOT,
                "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                    + "remote_refresh_seq_no:%s local_refresh_seq_no:%s",
                shardId,
                pressureTracker.getRemoteRefreshSeqNo(),
                pressureTracker.getLocalRefreshSeqNo()
            );
        }
    }

    /**
     * Check if the remote store is lagging more than the upload bytes average multiplied by a variance factor
     *
     * @opensearch.internal
     */
    private static class BytesLagValidator extends LagValidator {

        private BytesLagValidator(RemoteRefreshSegmentPressureSettings pressureSettings) {
            super(pressureSettings);
        }

        @Override
        public boolean validate(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            if (pressureTracker.isUploadBytesAverageReady() == false) {
                logger.trace("upload bytes moving average is not ready");
                return true;
            }
            double dynamicBytesLagThreshold = pressureTracker.getUploadBytesAverage() * pressureSettings.getBytesLagVarianceFactor();
            long bytesLag = pressureTracker.getBytesLag();
            return bytesLag <= dynamicBytesLagThreshold;
        }

        @Override
        public String rejectionMessage(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            double dynamicBytesLagThreshold = pressureTracker.getUploadBytesAverage() * pressureSettings.getBytesLagVarianceFactor();
            return String.format(
                Locale.ROOT,
                "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                    + "bytes_lag:%s dynamic_bytes_lag_threshold:%s",
                shardId,
                pressureTracker.getBytesLag(),
                dynamicBytesLagThreshold
            );
        }
    }

    /**
     * Check if the remote store is lagging more than the upload time average multiplied by a variance factor
     *
     * @opensearch.internal
     */
    private static class TimeLagValidator extends LagValidator {

        private TimeLagValidator(RemoteRefreshSegmentPressureSettings pressureSettings) {
            super(pressureSettings);
        }

        @Override
        public boolean validate(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            if (pressureTracker.isUploadTimeMsAverageReady() == false) {
                logger.trace("upload time moving average is not ready");
                return true;
            }
            long timeLag = pressureTracker.getTimeMsLag();
            double dynamicTimeLagThreshold = pressureTracker.getUploadTimeMsAverage() * pressureSettings.getUploadTimeLagVarianceFactor();
            return timeLag <= dynamicTimeLagThreshold;
        }

        @Override
        public String rejectionMessage(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            double dynamicTimeLagThreshold = pressureTracker.getUploadTimeMsAverage() * pressureSettings.getUploadTimeLagVarianceFactor();
            return String.format(
                Locale.ROOT,
                "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                    + "time_lag:%s ms dynamic_time_lag_threshold:%s ms",
                shardId,
                pressureTracker.getTimeMsLag(),
                dynamicTimeLagThreshold
            );
        }
    }

    /**
     * Check if consecutive failure limit has been breached
     *
     * @opensearch.internal
     */
    private static class ConsecutiveFailureValidator extends LagValidator {

        private ConsecutiveFailureValidator(RemoteRefreshSegmentPressureSettings pressureSettings) {
            super(pressureSettings);
        }

        @Override
        public boolean validate(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            int failureStreakCount = pressureTracker.getConsecutiveFailureCount();
            int minConsecutiveFailureThreshold = pressureSettings.getMinConsecutiveFailuresLimit();
            return failureStreakCount <= minConsecutiveFailureThreshold;
        }

        @Override
        public String rejectionMessage(RemoteRefreshSegmentTracker pressureTracker, ShardId shardId) {
            return String.format(
                Locale.ROOT,
                "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                    + "failure_streak_count:%s min_consecutive_failure_threshold:%s",
                shardId,
                pressureTracker.getConsecutiveFailureCount(),
                pressureSettings.getMinConsecutiveFailuresLimit()
            );
        }
    }
}
