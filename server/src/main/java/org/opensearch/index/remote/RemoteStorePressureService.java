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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.shard.ShardId;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Service used to validate if the incoming indexing request should be rejected based on the {@link RemoteSegmentTransferTracker}.
 *
 * @opensearch.internal
 */
public class RemoteStorePressureService {

    private static final Logger logger = LogManager.getLogger(RemoteStorePressureService.class);

    /**
     * Remote refresh segment pressure settings which is used for creation of the backpressure tracker and as well as rejection.
     */
    private final RemoteStorePressureSettings pressureSettings;

    private final List<LagValidator> lagValidators;

    private final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;

    @Inject
    public RemoteStorePressureService(
        ClusterService clusterService,
        Settings settings,
        RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory
    ) {
        pressureSettings = new RemoteStorePressureSettings(clusterService, settings, this);
        lagValidators = Arrays.asList(
            new ConsecutiveFailureValidator(pressureSettings),
            new BytesLagValidator(pressureSettings),
            new TimeLagValidator(pressureSettings)
        );
        this.remoteStoreStatsTrackerFactory = remoteStoreStatsTrackerFactory;
    }

    /**
     * Check if remote refresh segments backpressure is enabled. This is backed by a cluster level setting.
     *
     * @return true if enabled, else false.
     */
    public boolean isSegmentsUploadBackpressureEnabled() {
        return pressureSettings.isRemoteRefreshSegmentPressureEnabled();
    }

    /**
     * Validates if segments are lagging more than the limits. If yes, it would lead to rejections of the requests.
     *
     * @param shardId shardId for which the validation needs to be done.
     */
    public void validateSegmentsUploadLag(ShardId shardId) {
        RemoteSegmentTransferTracker remoteSegmentTransferTracker = remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(shardId);
        // condition 1 - This will be null for non-remote backed indexes
        // condition 2 - This will be zero if the remote store is
        if (remoteSegmentTransferTracker == null || remoteSegmentTransferTracker.getRefreshSeqNoLag() == 0) {
            return;
        }

        for (LagValidator lagValidator : lagValidators) {
            if (lagValidator.validate(remoteSegmentTransferTracker, shardId) == false) {
                remoteSegmentTransferTracker.incrementRejectionCount(lagValidator.name());
                String rejectionMessage = lagValidator.rejectionMessage(remoteSegmentTransferTracker, shardId);
                logger.warn("Rejecting write requests for shard due to remote backpressure:  {}", rejectionMessage);
                throw new OpenSearchRejectedExecutionException(rejectionMessage);
            }
        }
    }

    /**
     * Abstract class for validating if lag is acceptable or not.
     *
     * @opensearch.internal
     */
    private static abstract class LagValidator {

        final RemoteStorePressureSettings pressureSettings;

        private LagValidator(RemoteStorePressureSettings pressureSettings) {
            this.pressureSettings = pressureSettings;
        }

        /**
         * Validates the lag and returns value accordingly.
         *
         * @param pressureTracker tracker which holds information about the shard.
         * @param shardId         shard id of the {@code IndexShard} currently being validated.
         * @return true if successfully validated that lag is acceptable.
         */
        abstract boolean validate(RemoteSegmentTransferTracker pressureTracker, ShardId shardId);

        /**
         * Returns the name of the lag validator.
         *
         * @return the name using class name.
         */
        abstract String name();

        abstract String rejectionMessage(RemoteSegmentTransferTracker pressureTracker, ShardId shardId);
    }

    /**
     * Check if the remote store is lagging more than the upload bytes average multiplied by a variance factor
     *
     * @opensearch.internal
     */
    private static class BytesLagValidator extends LagValidator {

        private static final String NAME = "bytes_lag";

        private BytesLagValidator(RemoteStorePressureSettings pressureSettings) {
            super(pressureSettings);
        }

        @Override
        public boolean validate(RemoteSegmentTransferTracker pressureTracker, ShardId shardId) {
            if (pressureTracker.getRefreshSeqNoLag() <= 1) {
                return true;
            }
            if (pressureTracker.isUploadBytesMovingAverageReady() == false) {
                logger.trace("upload bytes moving average is not ready");
                return true;
            }
            double dynamicBytesLagThreshold = pressureTracker.getUploadBytesMovingAverage() * pressureSettings.getBytesLagVarianceFactor();
            long bytesLag = pressureTracker.getBytesLag();
            return bytesLag <= dynamicBytesLagThreshold;
        }

        @Override
        public String rejectionMessage(RemoteSegmentTransferTracker pressureTracker, ShardId shardId) {
            double dynamicBytesLagThreshold = pressureTracker.getUploadBytesMovingAverage() * pressureSettings.getBytesLagVarianceFactor();
            return String.format(
                Locale.ROOT,
                "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                    + "bytes_lag:%s dynamic_bytes_lag_threshold:%s",
                shardId,
                pressureTracker.getBytesLag(),
                dynamicBytesLagThreshold
            );
        }

        @Override
        String name() {
            return NAME;
        }
    }

    /**
     * Check if the remote store is lagging more than the upload time average multiplied by a variance factor
     *
     * @opensearch.internal
     */
    private static class TimeLagValidator extends LagValidator {

        private static final String NAME = "time_lag";

        private TimeLagValidator(RemoteStorePressureSettings pressureSettings) {
            super(pressureSettings);
        }

        @Override
        public boolean validate(RemoteSegmentTransferTracker pressureTracker, ShardId shardId) {
            if (pressureTracker.getRefreshSeqNoLag() <= 1) {
                return true;
            }
            if (pressureTracker.isUploadTimeMovingAverageReady() == false) {
                return true;
            }
            long timeLag = pressureTracker.getTimeMsLag();
            double dynamicTimeLagThreshold = pressureTracker.getUploadTimeMovingAverage() * pressureSettings
                .getUploadTimeLagVarianceFactor();
            return timeLag <= dynamicTimeLagThreshold;
        }

        @Override
        public String rejectionMessage(RemoteSegmentTransferTracker pressureTracker, ShardId shardId) {
            double dynamicTimeLagThreshold = pressureTracker.getUploadTimeMovingAverage() * pressureSettings
                .getUploadTimeLagVarianceFactor();
            return String.format(
                Locale.ROOT,
                "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                    + "time_lag:%s ms dynamic_time_lag_threshold:%s ms",
                shardId,
                pressureTracker.getTimeMsLag(),
                dynamicTimeLagThreshold
            );
        }

        @Override
        String name() {
            return NAME;
        }
    }

    /**
     * Check if consecutive failure limit has been breached
     *
     * @opensearch.internal
     */
    private static class ConsecutiveFailureValidator extends LagValidator {

        private static final String NAME = "consecutive_failures_lag";

        private ConsecutiveFailureValidator(RemoteStorePressureSettings pressureSettings) {
            super(pressureSettings);
        }

        @Override
        public boolean validate(RemoteSegmentTransferTracker pressureTracker, ShardId shardId) {
            int failureStreakCount = pressureTracker.getConsecutiveFailureCount();
            int minConsecutiveFailureThreshold = pressureSettings.getMinConsecutiveFailuresLimit();
            return failureStreakCount <= minConsecutiveFailureThreshold;
        }

        @Override
        public String rejectionMessage(RemoteSegmentTransferTracker pressureTracker, ShardId shardId) {
            return String.format(
                Locale.ROOT,
                "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                    + "failure_streak_count:%s min_consecutive_failure_threshold:%s",
                shardId,
                pressureTracker.getConsecutiveFailureCount(),
                pressureSettings.getMinConsecutiveFailuresLimit()
            );
        }

        @Override
        String name() {
            return NAME;
        }
    }
}
