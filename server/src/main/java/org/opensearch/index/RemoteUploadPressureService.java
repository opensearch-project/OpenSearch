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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Remote upload back pressure service.
 *
 * @opensearch.internal
 */
public class RemoteUploadPressureService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RemoteUploadPressureService.class);

    private final RemoteUploadPressureSettings remoteUploadPressureSettings;


    private final Map<ShardId, AtomicLong> rejectionCount = ConcurrentCollections.newConcurrentMap();

    @Inject
    public RemoteUploadPressureService(ClusterService clusterService, Settings settings) {
        remoteUploadPressureSettings = new RemoteUploadPressureSettings(clusterService, settings);
    }

    public RemoteSegmentUploadShardStatsTracker getStatsTracker(ShardId shardId) {
        return RemoteUploadStatsTracker.INSTANCE.getStatsTracker(shardId);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        RemoteUploadStatsTracker.INSTANCE.remove(shardId);
        rejectionCount.remove(shardId);
    }

    public boolean isSegmentsUploadBackpressureEnabled() {
        return remoteUploadPressureSettings.isRemoteSegmentUploadPressureEnabled();
    }

    public void validateSegmentsUploadLag(ShardId shardId) {
        RemoteSegmentUploadShardStatsTracker statsTracker = getStatsTracker(shardId);
        // Check if refresh checkpoint (a.k.a. seq number) lag is 2 or below - this is to handle segment merges that can
        // increase the bytes to upload almost suddenly.
        if (statsTracker.getLocalRefreshSeqNo() - statsTracker.getRemoteRefreshSeqNo() <= 2) {
            return;
        }

        // Check if the remote store seq no lag is above the min seq no lag limit
        validateSeqNoLag(statsTracker, shardId);

        // Check if the remote store is lagging more than the upload bytes average multiplied by a variance factor
        validateBytesBehindLag(statsTracker, shardId);

        // Check if the remote store is lagging more than the upload time average multiplied by a variance factor
        validateTimeBehindLag(statsTracker, shardId);

        // Check if consecutive failure limit has been breached
        validateConsecutiveFailureLimitBreached(statsTracker, shardId);
    }

    private void validateSeqNoLag(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        long seqNoLag = statsTracker.getLocalRefreshSeqNo() - statsTracker.getRemoteRefreshSeqNo();
        // Check if the remote store seq no lag is above the min seq no lag limit
        if (seqNoLag > remoteUploadPressureSettings.getMinSeqNoLagLimit()) {
            rejectRequest(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "remote_refresh_seq_no:%s local_refresh_seq_no:%s",
                    shardId,
                    statsTracker.getRemoteRefreshSeqNo(),
                    statsTracker.getLocalRefreshSeqNo()
                ),
                shardId
            );
        }
    }

    private void validateBytesBehindLag(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        if (statsTracker.isUploadBytesAverageReady() == false) {
            return;
        }
        Map<String, Long> localFileSizeMap = statsTracker.getLatestLocalFileNameLengthMap();
        Set<String> remoteFiles = statsTracker.getLatestUploadFiles();
        Set<String> filesNotYetUploaded = localFileSizeMap.keySet()
            .stream()
            .filter(f -> remoteFiles.contains(f) == false)
            .collect(Collectors.toSet());
        long bytesBehind = filesNotYetUploaded.stream().map(localFileSizeMap::get).mapToLong(Long::longValue).sum();
        double dynamicBytesBehindThreshold = statsTracker.getUploadBytesAverage() * remoteUploadPressureSettings
            .getBytesBehindVarianceThreshold();
        if (bytesBehind > dynamicBytesBehindThreshold) {
            rejectRequest(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "bytes_behind:%s dynamic_bytes_behind_threshold:%s",
                    shardId,
                    bytesBehind,
                    dynamicBytesBehindThreshold
                ),
                shardId
            );
        }
    }

    private void validateTimeBehindLag(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        if (statsTracker.isUploadTimeAverageReady() == false) {
            return;
        }
        long timeBehind = statsTracker.getLocalRefreshTime() - statsTracker.getRemoteRefreshTime();
        double dynamicTimeBehindThreshold = statsTracker.getUploadTimeAverage() * remoteUploadPressureSettings
            .getTimeBehindVarianceThreshold();
        if (timeBehind > dynamicTimeBehindThreshold) {
            rejectRequest(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "time_behind:%s ns dynamic_time_behind_threshold:%s ns",
                    shardId,
                    timeBehind,
                    dynamicTimeBehindThreshold
                ),
                shardId
            );
        }
    }

    private void validateConsecutiveFailureLimitBreached(RemoteSegmentUploadShardStatsTracker statsTracker, ShardId shardId) {
        int failureStreakCount = statsTracker.getConsecutiveFailureCount();
        int minConsecutiveFailureThreshold = remoteUploadPressureSettings.getMinConsecutiveFailuresLimit();
        if (failureStreakCount > minConsecutiveFailureThreshold) {
            rejectRequest(
                String.format(
                    Locale.ROOT,
                    "rejected execution on primary shard:%s due to remote segments lagging behind local segments."
                        + "failure_streak_count:%s min_consecutive_failure_threshold:%s",
                    shardId,
                    failureStreakCount,
                    minConsecutiveFailureThreshold
                ),
                shardId
            );
        }
    }

    private void rejectRequest(String rejectionReason, ShardId shardId) {
        rejectionCount.computeIfAbsent(shardId, k -> new AtomicLong()).incrementAndGet();
        throw new OpenSearchRejectedExecutionException(rejectionReason, false);
    }
}
