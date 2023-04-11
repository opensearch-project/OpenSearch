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

/**
 * Remote upload back pressure service.
 *
 * @opensearch.internal
 */
public class RemoteUploadPressureService implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(RemoteUploadPressureService.class);

    private final RemoteUploadPressureSettings remoteUploadPressureSettings;

    private final RemoteUploadStatsTracker remoteUploadStatsTracker;

    @Inject
    public RemoteUploadPressureService(ClusterService clusterService, Settings settings) {
        remoteUploadStatsTracker = new RemoteUploadStatsTracker();
        remoteUploadPressureSettings = new RemoteUploadPressureSettings(clusterService, settings);
    }

    public RemoteSegmentUploadShardStatsTracker getStatsTracker(ShardId shardId) {
        return remoteUploadStatsTracker.getStatsTracker(shardId);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        remoteUploadStatsTracker.remove(shardId);
    }

    public boolean isSegmentsUploadBackpressureEnabled() {
        return remoteUploadPressureSettings.isRemoteSegmentUploadPressureEnabled();
    }

    public void validateSegmentsUploadLag(ShardId shardId) {
        RemoteSegmentUploadShardStatsTracker statsTracker = getStatsTracker(shardId);
        // Check if refresh checkpoint (a.k.a. seq number) lag is below 3 - this is to handle segment merges that can increase the bytes to
        // upload almost suddenly.
        long seqNoLag = statsTracker.getLocalRefreshSeqNo() - statsTracker.getRemoteRefreshSeqNo();
        if (seqNoLag <= 2) {
            return;
        }

        if (seqNoLag > remoteUploadPressureSettings.getMinSeqNoLagLimit()) {
            rejectRequest(String.format(Locale.ROOT, "rejected execution on primary shard:%s reason:%s", shardId, "seqNoLag"));
        }

    }

    private void rejectRequest(String rejectionReason) {
        throw new OpenSearchRejectedExecutionException(rejectionReason, false);
    }
}
