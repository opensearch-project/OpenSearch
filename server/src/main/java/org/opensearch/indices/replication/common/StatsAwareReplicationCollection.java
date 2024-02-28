/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.recovery.PeerRecoveryStats;
import org.opensearch.indices.recovery.PeerRecoveryStatsTracker;
import org.opensearch.threadpool.ThreadPool;

/**
 * Wrapper of {@link ReplicationRef} which attaches {@link PeerRecoveryStatsTracker} to track stats of
 * ongoing relocation.
 *
 * @opensearch.internal
 */
public class StatsAwareReplicationCollection<T extends ReplicationTarget> extends ReplicationCollection<T> {

    private final PeerRecoveryStatsTracker tracker;

    public StatsAwareReplicationCollection(Logger logger, ThreadPool threadPool) {
        super(logger, threadPool);
        this.tracker = new PeerRecoveryStatsTracker();
    }

    @Override
    public boolean cancelForShard(ShardId shardId, String reason) {
        tracker.incrementTotalCancelledRelocation(1);
        return super.cancelForShard(shardId, reason);
    }

    @Override
    public long start(T target, TimeValue activityTimeout) {
        tracker.incrementTotalStartedRelocation(1);
        return super.start(target, activityTimeout);
    }

    @Override
    public T reset(final long id, final TimeValue activityTimeout) {
        tracker.incrementTotalRetriedRelocation(1);
        return super.reset(id, activityTimeout);
    }

    @Override
    public void fail(long id, ReplicationFailedException e, boolean sendShardFailure) {
        tracker.incrementTotalFailedRelocation(1);
        super.fail(id, e, sendShardFailure);
    }

    @Override
    public void markAsDone(long id) {
        tracker.incrementTotalCompletedRelocation(1);
        super.markAsDone(id);
    }

    public PeerRecoveryStats stats() {
        return new PeerRecoveryStats(
            tracker.getTotalStartedRecoveries(),
            tracker.getTotalFailedRecoveries(),
            tracker.getTotalCompletedRecoveries(),
            tracker.getTotalRetriedRecoveries(),
            tracker.getTotalCancelledRecoveries()
        );
    }
}
