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
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Service responsible for applying backpressure for lagging behind replicas when Segment Replication is enabled.
 *
 * @opensearch.internal
 */
public class SegmentReplicationPressureService implements Closeable {

    private volatile boolean isSegmentReplicationBackpressureEnabled;
    private volatile int maxCheckpointsBehind;
    private volatile double maxAllowedStaleReplicas;
    private volatile TimeValue maxReplicationTime;

    private static final Logger logger = LogManager.getLogger(SegmentReplicationPressureService.class);

    /**
     * When enabled, writes will be rejected when a replica shard falls behind by both the MAX_REPLICATION_TIME_SETTING time value and MAX_INDEXING_CHECKPOINTS number of checkpoints.
     * Once a shard falls behind double the MAX_REPLICATION_TIME_SETTING time value it will be marked as failed.
     */
    public static final Setting<Boolean> SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED = Setting.boolSetting(
        "segrep.pressure.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_INDEXING_CHECKPOINTS = Setting.intSetting(
        "segrep.pressure.checkpoint.limit",
        4,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MAX_REPLICATION_TIME_SETTING = Setting.positiveTimeSetting(
        "segrep.pressure.time.limit",
        TimeValue.timeValueMinutes(5),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> MAX_ALLOWED_STALE_SHARDS = Setting.doubleSetting(
        "segrep.pressure.replica.stale.limit",
        .5,
        0,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final IndicesService indicesService;

    private final ThreadPool threadPool;
    private final SegmentReplicationStatsTracker tracker;
    private final ShardStateAction shardStateAction;

    private final AsyncFailStaleReplicaTask failStaleReplicaTask;

    @Inject
    public SegmentReplicationPressureService(
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        ShardStateAction shardStateAction,
        ThreadPool threadPool
    ) {
        this.indicesService = indicesService;
        this.tracker = new SegmentReplicationStatsTracker(this.indicesService);
        this.shardStateAction = shardStateAction;
        this.threadPool = threadPool;

        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        this.isSegmentReplicationBackpressureEnabled = SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            SEGMENT_REPLICATION_INDEXING_PRESSURE_ENABLED,
            this::setSegmentReplicationBackpressureEnabled
        );

        this.maxCheckpointsBehind = MAX_INDEXING_CHECKPOINTS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEXING_CHECKPOINTS, this::setMaxCheckpointsBehind);

        this.maxReplicationTime = MAX_REPLICATION_TIME_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_REPLICATION_TIME_SETTING, this::setMaxReplicationTime);

        this.maxAllowedStaleReplicas = MAX_ALLOWED_STALE_SHARDS.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAX_ALLOWED_STALE_SHARDS, this::setMaxAllowedStaleReplicas);

        this.failStaleReplicaTask = new AsyncFailStaleReplicaTask(this);
    }

    // visible for testing
    AsyncFailStaleReplicaTask getFailStaleReplicaTask() {
        return failStaleReplicaTask;
    }

    public void isSegrepLimitBreached(ShardId shardId) {
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        if (indexService != null) {
            final IndexShard shard = indexService.getShard(shardId.id());
            if (isSegmentReplicationBackpressureEnabled && shard.indexSettings().isSegRepEnabled() && shard.routingEntry().primary()) {
                validateReplicationGroup(shard);
            }
        }
    }

    private void validateReplicationGroup(IndexShard shard) {
        final Set<SegmentReplicationShardStats> replicaStats = shard.getReplicationStats();
        final Set<SegmentReplicationShardStats> staleReplicas = getStaleReplicas(replicaStats);
        if (staleReplicas.isEmpty() == false) {
            // inSyncIds always considers the primary id, so filter it out.
            final float percentStale = staleReplicas.size() * 100f / (shard.getReplicationGroup().getInSyncAllocationIds().size() - 1);
            final double maxStaleLimit = maxAllowedStaleReplicas * 100f;
            if (percentStale >= maxStaleLimit) {
                tracker.incrementRejectionCount(shard.shardId());
                logger.warn("Rejecting write requests for shard, stale shards [{}%] shards: {}", percentStale, staleReplicas);
                throw new OpenSearchRejectedExecutionException(
                    "rejected execution on primary shard: " + shard.shardId() + " Stale Replicas: " + staleReplicas + "]",
                    false
                );
            }
        }
    }

    private Set<SegmentReplicationShardStats> getStaleReplicas(final Set<SegmentReplicationShardStats> replicas) {
        return replicas.stream()
            .filter(entry -> entry.getCheckpointsBehindCount() > maxCheckpointsBehind)
            .filter(entry -> entry.getCurrentReplicationTimeMillis() > maxReplicationTime.millis())
            .collect(Collectors.toSet());
    }

    public SegmentReplicationStats nodeStats() {
        return tracker.getStats();
    }

    public SegmentReplicationPerGroupStats getStatsForShard(IndexShard indexShard) {
        return tracker.getStatsForShard(indexShard);
    }

    public boolean isSegmentReplicationBackpressureEnabled() {
        return isSegmentReplicationBackpressureEnabled;
    }

    public void setSegmentReplicationBackpressureEnabled(boolean segmentReplicationBackpressureEnabled) {
        isSegmentReplicationBackpressureEnabled = segmentReplicationBackpressureEnabled;
    }

    public void setMaxCheckpointsBehind(int maxCheckpointsBehind) {
        this.maxCheckpointsBehind = maxCheckpointsBehind;
    }

    public void setMaxAllowedStaleReplicas(double maxAllowedStaleReplicas) {
        this.maxAllowedStaleReplicas = maxAllowedStaleReplicas;
    }

    public void setMaxReplicationTime(TimeValue maxReplicationTime) {
        this.maxReplicationTime = maxReplicationTime;
    }

    @Override
    public void close() throws IOException {
        failStaleReplicaTask.close();
    }

    // Background Task to fail replica shards if they are too far behind primary shard.
    final static class AsyncFailStaleReplicaTask extends AbstractAsyncTask {

        final SegmentReplicationPressureService pressureService;

        static final TimeValue INTERVAL = TimeValue.timeValueSeconds(30);

        AsyncFailStaleReplicaTask(SegmentReplicationPressureService pressureService) {
            super(logger, pressureService.threadPool, INTERVAL, true);
            this.pressureService = pressureService;
            rescheduleIfNecessary();
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        protected void runInternal() {
            if (pressureService.isSegmentReplicationBackpressureEnabled) {
                final SegmentReplicationStats stats = pressureService.tracker.getStats();

                // Find the shardId in node which is having stale replicas with highest current replication time.
                // This way we only fail one shardId's stale replicas in every iteration of this background async task and there by decrease
                // load gradually on node.
                stats.getShardStats()
                    .entrySet()
                    .stream()
                    .flatMap(
                        entry -> pressureService.getStaleReplicas(entry.getValue().getReplicaStats())
                            .stream()
                            .map(r -> Tuple.tuple(entry.getKey(), r.getCurrentReplicationTimeMillis()))
                    )
                    .max(Comparator.comparingLong(Tuple::v2))
                    .map(Tuple::v1)
                    .ifPresent(shardId -> {
                        final Set<SegmentReplicationShardStats> staleReplicas = pressureService.getStaleReplicas(
                            stats.getShardStats().get(shardId).getReplicaStats()
                        );
                        final IndexService indexService = pressureService.indicesService.indexService(shardId.getIndex());
                        if (indexService.getIndexSettings() != null && indexService.getIndexSettings().isSegRepEnabled() == false) {
                            return;
                        }
                        final IndexShard primaryShard = indexService.getShard(shardId.getId());
                        for (SegmentReplicationShardStats staleReplica : staleReplicas) {
                            if (staleReplica.getCurrentReplicationTimeMillis() > 2 * pressureService.maxReplicationTime.millis()) {
                                pressureService.shardStateAction.remoteShardFailed(
                                    shardId,
                                    staleReplica.getAllocationId(),
                                    primaryShard.getOperationPrimaryTerm(),
                                    true,
                                    "replica too far behind primary, marking as stale",
                                    null,
                                    new ActionListener<>() {
                                        @Override
                                        public void onResponse(Void unused) {
                                            logger.trace(
                                                "Successfully failed remote shardId [{}] allocation id [{}]",
                                                shardId,
                                                staleReplica.getAllocationId()
                                            );
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
                                            logger.error("Failed to send remote shard failure", e);
                                        }
                                    }
                                );
                            }
                        }
                    });
            }
        }

        @Override
        protected String getThreadPool() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        public String toString() {
            return "fail_stale_replica";
        }

    }

}
