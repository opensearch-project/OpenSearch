/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Async task responsible for failing stale replicas.
 */
final class AsyncFailStaleReplicaTask extends AbstractAsyncTask {
    private static final Logger logger = LogManager.getLogger(AsyncFailStaleReplicaTask.class);
    private final SegmentReplicationPendingCheckpoints pendingCheckpoints;
    private final IndicesService indicesService;

    private final BiConsumer<ReplicationFailedException, IndexShard> failShard;
    static final TimeValue INTERVAL = TimeValue.timeValueSeconds(30);

    public AsyncFailStaleReplicaTask(
        IndicesService indicesService,
        ThreadPool threadPool,
        SegmentReplicationPendingCheckpoints pendingCheckpoints,
        BiConsumer<ReplicationFailedException, IndexShard> failShard
    ) {
        super(logger, threadPool, INTERVAL, true);
        this.pendingCheckpoints = pendingCheckpoints;
        this.indicesService = indicesService;
        this.failShard = failShard;
        rescheduleIfNecessary();
    }

    @Override
    protected boolean mustReschedule() {
        return true;
    }

    @Override
    protected void runInternal() {
        List<ShardId> shardsToFail = pendingCheckpoints.getStaleShardsToFail();
        for (ShardId shardId : shardsToFail) {
            IndexShard replicaShard = this.indicesService.getShardOrNull(shardId);
            if (replicaShard != null && replicaShard.state() == IndexShardState.STARTED) {
                failShard.accept(new ReplicationFailedException("replica too far behind primary, marking as stale"), replicaShard);
            }
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
