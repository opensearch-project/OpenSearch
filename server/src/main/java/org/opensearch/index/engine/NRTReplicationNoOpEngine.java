/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.util.SetOnce;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;

import java.io.IOException;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication and remote store
 * are enabled together. The new engine has been extended from {@link AbstractNRTReplicationEngine} to allow NRT
 * Replication to work as is. Also, we want the recovery to work for which this engine would act as a dummy engine which
 * acks the primary with the expected value as the status quo. There is a plan to remove this or refactor this basis the
 * primary-primary recovery with SegRep and Remote store for translog. If we were to remove this engine, we can programme
 * the {@link org.opensearch.action.bulk.TransportShardBulkAction#dispatchedShardOperationOnReplica(BulkShardRequest,
 * IndexShard, ActionListener)} method to complete the listener on a dummy response.
 *
 * @opensearch.internal
 */
public class NRTReplicationNoOpEngine extends AbstractNRTReplicationEngine {

    private static final TranslogStats DEFAULT_STATS = new TranslogStats();

    public NRTReplicationNoOpEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    @Override
    protected TranslogManager createTranslogManager(String translogUUID, SetOnce<TranslogManager> translogManager) throws IOException {
        return new NoOpTranslogManager(shardId, readLock, this::ensureOpen, null, new Translog.Snapshot() {
            @Override
            public void close() {
            }

            @Override
            public int totalOperations() {
                return 0;
            }

            @Override
            public Translog.Operation next() {
                return null;
            }
        }) {
            @Override
            public TranslogStats getTranslogStats() {
                return DEFAULT_STATS;
            }
        };
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return -1;
    }

    /**
     * This method is essentially no-op but does however updates in-memory local checkpoint tracker.
     *
     * @param index request for index
     * @return index result.
     */
    @Override
    public IndexResult index(Index index) throws IOException {
        throw new RuntimeException("You are not supposed to be here");
    }

    /**
     * This method is essentially no-op but does however updates in-memory local checkpoint tracker.
     *
     * @param delete request for delete.
     * @return delete result.
     */
    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        throw new RuntimeException("You are not supposed to be here");
    }

    /**
     * This method is essentially no-op but does however updates in-memory local checkpoint tracker.
     *
     * @param noOp request for no-op.
     * @return no-op result.
     */
    @Override
    public NoOpResult noOp(NoOp noOp) throws IOException {
        throw new RuntimeException("You are not supposed to be here");
    }

//    /**
//     * This method tracks the maximum sequence number of the request that has been given for indexing to this replica.
//     * Currently, the recovery process involves replaying translog operation on the replica by the primary. For recovery
//     * step to finish, and finalize step to kick in, this method should return expected value. Hence it has been overridden.
//     * There is plan to make changes in recovery code to skip the translog replay. Post which this engine can be evaluated
//     * for its existence basis the primary-primary recovery usecase.
//     *
//     * @return the max sequence number that has been sent to {@link #index(Index)}, {@link #delete(Delete)}, and {@link
//     * #noOp(NoOp)} methods.
//     */
//    @Override
//    public long getPersistedLocalCheckpoint() {
//        return getLocalCheckpointTracker().getMaxSeqNo();
//    }
}
