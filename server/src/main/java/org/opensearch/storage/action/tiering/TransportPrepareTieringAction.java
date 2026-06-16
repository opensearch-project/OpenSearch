/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Transport action that prepares DFA indices for tiering by performing
 * sync, flush, refresh, and remote store sync on primary shards.
 * <p>
 * This action is a single broadcast RPC to all nodes with primary shards,
 * executing operations in parallel across nodes. The total time equals the
 * slowest shard's upload time.
 * <p>
 * Registered as internal action: {@code internal:admin/indices/prepare_tiering}
 *
 * @opensearch.internal
 */
public class TransportPrepareTieringAction extends TransportBroadcastByNodeAction<
    PrepareTieringRequest,
    BroadcastResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    private static final Logger logger = LogManager.getLogger(TransportPrepareTieringAction.class);
    private static final TimeValue PERMITS_ACQUIRE_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final IndicesService indicesService;
    private final ThreadPool threadPool;
    private volatile TimeValue prepareTieringTimeout;

    /**
     * Constructs a TransportPrepareTieringAction.
     *
     * @param clusterService the cluster service
     * @param transportService the transport service
     * @param indicesService the indices service
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     */
    @Inject
    public TransportPrepareTieringAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PrepareTieringAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            PrepareTieringRequest::new,
            ThreadPool.Names.GENERIC
        );
        this.indicesService = indicesService;
        this.threadPool = transportService.getThreadPool();
        this.prepareTieringTimeout = TieringUtils.PREPARE_TIERING_TIMEOUT.get(clusterService.getSettings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TieringUtils.PREPARE_TIERING_TIMEOUT, this::setPrepareTieringTimeout);
    }

    private void setPrepareTieringTimeout(TimeValue timeout) {
        this.prepareTieringTimeout = timeout;
        logger.info("Updated prepare tiering timeout to [{}]", timeout);
    }

    /**
     * Opt-in to async shard operation execution. Each shard's syncAndFlush is dispatched
     * to the thread pool in parallel, and the transport response is sent only after
     * all shards complete. This prevents long-running flush + remote sync from blocking
     * the thread that received the transport request.
     */
    @Override
    protected boolean isAsyncShardOperation() {
        return true;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected BroadcastResponse newResponse(
        PrepareTieringRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new BroadcastResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected PrepareTieringRequest readRequestFrom(StreamInput in) throws IOException {
        return new PrepareTieringRequest(in);
    }

    /**
     * Sync fallback — required by the abstract contract but never called because
     * {@link #isAsyncShardOperation()} returns true and we override
     * {@link #shardOperationAsync}. Throws unconditionally to catch misuse.
     */
    @Override
    protected EmptyResult shardOperation(PrepareTieringRequest request, ShardRouting shardRouting) throws IOException {
        throw new UnsupportedOperationException(
            "TransportPrepareTieringAction uses shardOperationAsync exclusively; sync path should never be called"
        );
    }

    /**
     * Non-blocking async shard operation. Registers a merge drain listener instead of
     * blocking a thread waiting for merges. Uses AtomicBoolean to ensure only one path
     * (merge callback OR timeout) completes the operation.
     */
    @Override
    protected void shardOperationAsync(PrepareTieringRequest request, ShardRouting shardRouting, ActionListener<EmptyResult> listener) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());
        logger.debug("Preparing shard [{}] for tiering (async path)", shardRouting.shardId());

        // Fail fast if shard is not fully started
        if (indexShard.state() != IndexShardState.STARTED) {
            listener.onFailure(
                new IOException(
                    "Shard ["
                        + shardRouting.shardId()
                        + "] is not in STARTED state (current: "
                        + indexShard.state()
                        + "). Cannot prepare for tiering — will retry."
                )
            );
            return;
        }

        Releasable permit;
        try {
            permit = acquirePrimaryPermits(indexShard, shardRouting);
        } catch (IOException e) {
            logger.debug(
                () -> "Failed to acquire primary permits for shard [" + shardRouting.shardId() + "] during tiering prepare — will retry",
                e
            );
            listener.onFailure(e);
            return;
        }

        indexShard.freezeForTiering();

        // Timeout: use 80% of the transport timeout so we fail with a meaningful message
        // before the transport channel times out with a generic error. This allows the caller to retry.
        TimeValue mergeTimeout = request.timeout() != null ? request.timeout() : prepareTieringTimeout;
        long mergeTimeoutMillis = (long) (mergeTimeout.millis() * 0.8);
        TimeValue effectiveTimeout = TimeValue.timeValueMillis(mergeTimeoutMillis);
        AtomicBoolean completed = new AtomicBoolean(false);

        // Schedule timeout — fires before transport timeout with diagnostic info about pending merges
        Scheduler.ScheduledCancellable timeout = threadPool.schedule(() -> {
            if (completed.compareAndSet(false, true)) {
                int activeMerges = indexShard.getActiveMergeCount();
                int pendingMerges = indexShard.getPendingMergeCount();
                try {
                    listener.onFailure(
                        new MergeDrainTimeoutException(shardRouting.shardId(), activeMerges, pendingMerges, mergeTimeout.toString())
                    );
                } finally {
                    permit.close();
                }
            }
        }, effectiveTimeout, ThreadPool.Names.GENERIC);

        // Non-blocking merge wait. The CAS makes this listener idempotent — onMergesDrained may
        // invoke us more than once under a narrow scheduler race, and `completed` ensures the body
        // (sync/flush, listener.onResponse, permit.close) runs at most once.
        indexShard.onMergesDrained(() -> {
            if (completed.compareAndSet(false, true)) {
                timeout.cancel();
                try {
                    completeSyncAndFlush(indexShard, shardRouting);
                    listener.onResponse(EmptyResult.INSTANCE);
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    permit.close();
                }
            }
        });
        // Thread returns immediately — not blocked!
    }

    /**
     * Acquires all primary operation permits to drain in-flight indexing operations.
     * Blocks until all currently executing operations complete (up to 30s timeout).
     * While permits are held, no new indexing operations can start on this shard.
     */
    private Releasable acquirePrimaryPermits(IndexShard indexShard, ShardRouting shardRouting) throws IOException {
        try {
            PlainActionFuture<Releasable> permitFuture = new PlainActionFuture<>();
            indexShard.acquireAllPrimaryOperationsPermits(permitFuture, PERMITS_ACQUIRE_TIMEOUT);
            return permitFuture.actionGet();
        } catch (Exception e) {
            throw new IOException("Failed to acquire primary operation permits for shard [" + shardRouting.shardId() + "]", e);
        }
    }

    /**
     * Performs the final sync, flush, refresh, remote store sync, and verification.
     * Called after merges have drained (either via blocking wait or async listener).
     */
    private void completeSyncAndFlush(IndexShard indexShard, ShardRouting shardRouting) throws IOException {
        logger.trace("Completing sync and flush for shard [{}]", shardRouting.shardId());
        indexShard.sync();
        // Flush before refresh: committed segments_N must exist before waitForRemoteStoreSync uploads them.
        // force=true bypasses the freeze guard — this is the last flush ever for this engine.
        indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
        // "prepare_tiering" source bypasses the freeze guard — this is the last refresh ever.
        indexShard.refresh("prepare_tiering");
        indexShard.waitForRemoteStoreSync();
        verifyNoUncommittedOps(indexShard, shardRouting);

        logger.debug("Shard [{}] prepared for tiering successfully", shardRouting.shardId());
    }

    /**
     * Verifies that no uncommitted translog operations remain after flush.
     * This is a defensive check — with read-only block + force flush, uncommitted should always be 0.
     */
    private void verifyNoUncommittedOps(IndexShard indexShard, ShardRouting shardRouting) throws IOException {
        int uncommitted = indexShard.translogStats().getUncommittedOperations();
        if (uncommitted > 0) {
            throw new IOException(
                "Shard [" + shardRouting.shardId() + "] still has " + uncommitted + " uncommitted translog ops after flush"
            );
        }
    }

    /**
     * Targets primary shards only. Replicas get data via segment replication from primary.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, PrepareTieringRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShardsSatisfyingPredicate(concreteIndices, ShardRouting::primary);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PrepareTieringRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PrepareTieringRequest request, String[] concreteIndices) {
        // Do not check index-level blocks here. This action is called internally after we intentionally
        // add a write block on the index. That block includes METADATA_WRITE level,
        // which would reject this action. Since this is an internal operation that runs after the block
        // is deliberately set, we skip the index-level block check.
        return null;
    }
}
