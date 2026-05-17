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
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

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
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
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

    @Override
    protected EmptyResult shardOperation(PrepareTieringRequest request, ShardRouting shardRouting) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());
        logger.debug("Preparing shard [{}] for tiering", shardRouting.shardId());

        Releasable permit = acquirePrimaryPermits(indexShard, shardRouting);
        try {
            syncAndFlush(indexShard, shardRouting);
            waitForRemoteSync(indexShard, shardRouting);
            verifyNoUncommittedOps(indexShard, shardRouting);
        } finally {
            permit.close();
        }

        logger.debug("Shard [{}] prepared for tiering successfully", shardRouting.shardId());
        return EmptyResult.INSTANCE;
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
     * Syncs translog to remote store, then flushes all data to segments and refreshes.
     * After this method, all indexed data is in segments on disk and visible to readers.
     */
    private void syncAndFlush(IndexShard indexShard, ShardRouting shardRouting) throws IOException {
        logger.trace("Syncing and flushing shard [{}]", shardRouting.shardId());
        indexShard.sync();
        indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
        indexShard.refresh("prepare_tiering");
    }

    /**
     * Waits until all local segments are uploaded to remote store.
     * Blocks until remote store is fully in sync with local state.
     */
    private void waitForRemoteSync(IndexShard indexShard, ShardRouting shardRouting) throws IOException {
        logger.trace("Waiting for remote store sync on shard [{}]", shardRouting.shardId());
        indexShard.waitForRemoteStoreSync();
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
        // add a read_only_allow_delete block on the index. That block includes METADATA_WRITE level,
        // which would reject this action. Since this is an internal operation that runs after the block
        // is deliberately set, we skip the index-level block check.
        return null;
    }
}
