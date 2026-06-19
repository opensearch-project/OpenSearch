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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.index.Index;
import org.opensearch.storage.common.tiering.TieringUtils;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.opensearch.storage.common.tiering.TieringUtils.resolveRequestIndex;

/**
 * Transport Tiering action to move indices from hot to warm.
 * For DFA (pluggable data format) indices, this action:
 * 1. Adds a write block to prevent new writes
 * 2. Performs pre-tiering sync (flush + refresh + remote store sync) on all primary shards
 * 3. Proceeds with the tiering operation
 *
 * Non-DFA indices skip steps 1 and 2 and go directly to tiering.
 */
public class TransportHotToWarmTierAction extends TransportTierAction {

    private static final Logger logger = LogManager.getLogger(TransportHotToWarmTierAction.class);
    private static final int MAX_PREPARE_RETRIES = 3;

    private final TransportPrepareTieringAction prepareTieringAction;
    private final HotToWarmTieringService hotToWarmTieringService;

    /**
     * Constructs a TransportHotToWarmTierAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     * @param hotToWarmTieringService the hot to warm tiering service
     * @param prepareTieringAction the prepare tiering action for DFA indices
     */
    @Inject
    public TransportHotToWarmTierAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        HotToWarmTieringService hotToWarmTieringService,
        TransportPrepareTieringAction prepareTieringAction
    ) {
        super(
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            HotToWarmTierAction.NAME,
            hotToWarmTieringService
        );
        this.prepareTieringAction = prepareTieringAction;
        this.hotToWarmTieringService = hotToWarmTieringService;
    }

    @Override
    protected void clusterManagerOperation(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        if (TieringUtils.isDfaIndex(state.metadata().index(request.getIndex()))) {
            // Validate FIRST — before any state-mutating or expensive operations.
            // If validation fails (e.g. warm nodes full, too many concurrent requests),
            // reject immediately without adding a write block or running prepare.
            // Note: validation also runs inside TieringService.tier() for double-safety.
            try {
                Index index = resolveRequestIndex(indexNameExpressionResolver, request.getIndex(), state);
                hotToWarmTieringService.preflightValidate(state, index);
            } catch (Exception e) {
                logger.info("Preflight validation failed for DFA index [{}]: {}", request.getIndex(), e.getMessage());
                listener.onFailure(e);
                return;
            }
            logger.info("Index [{}] is a DFA index, adding write block and performing pre-tiering sync", request.getIndex());
            addWriteBlockAndPrepare(request, state, listener);
        } else {
            super.clusterManagerOperation(request, state, listener);
        }
    }

    /**
     * Step 1: write-block the DFA index so no new writes land during prepare. Applies both the
     * {@code blocks.write} setting (persisted in index metadata, cleanly reverted on cancel/failure) and
     * the {@link IndexMetadata#INDEX_WRITE_BLOCK} cluster block (enforced immediately). On success,
     * proceeds to step 2 (prepare tiering).
     */
    private void addWriteBlockAndPrepare(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "add-write-block-for-tiering [" + request.getIndex() + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    IndexMetadata indexMetadata = currentState.metadata().index(request.getIndex());
                    if (indexMetadata == null) {
                        throw new IllegalStateException("Index [" + request.getIndex() + "] not found");
                    }
                    // Block writes before pre-tiering sync: persist blocks.write (revertible on
                    // cancel/failure) and apply the INDEX_WRITE_BLOCK cluster block (enforced immediately).
                    return applyWriteBlock(currentState, indexMetadata, true);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("Write block added for index [{}], proceeding with pre-tiering sync", request.getIndex());
                    executePrepareTiering(request, newState, listener, 1);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error(() -> "Failed to add write block for index [" + request.getIndex() + "]", e);
                    listener.onFailure(
                        new IllegalStateException("Failed to add write block for DFA index [" + request.getIndex() + "]. Please retry.", e)
                    );
                }
            }
        );
    }

    /**
     * Step 2: Execute the prepare tiering action (flush + refresh + waitForRemoteStoreSync) on primary shards.
     * Retries up to MAX_PREPARE_RETRIES times on shard failures before giving up.
     * On success, proceeds to step 3 (tier).
     * On final failure, removes the write block to avoid leaving the index in a stuck, write-blocked state.
     */
    private void executePrepareTiering(
        IndexTieringRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener,
        int attempt
    ) {
        PrepareTieringRequest prepareTieringRequest = new PrepareTieringRequest(request.getIndex());
        // Use the cluster setting for timeout instead of the short AcknowledgedRequest default (30s).
        // This controls both the transport channel timeout and the merge drain timeout on the data node.
        prepareTieringRequest.timeout(TieringUtils.PREPARE_TIERING_TIMEOUT.get(clusterService.getSettings()));

        prepareTieringAction.execute(prepareTieringRequest, new ActionListener<BroadcastResponse>() {
            @Override
            public void onResponse(BroadcastResponse broadcastResponse) {
                if (broadcastResponse.getFailedShards() > 0) {
                    if (attempt < MAX_PREPARE_RETRIES) {
                        logger.warn(
                            "Pre-tiering sync attempt [{}/{}] had {} failed shard(s) for index [{}], retrying",
                            attempt,
                            MAX_PREPARE_RETRIES,
                            broadcastResponse.getFailedShards(),
                            request.getIndex()
                        );
                        retryPrepareTiering(request, state, listener, attempt);
                        return;
                    }
                    // Build a targeted error message based on failure type. A MergeDrainTimeoutException
                    // is detected primarily by instanceof: the type is registered for serialization
                    // (see OpenSearchServerException) and round-trips as a typed exception between nodes
                    // running 3.8.0+. As a fallback for mixed-version clusters — where a peer older than
                    // the registration version deserializes it as a generic wrapper — detection also matches
                    // MERGE_DRAIN_TIMEOUT_MARKER in the (always wire-preserved) message.
                    DefaultShardOperationFailedException[] failures = broadcastResponse.getShardFailures();
                    String mergeTimeoutSampleMessage = null;
                    int mergeTimeoutCount = 0;
                    int otherFailureCount = 0;

                    for (DefaultShardOperationFailedException f : failures) {
                        String timeoutMessage = findMergeDrainTimeout(f.getCause());
                        if (timeoutMessage != null) {
                            mergeTimeoutCount++;
                            if (mergeTimeoutSampleMessage == null) {
                                mergeTimeoutSampleMessage = timeoutMessage;
                            }
                        } else {
                            otherFailureCount++;
                        }
                    }

                    String errorMsg;
                    if (mergeTimeoutCount > 0 && otherFailureCount == 0) {
                        // All failures are merge drain timeouts — surface the per-shard detail (the
                        // sample's message already carries shard id, merge counts, and the timeout).
                        errorMsg = "Tiering preparation timed out: "
                            + mergeTimeoutCount
                            + " shard(s) still waiting for merges to drain after "
                            + MAX_PREPARE_RETRIES
                            + " attempts. Example: "
                            + mergeTimeoutSampleMessage;
                    } else if (mergeTimeoutCount > 0) {
                        // Mixed failures
                        errorMsg = "Pre-tiering sync failed for index ["
                            + request.getIndex()
                            + "] after "
                            + MAX_PREPARE_RETRIES
                            + " attempts: "
                            + mergeTimeoutCount
                            + " shard(s) timed out waiting for merges, "
                            + otherFailureCount
                            + " shard(s) failed for other reasons. "
                            + "Consider increasing cluster.tiering.prepare_timeout or retry later.";
                    } else {
                        // No merge timeouts — generic message with first failure details
                        String firstFailure = failures.length == 0
                            ? "unknown"
                            : (failures[0].getCause() != null ? failures[0].getCause().getMessage() : "unknown");
                        errorMsg = "Pre-tiering sync failed for index ["
                            + request.getIndex()
                            + "] after "
                            + MAX_PREPARE_RETRIES
                            + " attempts: "
                            + broadcastResponse.getFailedShards()
                            + " shard(s) failed. "
                            + "First failure: "
                            + firstFailure
                            + ". Please retry.";
                    }
                    logger.error(errorMsg);
                    failAndRemoveWriteBlock(request.getIndex(), new IllegalStateException(errorMsg), listener);
                    return;
                }
                logger.info("Pre-tiering sync completed for index [{}], proceeding with tiering", request.getIndex());
                // The tier step (super.clusterManagerOperation) runs an async cluster-state update and
                // reports failure via listener.onFailure, not by throwing — so wrap the listener to remove
                // the write block on failure (sync OR async). On success the block is intentionally retained
                // (the index is moving to warm). The try/catch still covers a synchronous throw before the
                // listener is ever invoked. removeWriteBlock is idempotent, so there is no double-removal risk.
                ActionListener<AcknowledgedResponse> tierListener = ActionListener.wrap(
                    listener::onResponse,
                    e -> failAndRemoveWriteBlock(request.getIndex(), e, listener)
                );
                try {
                    TransportHotToWarmTierAction.super.clusterManagerOperation(request, state, tierListener);
                } catch (Exception e) {
                    failAndRemoveWriteBlock(request.getIndex(), e, listener);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (attempt < MAX_PREPARE_RETRIES) {
                    logger.warn(
                        "Pre-tiering sync attempt [{}/{}] failed for index [{}], retrying: {}",
                        attempt,
                        MAX_PREPARE_RETRIES,
                        request.getIndex(),
                        e
                    );
                    retryPrepareTiering(request, state, listener, attempt);
                    return;
                }
                String errorMsg = "Pre-tiering sync failed for DFA index ["
                    + request.getIndex()
                    + "] after "
                    + MAX_PREPARE_RETRIES
                    + " attempts. Please retry.";
                logger.error(errorMsg, e);
                failAndRemoveWriteBlock(request.getIndex(), new IllegalStateException(errorMsg, e), listener);
            }
        });
    }

    /**
     * Retries the prepare tiering action with an incremented attempt counter.
     * Common retry logic extracted from onResponse (shard failures) and onFailure (transport failures).
     */
    private void retryPrepareTiering(
        IndexTieringRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener,
        int attempt
    ) {
        executePrepareTiering(request, state, listener, attempt + 1);
    }

    /**
     * Walks a failure's cause chain (bounded depth) looking for a merge-drain timeout and returns its
     * detail message, or {@code null} if none is found.
     * <p>
     * Detection is primarily by type: {@link MergeDrainTimeoutException} is registered for
     * serialization (see {@code OpenSearchServerException}) and round-trips as a typed exception
     * between nodes running 3.8.0+. As a fallback for mixed-version clusters — where a peer older than
     * the registration version deserializes it as a generic wrapper — detection also matches
     * {@link MergeDrainTimeoutException#MERGE_DRAIN_TIMEOUT_MARKER} in the (always wire-preserved)
     * message.
     */
    private static String findMergeDrainTimeout(Throwable t) {
        int depth = 0;
        while (t != null && depth++ < 10) {
            if (t instanceof MergeDrainTimeoutException) {
                return t.getMessage();
            }
            final String message = t.getMessage();
            if (message != null && message.contains(MergeDrainTimeoutException.MERGE_DRAIN_TIMEOUT_MARKER)) {
                return message;
            }
            t = t.getCause();
        }
        return null;
    }

    /**
     * Idempotently drives the index's write block to the desired state. Always (re)asserts the
     * {@link IndexMetadata#INDEX_WRITE_BLOCK} cluster block to match {@code blockWrites}, but rewrites
     * the persisted {@code index.blocks.write} setting (and bumps {@code settingsVersion}) only when it
     * actually changes. Skipping the no-op rewrite avoids bumping {@code settingsVersion} without a real
     * settings change, which would violate the {@code IndexService.updateMetadata} invariant.
     *
     * @param currentState  the current cluster state
     * @param indexMetadata the (non-null) metadata of the index to update
     * @param blockWrites   {@code true} to block writes, {@code false} to unblock
     * @return the updated cluster state
     */
    // Package-private (not private) so unit tests in the same package can exercise the
    // idempotency invariant directly (no settingsVersion bump when the setting is unchanged).
    static ClusterState applyWriteBlock(ClusterState currentState, IndexMetadata indexMetadata, boolean blockWrites) {
        final String indexName = indexMetadata.getIndex().getName();

        ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
        if (blockWrites) {
            blocks.addIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);
        } else {
            blocks.removeIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);
        }

        // Setting already matches the desired state — only (re)assert the cluster block, no version bump.
        if (IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(indexMetadata.getSettings()) == blockWrites) {
            return ClusterState.builder(currentState).blocks(blocks).build();
        }

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(indexMetadata.getSettings())
            .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), blockWrites);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
            .settings(indexSettingsBuilder)
            .settingsVersion(1 + indexMetadata.getSettingsVersion());

        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);

        return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
    }

    /**
     * Terminal-failure cleanup for the tiering flow: removes the write block (so the index is not left
     * stuck in a write-blocked state) and then fails the listener. Co-locating the two steps ensures
     * every failure exit removes the block. {@link #removeWriteBlock} is idempotent, so calling this
     * more than once for the same index is safe.
     */
    private void failAndRemoveWriteBlock(String indexName, Exception e, ActionListener<AcknowledgedResponse> listener) {
        removeWriteBlock(indexName);
        listener.onFailure(e);
    }

    /**
     * Removes the write block from the index.
     * Called on prepare failure to avoid leaving the index in a stuck write-blocked state.
     * Best-effort — if this fails, the user can manually remove the block via index settings.
     */
    private void removeWriteBlock(String indexName) {
        clusterService.submitStateUpdateTask(
            "remove-write-block-for-tiering [" + indexName + "]",
            new ClusterStateUpdateTask(Priority.URGENT) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                    if (indexMetadata == null) {
                        return currentState;
                    }
                    return applyWriteBlock(currentState, indexMetadata, false);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        () -> "Failed to remove write block for index ["
                            + indexName
                            + "] after tiering failure. The block can be removed manually via index settings.",
                        e
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("Write block removed for index [{}] after tiering failure", indexName);
                }
            }
        );
    }
}
