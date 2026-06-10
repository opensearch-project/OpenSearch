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
     * Step 1: Add a write block on the DFA index to prevent new writes during prepare.
     * Using the setting (not ClusterBlocks API) ensures the block is persisted in index metadata and can be
     * cleanly removed on cancel or failure.
     * On success, proceeds to step 2 (prepare tiering).
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
                    // Block writes before pre-tiering sync. blocks.write cannot be auto-removed by
                    // DiskThresholdMonitor (unlike read_only_allow_delete) so it is sufficient alone.
                    Settings.Builder indexSettingsBuilder = Settings.builder()
                        .put(indexMetadata.getSettings())
                        .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true);

                    IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                        .settings(indexSettingsBuilder)
                        .settingsVersion(1 + indexMetadata.getSettingsVersion());

                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    blocks.addIndexBlock(request.getIndex(), IndexMetadata.INDEX_WRITE_BLOCK);

                    return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.info("Write block added for index [{}], proceeding with pre-tiering sync", request.getIndex());
                    executePrepareTiering(request, newState, listener, 1);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("Failed to add write block for index [{}]", request.getIndex());
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
     * On final failure, removes the write block to avoid leaving the index in a stuck state.
     */
    private void executePrepareTiering(
        IndexTieringRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener,
        int attempt
    ) {
        PrepareTieringRequest prepareTieringRequest = new PrepareTieringRequest(request.getIndex());
        prepareTieringRequest.timeout(request.timeout());

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
                        executePrepareTiering(request, state, listener, attempt + 1);
                        return;
                    }
                    String errorMsg = "Pre-tiering sync failed for index ["
                        + request.getIndex()
                        + "] after "
                        + MAX_PREPARE_RETRIES
                        + " attempts: "
                        + broadcastResponse.getFailedShards()
                        + " shard(s) failed. Please retry the tiering request.";
                    logger.error(errorMsg);
                    removeWriteBlock(request.getIndex());
                    listener.onFailure(new IllegalStateException(errorMsg));
                    return;
                }
                logger.info("Pre-tiering sync completed for index [{}], proceeding with tiering", request.getIndex());
                try {
                    TransportHotToWarmTierAction.super.clusterManagerOperation(request, state, listener);
                } catch (Exception e) {
                    removeWriteBlock(request.getIndex());
                    listener.onFailure(e);
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
                    executePrepareTiering(request, state, listener, attempt + 1);
                    return;
                }
                String errorMsg = "Pre-tiering sync failed for DFA index ["
                    + request.getIndex()
                    + "] after "
                    + MAX_PREPARE_RETRIES
                    + " attempts. Please retry.";
                logger.error(errorMsg, e);
                removeWriteBlock(request.getIndex());
                listener.onFailure(new IllegalStateException(errorMsg, e));
            }
        });
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
                    Settings.Builder indexSettingsBuilder = Settings.builder()
                        .put(indexMetadata.getSettings())
                        .put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), false);

                    IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                        .settings(indexSettingsBuilder)
                        .settingsVersion(1 + indexMetadata.getSettingsVersion());

                    Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
                    ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                    blocks.removeIndexBlock(indexName, IndexMetadata.INDEX_WRITE_BLOCK);

                    return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        "Failed to remove write block for index [{}] after tiering failure: {}. "
                            + "Block can be removed manually via index settings.",
                        indexName,
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
