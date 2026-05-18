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
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport Tiering action to move indices from hot to warm.
 * For DFA (pluggable data format) indices, this action:
 * 1. Adds a read_only_allow_delete block to prevent new writes
 * 2. Performs pre-tiering sync (flush + refresh + remote store sync) on all primary shards
 * 3. Proceeds with the tiering operation
 *
 * Non-DFA indices skip steps 1 and 2 and go directly to tiering.
 */
public class TransportHotToWarmTierAction extends TransportTierAction {

    private static final Logger logger = LogManager.getLogger(TransportHotToWarmTierAction.class);
    private static final int MAX_PREPARE_RETRIES = 3;

    private final TransportPrepareTieringAction prepareTieringAction;

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
    }

    @Override
    protected void clusterManagerOperation(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        if (isAlreadyWarm(request.getIndex(), state)) {
            listener.onFailure(new IllegalArgumentException(
                "Index [" + request.getIndex() + "] is already on warm tier. Hot-to-warm tiering is not needed."));
            return;
        }
        if (isDfaIndex(request.getIndex(), state)) {
            logger.info("Index [{}] is a DFA index, adding read-only block and performing pre-tiering sync", request.getIndex());
            addReadOnlyBlockAndPrepare(request, state, listener);
        } else {
            super.clusterManagerOperation(request, state, listener);
        }
    }

    /**
     * Step 1: Add read_only_allow_delete block on the DFA index via setting to prevent new writes during prepare.
     * Using the setting (not ClusterBlocks API) ensures the block is persisted in index metadata and can be
     * cleanly removed on cancel or failure.
     * On success, proceeds to step 2 (prepare tiering).
     */
    private void addReadOnlyBlockAndPrepare(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("add-read-only-block-for-tiering [" + request.getIndex() + "]", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata indexMetadata = currentState.metadata().index(request.getIndex());
                if (indexMetadata == null) {
                    throw new IllegalStateException("Index [" + request.getIndex() + "] not found");
                }
                Settings.Builder indexSettingsBuilder = Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true);

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                    .settings(indexSettingsBuilder)
                    .settingsVersion(1 + indexMetadata.getSettingsVersion());

                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                blocks.addIndexBlock(request.getIndex(), IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);

                return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("Read-only block added for index [{}], proceeding with pre-tiering sync", request.getIndex());
                executePrepareTiering(request, newState, listener, 1);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("Failed to add read-only block for index [{}]", request.getIndex());
                listener.onFailure(new IllegalStateException(
                    "Failed to add read-only block for DFA index [" + request.getIndex() + "]. Please retry.", e));
            }
        });
    }

    /**
     * Step 2: Execute the prepare tiering action (flush + refresh + waitForRemoteStoreSync) on primary shards.
     * Retries up to MAX_PREPARE_RETRIES times on shard failures before giving up.
     * On success, proceeds to step 3 (tier).
     * On final failure, removes the read-only block to avoid leaving the index in a stuck state.
     */
    private void executePrepareTiering(IndexTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener, int attempt) {
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
                    removeReadOnlyBlock(request.getIndex());
                    listener.onFailure(new IllegalStateException(errorMsg));
                    return;
                }
                logger.info("Pre-tiering sync completed for index [{}], proceeding with tiering", request.getIndex());
                try {
                    TransportHotToWarmTierAction.super.clusterManagerOperation(request, state, listener);
                } catch (Exception e) {
                    removeReadOnlyBlock(request.getIndex());
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (attempt < MAX_PREPARE_RETRIES) {
                    logger.warn(
                        "Pre-tiering sync attempt [{}/{}] failed for index [{}], retrying",
                        attempt,
                        MAX_PREPARE_RETRIES,
                        request.getIndex(),
                        e
                    );
                    executePrepareTiering(request, state, listener, attempt + 1);
                    return;
                }
                String errorMsg = "Pre-tiering sync failed for DFA index [" + request.getIndex() + "] after "
                    + MAX_PREPARE_RETRIES + " attempts. Please retry.";
                logger.error(errorMsg, e);
                removeReadOnlyBlock(request.getIndex());
                listener.onFailure(new IllegalStateException(errorMsg, e));
            }
        });
    }

    /**
     * Removes the read_only_allow_delete block from the index.
     * Called on prepare failure to avoid leaving the index in a stuck read-only state.
     * Best-effort — if this fails, the user can manually remove the block.
     */
    private void removeReadOnlyBlock(String indexName) {
        clusterService.submitStateUpdateTask("remove-read-only-block-for-tiering [" + indexName + "]", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata indexMetadata = currentState.metadata().index(indexName);
                if (indexMetadata == null) {
                    return currentState;
                }
                Settings.Builder indexSettingsBuilder = Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), false);

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata)
                    .settings(indexSettingsBuilder)
                    .settingsVersion(1 + indexMetadata.getSettingsVersion());

                Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);
                ClusterBlocks.Builder blocks = ClusterBlocks.builder().blocks(currentState.blocks());
                blocks.removeIndexBlock(indexName, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);

                return ClusterState.builder(currentState).metadata(metadataBuilder).blocks(blocks).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("Failed to remove read-only block for index [{}] after tiering failure. "
                    + "Block can be removed manually via index settings.", indexName, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("Read-only block removed for index [{}] after tiering failure", indexName);
            }
        });
    }

    /**
     * Checks if the index has pluggable data format enabled (is a DFA index).
     */
    private boolean isDfaIndex(String indexName, ClusterState state) {
        IndexMetadata indexMetadata = state.metadata().index(indexName);
        if (indexMetadata == null) {
            return false;
        }
        return indexMetadata.getSettings().getAsBoolean(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), false);
    }

    /**
     * Checks if the index is already on the warm tier.
     * Prevents redundant tiering operations on indices that are already warm.
     */
    private boolean isAlreadyWarm(String indexName, ClusterState state) {
        IndexMetadata indexMetadata = state.metadata().index(indexName);
        if (indexMetadata == null) {
            return false;
        }
        return indexMetadata.getSettings().getAsBoolean(IndexModule.IS_WARM_INDEX_SETTING.getKey(), false);
    }
}
