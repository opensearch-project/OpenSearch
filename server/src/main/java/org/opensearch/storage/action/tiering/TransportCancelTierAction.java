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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.storage.tiering.TieringService;
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.storage.common.tiering.TieringUtils.resolveRequestIndex;

/**
 * Transport handler for cancelling ongoing tiering operations.
 * This handler can cancel both hot-to-warm and warm-to-hot tiering operations
 * by delegating to the appropriate tiering service.
 */
public class TransportCancelTierAction extends TransportClusterManagerNodeAction<CancelTieringRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportCancelTierAction.class);

    private final HotToWarmTieringService hotToWarmTieringService;
    private final WarmToHotTieringService warmToHotTieringService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    /**
     * Constructs a TransportCancelTierAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     * @param hotToWarmTieringService the hot to warm tiering service
     * @param warmToHotTieringService the warm to hot tiering service
     */
    @Inject
    public TransportCancelTierAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        HotToWarmTieringService hotToWarmTieringService,
        WarmToHotTieringService warmToHotTieringService
    ) {
        super(
            CancelTieringAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CancelTieringRequest::new,
            indexNameExpressionResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.hotToWarmTieringService = hotToWarmTieringService;
        this.warmToHotTieringService = warmToHotTieringService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CancelTieringRequest request, ClusterState state) {
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException == null) {
            blockException = state.blocks().globalBlockedException(ClusterBlockLevel.CREATE_INDEX);
        }
        return blockException != null ? blockException : state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected void clusterManagerOperation(CancelTieringRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {
        ActionListener<ClusterStateUpdateResponse> clusterStateUpdateListener = new ActionListener<>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new AcknowledgedResponse(clusterStateUpdateResponse.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> new ParameterizedMessage("failed to cancel tiering for index [{}]", request.getIndex()), e);
                listener.onFailure(e);
            }
        };

        // Determine which tiering service to use based on the current migration state
        TieringService tieringService = getTieringServiceForIndex(request.getIndex(), state);
        if (tieringService == null) {
            listener.onFailure(
                new IllegalArgumentException("Index [" + request.getIndex() + "] is not currently undergoing tiering operation")
            );
            return;
        }

        tieringService.cancelTiering(request, clusterStateUpdateListener, state);
    }

    /**
     * Determines the appropriate tiering service for cancelling the migration
     * based on the current state of the index.
     *
     * @param indexName the name of the index
     * @param state current cluster state
     * @return the appropriate TieringService or null if index is not being tiered
     */
    private TieringService getTieringServiceForIndex(String indexName, ClusterState state) {
        try {
            final Index index = resolveRequestIndex(indexNameExpressionResolver, indexName, state);
            // Check if index is being tracked by any tiering service
            if (hotToWarmTieringService.isIndexBeingTiered(index)) {
                return hotToWarmTieringService;
            }
            if (warmToHotTieringService.isIndexBeingTiered(index)) {
                return warmToHotTieringService;
            }
            return null;
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Error determining tiering service for index [{}]", indexName), e);
            return null;
        }
    }

}
