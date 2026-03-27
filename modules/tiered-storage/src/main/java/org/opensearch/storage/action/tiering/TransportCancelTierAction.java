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
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

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
     * @param hotToWarmTieringService the hot-to-warm tiering service
     * @param warmToHotTieringService the warm-to-hot tiering service
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
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected ClusterBlockException checkBlock(CancelTieringRequest request, ClusterState state) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected void clusterManagerOperation(
        CancelTieringRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
