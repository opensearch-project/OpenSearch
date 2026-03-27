/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.tiering.status.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexModule;
import org.opensearch.storage.action.tiering.status.GetTieringStatusAction;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusRequest;
import org.opensearch.storage.action.tiering.status.model.GetTieringStatusResponse;
import org.opensearch.storage.tiering.HotToWarmTieringService;
import org.opensearch.storage.tiering.TieringService;
import org.opensearch.storage.tiering.WarmToHotTieringService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport handler for getting tiering status of a single index.
 */
public class TransportGetTieringStatusAction extends TransportClusterManagerNodeReadAction<
    GetTieringStatusRequest,
    GetTieringStatusResponse> {

    private static final Logger logger = LogManager.getLogger(TransportGetTieringStatusAction.class);
    private final Map<IndexModule.TieringState, TieringService> migrationServiceMap = new HashMap<>();

    /**
     * Constructs a TransportGetTieringStatusAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param hotToWarmTieringService the hot-to-warm tiering service
     * @param warmToHotTieringService the warm-to-hot tiering service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     */
    @Inject
    public TransportGetTieringStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        HotToWarmTieringService hotToWarmTieringService,
        WarmToHotTieringService warmToHotTieringService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetTieringStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetTieringStatusRequest::new,
            indexNameExpressionResolver
        );
        migrationServiceMap.put(IndexModule.TieringState.HOT_TO_WARM, hotToWarmTieringService);
        migrationServiceMap.put(IndexModule.TieringState.WARM_TO_HOT, warmToHotTieringService);
    }

    @Override
    public String executor() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected GetTieringStatusResponse read(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterManagerOperation(
        GetTieringStatusRequest request,
        ClusterState clusterState,
        ActionListener<GetTieringStatusResponse> listener
    ) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ClusterBlockException checkBlock(GetTieringStatusRequest request, ClusterState state) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
