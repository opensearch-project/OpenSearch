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
import org.opensearch.storage.action.tiering.status.ListTieringStatusAction;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusRequest;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport handler for listing tiering status of all indices.
 * TieringService dependencies (HotToWarmTieringService, WarmToHotTieringService)
 * will be added in the implementation PR.
 */
public class TransportListTieringStatusAction extends TransportClusterManagerNodeReadAction<
    ListTieringStatusRequest,
    ListTieringStatusResponse> {

    private static final Logger logger = LogManager.getLogger(TransportListTieringStatusAction.class);

    /**
     * Constructs a TransportListTieringStatusAction.
     *
     * @param transportService the transport service
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param actionFilters the action filters
     * @param indexNameExpressionResolver the index name expression resolver
     */
    @Inject
    public TransportListTieringStatusAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ListTieringStatusAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ListTieringStatusRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    public String executor() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected ListTieringStatusResponse read(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterManagerOperation(
        ListTieringStatusRequest request,
        ClusterState clusterState,
        ActionListener<ListTieringStatusResponse> listener
    ) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public ClusterBlockException checkBlock(ListTieringStatusRequest request, ClusterState clusterState) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
