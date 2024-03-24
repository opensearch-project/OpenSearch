/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for obtaining cluster term and version from cluster-manager
 *
 * @opensearch.internal
 */
public class TransportGetTermVersionAction extends TransportClusterManagerNodeReadAction<GetTermVersionRequest, GetTermVersionResponse> {

    private final Logger logger = LogManager.getLogger(getClass());

    @Inject
    public TransportGetTermVersionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetTermVersionAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetTermVersionRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    public GetTermVersionResponse read(StreamInput in) throws IOException {
        return new GetTermVersionResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetTermVersionRequest request, ClusterState state) {
        // cluster state term and version needs to be retrieved even on a fully blocked cluster
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        GetTermVersionRequest request,
        ClusterState state,
        ActionListener<GetTermVersionResponse> listener
    ) throws Exception {
        ActionListener.completeWith(listener, () -> buildResponse(request, state));
    }

    private GetTermVersionResponse buildResponse(GetTermVersionRequest request, ClusterState state) {
        return new GetTermVersionResponse(new ClusterStateTermVersion(state));
    }
}
