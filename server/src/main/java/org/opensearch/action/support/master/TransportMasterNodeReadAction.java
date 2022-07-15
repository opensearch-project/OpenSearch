/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.master;

import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * A base class for read operations that needs to be performed on the cluster-manager node.
 * Can also be executed on the local node if needed.
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link TransportClusterManagerNodeReadAction}
 */
@Deprecated
public abstract class TransportMasterNodeReadAction<Request extends MasterNodeReadRequest<Request>, Response extends ActionResponse> extends
    TransportClusterManagerNodeReadAction<Request, Response> {

    protected TransportMasterNodeReadAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected TransportMasterNodeReadAction(
        String actionName,
        boolean checkSizeLimit,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            actionName,
            checkSizeLimit,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            request,
            indexNameExpressionResolver
        );
    }

}
