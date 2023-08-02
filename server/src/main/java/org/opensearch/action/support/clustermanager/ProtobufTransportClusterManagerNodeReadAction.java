/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager;

import org.opensearch.action.ProtobufActionResponse;
import org.opensearch.action.support.ProtobufActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * A base class for read operations that needs to be performed on the cluster-manager node.
 * Can also be executed on the local node if needed.
 *
 * @opensearch.internal
 */
public abstract class ProtobufTransportClusterManagerNodeReadAction<
    Request extends ProtobufClusterManagerNodeReadRequest<Request>,
    Response extends ProtobufActionResponse> extends ProtobufTransportClusterManagerNodeAction<Request, Response> {

    protected ProtobufTransportClusterManagerNodeReadAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this(actionName, true, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    protected ProtobufTransportClusterManagerNodeReadAction(
        String actionName,
        boolean checkSizeLimit,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ProtobufActionFilters actionFilters,
        ProtobufWriteable.Reader<Request> request,
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

    @Override
    protected final boolean localExecute(Request request) {
        return request.local();
    }
}
