/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support.clustermanager.info;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Perform cluster information action
 *
 * @opensearch.internal
 */
public abstract class TransportClusterInfoAction<Request extends ClusterInfoRequest<Request>, Response extends ActionResponse> extends
    TransportClusterManagerNodeReadAction<Request, Response> {

    public TransportClusterInfoAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(actionName, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        // read operation, lightweight...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #clusterManagerOperation(ClusterInfoRequest, ClusterState, ActionListener)} */
    @Deprecated
    protected final void masterOperation(final Request request, final ClusterState state, final ActionListener<Response> listener) {
        clusterManagerOperation(request, state, listener);
    }

    @Override
    protected final void clusterManagerOperation(final Request request, final ClusterState state, final ActionListener<Response> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        doClusterManagerOperation(request, concreteIndices, state, listener);
    }

    // TODO: Add abstract keyword after removing the deprecated doMasterOperation()
    protected void doClusterManagerOperation(
        Request request,
        String[] concreteIndices,
        ClusterState state,
        ActionListener<Response> listener
    ) {
        doMasterOperation(request, concreteIndices, state, listener);
    }

    /**
     * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #doClusterManagerOperation(ClusterInfoRequest, String[], ClusterState, ActionListener)}
     */
    @Deprecated
    protected void doMasterOperation(Request request, String[] concreteIndices, ClusterState state, ActionListener<Response> listener) {
        throw new UnsupportedOperationException("Must be overridden");
    }

}
