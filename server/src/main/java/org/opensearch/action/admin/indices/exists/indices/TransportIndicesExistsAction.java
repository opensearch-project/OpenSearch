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

package org.opensearch.action.admin.indices.exists.indices;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Indices exists action.
 *
 * @opensearch.internal
 */
public class TransportIndicesExistsAction extends TransportClusterManagerNodeReadAction<IndicesExistsRequest, IndicesExistsResponse> {

    @Inject
    public TransportIndicesExistsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            IndicesExistsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndicesExistsRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        // lightweight in memory check
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesExistsResponse read(StreamInput in) throws IOException {
        return new IndicesExistsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesExistsRequest request, ClusterState state) {
        // make sure through indices options that the concrete indices call never throws IndexMissingException
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            true,
            true,
            request.indicesOptions().expandWildcardsOpen(),
            request.indicesOptions().expandWildcardsClosed()
        );
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.indices())
            );
    }

    @Override
    protected void clusterManagerOperation(
        final IndicesExistsRequest request,
        final ClusterState state,
        final ActionListener<IndicesExistsResponse> listener
    ) {
        boolean exists;
        try {
            // Similar as the previous behaviour, but now also aliases and wildcards are supported.
            indexNameExpressionResolver.concreteIndexNames(state, request);
            exists = true;
        } catch (IndexNotFoundException e) {
            exists = false;
        }
        listener.onResponse(new IndicesExistsResponse(exists));
    }
}
