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

package org.opensearch.action.admin.indices.alias.exists;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.master.TransportMasterNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportAliasesExistAction extends TransportMasterNodeReadAction<GetAliasesRequest, AliasesExistResponse> {

    @Inject
    public TransportAliasesExistAction(TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(AliasesExistAction.NAME, transportService, clusterService, threadPool, actionFilters, GetAliasesRequest::new,
            indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AliasesExistResponse read(StreamInput in) throws IOException {
        return new AliasesExistResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(GetAliasesRequest request, ClusterState state, ActionListener<AliasesExistResponse> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        boolean result = state.metadata().hasAliases(request.aliases(), concreteIndices);
        listener.onResponse(new AliasesExistResponse(result));
    }

}
