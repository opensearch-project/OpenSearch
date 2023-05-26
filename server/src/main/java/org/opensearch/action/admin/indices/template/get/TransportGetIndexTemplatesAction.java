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

package org.opensearch.action.admin.indices.template.get;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.IndexTemplateMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.regex.Regex;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Transport action to retrieve one or more Index templates
 *
 * @opensearch.internal
 */
public class TransportGetIndexTemplatesAction extends TransportClusterManagerNodeReadAction<
    GetIndexTemplatesRequest,
    GetIndexTemplatesResponse> {

    @Inject
    public TransportGetIndexTemplatesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetIndexTemplatesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetIndexTemplatesRequest::new,
            indexNameExpressionResolver
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetIndexTemplatesResponse read(StreamInput in) throws IOException {
        return new GetIndexTemplatesResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexTemplatesRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        GetIndexTemplatesRequest request,
        ClusterState state,
        ActionListener<GetIndexTemplatesResponse> listener
    ) {
        List<IndexTemplateMetadata> results;

        // If we did not ask for a specific name, then we return all templates
        if (request.names().length == 0) {
            results = Arrays.asList(state.metadata().templates().values().toArray(new IndexTemplateMetadata[0]));
        } else {
            results = new ArrayList<>();
        }

        for (String name : request.names()) {
            if (Regex.isSimpleMatchPattern(name)) {
                for (final Map.Entry<String, IndexTemplateMetadata> entry : state.metadata().templates().entrySet()) {
                    if (Regex.simpleMatch(name, entry.getKey())) {
                        results.add(entry.getValue());
                    }
                }
            } else if (state.metadata().templates().containsKey(name)) {
                results.add(state.metadata().templates().get(name));
            }
        }

        listener.onResponse(new GetIndexTemplatesResponse(results));
    }
}
