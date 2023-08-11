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

package org.opensearch.action.admin.indices.mapping.get;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Transport action to get field mappings.
 *
 * @opensearch.internal
 */
public class TransportGetFieldAliasesMappingsAction extends HandledTransportAction<
    GetFieldAliasesMappingsRequest,
    GetFieldAliasesMappingsResponse> {

    private final ClusterService clusterService;
    private final TransportGetFieldAliasesMappingsIndexAction shardAction;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportGetFieldAliasesMappingsAction(
        TransportService transportService,
        ClusterService clusterService,
        TransportGetFieldAliasesMappingsIndexAction shardAction,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(GetFieldAliasesMappingsAction.NAME, transportService, actionFilters, GetFieldAliasesMappingsRequest::new);
        this.clusterService = clusterService;
        this.shardAction = shardAction;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void doExecute(
        Task task,
        GetFieldAliasesMappingsRequest request,
        final ActionListener<GetFieldAliasesMappingsResponse> listener
    ) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        final AtomicInteger indexCounter = new AtomicInteger();
        final AtomicInteger completionCounter = new AtomicInteger(concreteIndices.length);
        final AtomicReferenceArray<Object> indexResponses = new AtomicReferenceArray<>(concreteIndices.length);

        if (concreteIndices.length == 0) {
            listener.onResponse(new GetFieldAliasesMappingsResponse(emptyMap()));
        } else {
            for (final String index : concreteIndices) {
                GetFieldAliasesMappingsIndexRequest shardRequest = new GetFieldAliasesMappingsIndexRequest(request, index);
                shardAction.execute(shardRequest, new ActionListener<GetFieldAliasesMappingsResponse>() {
                    @Override
                    public void onResponse(GetFieldAliasesMappingsResponse result) {
                        indexResponses.set(indexCounter.getAndIncrement(), result);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(indexResponses));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        int index = indexCounter.getAndIncrement();
                        indexResponses.set(index, e);
                        if (completionCounter.decrementAndGet() == 0) {
                            listener.onResponse(merge(indexResponses));
                        }
                    }
                });
            }
        }
    }

    private GetFieldAliasesMappingsResponse merge(AtomicReferenceArray<Object> indexResponses) {
        Map<String, Map<String, GetFieldAliasesMappingsResponse.FieldAliasesMappingMetadata>> mergedResponses = new HashMap<>();
        for (int i = 0; i < indexResponses.length(); i++) {
            Object element = indexResponses.get(i);
            if (element instanceof GetFieldAliasesMappingsResponse) {
                GetFieldAliasesMappingsResponse response = (GetFieldAliasesMappingsResponse) element;
                mergedResponses.putAll(response.mappings());
            }
        }
        return new GetFieldAliasesMappingsResponse(unmodifiableMap(mergedResponses));
    }
}
