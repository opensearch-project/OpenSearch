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

package org.opensearch.action.admin.indices.get;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.info.TransportClusterInfoAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Get index action.
 *
 * @opensearch.internal
 */
public class TransportGetIndexAction extends TransportClusterInfoAction<GetIndexRequest, GetIndexResponse> {

    private final IndicesService indicesService;
    private final IndexScopedSettings indexScopedSettings;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        IndexScopedSettings indexScopedSettings
    ) {
        super(
            GetIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetIndexRequest::new,
            indexNameExpressionResolver
        );
        this.indicesService = indicesService;
        this.settingsFilter = settingsFilter;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    protected GetIndexResponse read(StreamInput in) throws IOException {
        return new GetIndexResponse(in);
    }

    @Override
    protected void doClusterManagerOperation(
        final GetIndexRequest request,
        String[] concreteIndices,
        final ClusterState state,
        final ActionListener<GetIndexResponse> listener
    ) {
        Map<String, MappingMetadata> mappingsResult = Map.of();
        Map<String, List<AliasMetadata>> aliasesResult = Map.of();
        Map<String, Settings> settings = Map.of();
        Map<String, Settings> defaultSettings = Map.of();
        final Map<String, String> dataStreams = new HashMap<>(
            StreamSupport.stream(Spliterators.spliterator(state.metadata().findDataStreams(concreteIndices).entrySet(), 0), false)
                .collect(Collectors.toMap(k -> k.getKey(), v -> v.getValue().getName()))
        );
        GetIndexRequest.Feature[] features = request.features();
        boolean doneAliases = false;
        boolean doneMappings = false;
        boolean doneSettings = false;
        for (GetIndexRequest.Feature feature : features) {
            switch (feature) {
                case MAPPINGS:
                    if (!doneMappings) {
                        try {
                            mappingsResult = state.metadata().findMappings(concreteIndices, indicesService.getFieldFilter());
                            doneMappings = true;
                        } catch (IOException e) {
                            listener.onFailure(e);
                            return;
                        }
                    }
                    break;
                case ALIASES:
                    if (doneAliases == false) {
                        aliasesResult = state.metadata().findAllAliases(concreteIndices);
                        doneAliases = true;
                    }
                    break;
                case SETTINGS:
                    if (!doneSettings) {
                        final Map<String, Settings> settingsMapBuilder = new HashMap<>();
                        final Map<String, Settings> defaultSettingsMapBuilder = new HashMap<>();
                        for (String index : concreteIndices) {
                            Settings indexSettings = state.metadata().index(index).getSettings();
                            if (request.humanReadable()) {
                                indexSettings = IndexMetadata.addHumanReadableSettings(indexSettings);
                            }
                            settingsMapBuilder.put(index, indexSettings);
                            if (request.includeDefaults()) {
                                Settings defaultIndexSettings = settingsFilter.filter(
                                    indexScopedSettings.diff(indexSettings, Settings.EMPTY)
                                );
                                defaultSettingsMapBuilder.put(index, defaultIndexSettings);
                            }
                        }
                        settings = settingsMapBuilder;
                        defaultSettings = defaultSettingsMapBuilder;
                        doneSettings = true;
                    }
                    break;

                default:
                    throw new IllegalStateException("feature [" + feature + "] is not valid");
            }
        }
        listener.onResponse(new GetIndexResponse(concreteIndices, mappingsResult, aliasesResult, settings, defaultSettings, dataStreams));
    }
}
