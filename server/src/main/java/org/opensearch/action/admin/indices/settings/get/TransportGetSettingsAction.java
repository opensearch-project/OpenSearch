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

package org.opensearch.action.admin.indices.settings.get;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.util.CollectionUtils;
import org.opensearch.index.Index;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport action for getting index settings
 *
 * @opensearch.internal
 */
public class TransportGetSettingsAction extends TransportClusterManagerNodeReadAction<GetSettingsRequest, GetSettingsResponse> {

    private final SettingsFilter settingsFilter;
    private final IndexScopedSettings indexScopedSettings;

    @Inject
    public TransportGetSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        SettingsFilter settingsFilter,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexedScopedSettings
    ) {
        super(
            GetSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetSettingsRequest::new,
            indexNameExpressionResolver
        );
        this.settingsFilter = settingsFilter;
        this.indexScopedSettings = indexedScopedSettings;
    }

    @Override
    protected String executor() {
        // Very lightweight operation
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSettingsRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected GetSettingsResponse read(StreamInput in) throws IOException {
        return new GetSettingsResponse(in);
    }

    private static boolean isFilteredRequest(GetSettingsRequest request) {
        return CollectionUtils.isEmpty(request.names()) == false;
    }

    @Override
    protected void clusterManagerOperation(GetSettingsRequest request, ClusterState state, ActionListener<GetSettingsResponse> listener) {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        final Map<String, Settings> indexToSettingsBuilder = new HashMap<>();
        final Map<String, Settings> indexToDefaultSettingsBuilder = new HashMap<>();
        for (Index concreteIndex : concreteIndices) {
            IndexMetadata indexMetadata = state.getMetadata().index(concreteIndex);
            if (indexMetadata == null) {
                continue;
            }

            Settings indexSettings = settingsFilter.filter(indexMetadata.getSettings());
            if (request.humanReadable()) {
                indexSettings = IndexMetadata.addHumanReadableSettings(indexSettings);
            }

            if (isFilteredRequest(request)) {
                indexSettings = indexSettings.filter(k -> Regex.simpleMatch(request.names(), k));
            }

            indexToSettingsBuilder.put(concreteIndex.getName(), indexSettings);
            if (request.includeDefaults()) {
                Settings defaultSettings = settingsFilter.filter(indexScopedSettings.diff(indexSettings, Settings.EMPTY));
                if (isFilteredRequest(request)) {
                    defaultSettings = defaultSettings.filter(k -> Regex.simpleMatch(request.names(), k));
                }
                indexToDefaultSettingsBuilder.put(concreteIndex.getName(), defaultSettings);
            }
        }
        listener.onResponse(new GetSettingsResponse(indexToSettingsBuilder, indexToDefaultSettingsBuilder));
    }
}
