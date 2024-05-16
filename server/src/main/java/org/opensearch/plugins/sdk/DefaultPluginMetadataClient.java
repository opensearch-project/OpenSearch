/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.plugins.sdk.actions.ConcreteIndicesRequest;
import org.opensearch.plugins.sdk.actions.ConcreteIndicesResponse;
import org.opensearch.plugins.sdk.actions.GetIndexMappingMetadataRequest;
import org.opensearch.plugins.sdk.actions.GetIndexMappingMetadataResponse;
import org.opensearch.plugins.sdk.actions.GetIndexSettingsRequest;
import org.opensearch.plugins.sdk.actions.GetIndexSettingsResponse;
import org.opensearch.plugins.sdk.actions.GetSystemSettingsRequest;
import org.opensearch.plugins.sdk.actions.GetSystemSettingsResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * DefaultPluginMetadataClient is the de-facto implementation of the
 * PluginMetadataClient.
 *
 * @see PluginMetadataClient
 */
@ExperimentalApi
public class DefaultPluginMetadataClient implements PluginMetadataClient {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;

    public DefaultPluginMetadataClient(ClusterService clusterService,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       IndexScopedSettings indexScopedSettings) {

        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
    }

    @Override
    public CompletionStage<ConcreteIndicesResponse> concreteIndices(
        ConcreteIndicesRequest request
    ) {
        return synchronously(() -> {
            ClusterState state = clusterService.state();

            Index[] result = indexNameExpressionResolver.concreteIndices(
                state,
                request.indicesOptions(),
                request.indexExpressions());

            return ConcreteIndicesResponse.builder()
                .indices(result)
                .build();
        });
    }

    @Override
    public CompletionStage<GetSystemSettingsResponse> getSystemSettings(
        GetSystemSettingsRequest request
    ) {
        return synchronously(() -> {
            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            Settings settings = clusterSettings.toSettings();
            Set<String> settingKeys = Set.of(request.settings());
            settings = settings.filter(settingKeys::contains);
            return GetSystemSettingsResponse.builder()
                .settings(settings)
                .build();
        });
    }

    @Override
    public CompletionStage<GetIndexMappingMetadataResponse> getIndexMappingMetadata(
        GetIndexMappingMetadataRequest request
    ) {
        return synchronously(() -> {
            ClusterState state = clusterService.state();
            Metadata metadata = state.metadata();
            Map<Index, MappingMetadata> result = new HashMap<>();
            for (Index index : request.indices()) {
                MappingMetadata mappingMetadata = metadata.getIndexSafe(index).mapping();
                result.put(index, mappingMetadata);
            }
            return GetIndexMappingMetadataResponse.builder()
                .mappingMetadata(result)
                .build();
        });
    }

    @Override
    public CompletionStage<GetIndexSettingsResponse> getIndexSettings(
        GetIndexSettingsRequest request
    ) {
        return synchronously(() -> {
            ClusterState state = clusterService.state();
            Settings currentDefaultIndexScopedSettings = indexScopedSettings.toSettings();
            Set<String> settingsKeys = Set.of(request.settings());
            Map<Index, Settings> result = new HashMap<>();

            for (Index index : request.indices()) {
                IndexMetadata indexMetadata = state.metadata().getIndexSafe(index);

                Settings indexSettings = indexMetadata.getSettings();

                Settings resultSettings = Settings.builder()
                    .put(currentDefaultIndexScopedSettings)
                    .put(indexSettings)
                    .build()
                    .filter(settingsKeys::contains);

                result.put(index, resultSettings);
            }

            return GetIndexSettingsResponse.builder()
                .settings(result)
                .build();
        });
    }

    private static <T> CompletionStage<T> synchronously(CheckedSupplier<T, Exception> supplier) {
        try {
            T result = supplier.get();
            return CompletableFuture.completedStage(result);
        } catch (Exception e) {
            return CompletableFuture.failedStage(e);
        }
    }
}
