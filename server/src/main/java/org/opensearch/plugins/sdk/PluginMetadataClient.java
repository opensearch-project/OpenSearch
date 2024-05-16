/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.sdk.actions.ConcreteIndicesRequest;
import org.opensearch.plugins.sdk.actions.ConcreteIndicesResponse;
import org.opensearch.plugins.sdk.actions.GetIndexMappingMetadataRequest;
import org.opensearch.plugins.sdk.actions.GetIndexMappingMetadataResponse;
import org.opensearch.plugins.sdk.actions.GetIndexSettingsRequest;
import org.opensearch.plugins.sdk.actions.GetIndexSettingsResponse;
import org.opensearch.plugins.sdk.actions.GetSystemSettingsRequest;
import org.opensearch.plugins.sdk.actions.GetSystemSettingsResponse;

import java.util.concurrent.CompletionStage;

/**
 * PluginMetadataClient provides an abstract interface for plugins to access OpenSearch core cluster metadata.
 */
@ExperimentalApi
public interface PluginMetadataClient {

    /**
     * Resolves the concrete indices for a given set of index name expressions.
     *
     * This is an abstraction over IndexNameExpressionResolver.
     *
     * @see org.opensearch.cluster.metadata.IndexNameExpressionResolver#concreteIndexNames(ClusterState, IndicesOptions, String...)
     *
     * @param request The index names to resolve and associated resolution options
     * @return The resolved concrete index names
     */
    CompletionStage<ConcreteIndicesResponse> concreteIndices(ConcreteIndicesRequest request);

    /**
     * Gets the system settings for a particular set of setting.
     *
     * This is an abstraction over ClusterSetting access.
     *
     * @see org.opensearch.common.settings.ClusterSettings#get(Setting)
     *
     * @param request The settings to retrieve.
     * @return The system settings.
     */
    CompletionStage<GetSystemSettingsResponse> getSystemSettings(GetSystemSettingsRequest request);

    /**
     * Gets the mapping metadata of the provided indices.
     *
     * This is an abstraction over ClusterState Metadata access.
     *
     * @see org.opensearch.cluster.metadata.Metadata#indices()
     *
     * @param request The indices to retrieve mapping metadata for.
     * @return The mapping metadata for the requested indices.
     */
    CompletionStage<GetIndexMappingMetadataResponse> getIndexMappingMetadata(GetIndexMappingMetadataRequest request);

    /**
     * Gets the index settings for a particular set of indices.
     *
     * This is an abstraction over ClusterState Metadata access.
     *
     * @see org.opensearch.cluster.metadata.IndexMetadata#getSettings()
     *
     * @param request The indices to retrieve mapping metadata for.
     * @return The mapping metadata for the requested indices.
     */
    CompletionStage<GetIndexSettingsResponse> getIndexSettings(GetIndexSettingsRequest request);
}
