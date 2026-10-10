/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.validatepluginusage;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugins.PluginDependencyService;
import org.opensearch.plugins.PluginReloadValidation;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for validating plugin usage before uninstallation
 *
 * @opensearch.internal
 */
public class TransportValidatePluginUsageAction extends TransportClusterManagerNodeAction<
    ValidatePluginUsageRequest, ValidatePluginUsageResponse> {

    private final PluginDependencyService pluginDependencyService;

    @Inject
    public TransportValidatePluginUsageAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        PluginDependencyService pluginDependencyService
    ) {
        super(
            ValidatePluginUsageAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ValidatePluginUsageRequest::new,
            indexNameExpressionResolver
        );
        this.pluginDependencyService = pluginDependencyService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ValidatePluginUsageResponse read(StreamInput in) throws IOException {
        return new ValidatePluginUsageResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ValidatePluginUsageRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void clusterManagerOperation(
        ValidatePluginUsageRequest request,
        ClusterState state,
        ActionListener<ValidatePluginUsageResponse> listener
    ) throws Exception {
        try {
            String pluginName = request.getPluginName();
            
            // Validate plugin exists
            if (!isPluginLoaded(pluginName, state)) {
                ValidatePluginUsageResponse response = new ValidatePluginUsageResponse()
                    .setPluginName(pluginName)
                    .setSafeToUnload(true)
                    .setActiveUsageCount(0)
                    .setRiskLevel("LOW")
                    .setRecommendations(List.of("Plugin is not currently loaded"))
                    .setErrorMessage("Plugin '" + pluginName + "' is not currently loaded");
                listener.onResponse(response);
                return;
            }

            // Perform validation using dependency service
            PluginReloadValidation validation = pluginDependencyService.validatePluginReload(pluginName);
            ValidatePluginUsageResponse response = new ValidatePluginUsageResponse(validation);
            
            // Add additional context if requested
            if (request.isIncludeUsageDetails()) {
                // Add detailed usage information
                response.setAffectedIndices(getAffectedIndices(pluginName));
                response.setAffectedPipelines(getAffectedPipelines(pluginName));
            }
            
            listener.onResponse(response);
            
        } catch (Exception e) {
            ValidatePluginUsageResponse errorResponse = new ValidatePluginUsageResponse()
                .setPluginName(request.getPluginName())
                .setSafeToUnload(false)
                .setActiveUsageCount(0)
                .setRiskLevel("UNKNOWN")
                .setErrorMessage("Error validating plugin usage: " + e.getMessage());
            listener.onResponse(errorResponse);
        }
    }

    private boolean isPluginLoaded(String pluginName, ClusterState state) {
        // Check if plugin is currently loaded
        // This would integrate with the existing plugin tracking system
        return true; // Simplified for now
    }

    private List<String> getAffectedIndices(String pluginName) {
        // Scan cluster state for indices using this plugin
        // This would analyze index mappings for plugin-specific analyzers
        return List.of(); // Simplified for now
    }

    private List<String> getAffectedPipelines(String pluginName) {
        // Scan ingest pipelines for plugin processors
        return List.of(); // Simplified for now
    }
}
