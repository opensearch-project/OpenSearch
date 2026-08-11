/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.removeplugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Transport action for removing plugins
 *
 * @opensearch.internal
 */
public class TransportRemovePluginsAction extends TransportClusterManagerNodeAction<RemovePluginsRequest, RemovePluginsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRemovePluginsAction.class);
    
    private final PluginsService pluginsService;
    private final AnalysisRegistry analysisRegistry;

    @Inject
    public TransportRemovePluginsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        PluginsService pluginsService,
        AnalysisRegistry analysisRegistry
    ) {
        super(
            RemovePluginsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RemovePluginsRequest::new,
            indexNameExpressionResolver
        );
        this.pluginsService = pluginsService;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RemovePluginsResponse read(StreamInput in) throws IOException {
        return new RemovePluginsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(RemovePluginsRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        RemovePluginsRequest request,
        ClusterState state,
        ActionListener<RemovePluginsResponse> listener
    ) throws Exception {
        try {
            List<String> removedPlugins = new ArrayList<>();
            List<String> failedPlugins = new ArrayList<>();
            
            // Collect plugin names to remove
            List<String> pluginNamesToRemove = new ArrayList<>();
            if (request.getPluginName() != null) {
                pluginNamesToRemove.add(request.getPluginName());
            }
            if (request.getPluginNames() != null && !request.getPluginNames().isEmpty()) {
                pluginNamesToRemove.addAll(request.getPluginNames());
            }

            // Remove each plugin
            for (String pluginName : pluginNamesToRemove) {
                try {
                    // Check if plugin exists before attempting to remove
                    var pluginTuple = pluginsService.getPluginByName(pluginName);
                    if (pluginTuple == null) {
                        failedPlugins.add(pluginName + " (not found)");
                        continue;
                    }

                    // CRITICAL SAFETY CHECK: Validate plugin usage before removal
                    if (!request.isForceRemove()) {
                        List<String> affectedIndices = validatePluginUsage(pluginName, state);
                        if (!affectedIndices.isEmpty()) {
                            String errorMsg = String.format(
                                "%s (unsafe to remove: actively used by %d indices: %s. Use force=true to override)",
                                pluginName,
                                affectedIndices.size(),
                                String.join(", ", affectedIndices.subList(0, Math.min(3, affectedIndices.size())))
                            );
                            failedPlugins.add(errorMsg);
                            logger.warn("Prevented unsafe plugin removal: {} is used by indices: {}", pluginName, affectedIndices);
                            continue;
                        }
                    } else {
                        // Log warning for forced removal
                        List<String> affectedIndices = validatePluginUsage(pluginName, state);
                        if (!affectedIndices.isEmpty()) {
                            logger.warn("FORCED plugin removal: {} is used by {} indices: {}", 
                                pluginName, affectedIndices.size(), affectedIndices);
                        }
                    }

                    // If it's an analysis plugin and refresh is requested, remove from analysis registry
                    if (request.isRefreshAnalysis() && pluginTuple.v2() instanceof AnalysisPlugin) {
                        try {
                            analysisRegistry.removeAnalysisPlugin((AnalysisPlugin) pluginTuple.v2());
                        } catch (Exception e) {
                            // Log warning but continue with plugin removal
                            logger.warn("Failed to remove analysis plugin [{}] from registry: {}", pluginName, e.getMessage());
                        }
                    }

                    // Remove the plugin
                    boolean success = pluginsService.unloadPluginDynamically(pluginName);
                    if (success) {
                        removedPlugins.add(pluginName);
                    } else {
                        failedPlugins.add(pluginName + " (removal failed)");
                    }
                } catch (Exception e) {
                    failedPlugins.add(pluginName + " (error: " + e.getMessage() + ")");
                    logger.error("Failed to remove plugin [{}]: {}", pluginName, e.getMessage(), e);
                }
            }
            
            // Prepare response message
            StringBuilder messageBuilder = new StringBuilder();
            boolean success = failedPlugins.isEmpty();
            
            if (!removedPlugins.isEmpty()) {
                messageBuilder.append("Successfully removed ").append(removedPlugins.size()).append(" plugin(s)");
                if (!failedPlugins.isEmpty()) {
                    messageBuilder.append(", ");
                }
            }
            
            if (!failedPlugins.isEmpty()) {
                messageBuilder.append("Failed to remove ").append(failedPlugins.size()).append(" plugin(s): ")
                    .append(String.join(", ", failedPlugins));
            }
            
            if (removedPlugins.isEmpty() && failedPlugins.isEmpty()) {
                messageBuilder.append("No plugins specified for removal");
                success = false;
            }
            
            listener.onResponse(new RemovePluginsResponse(success, messageBuilder.toString(), removedPlugins));
        } catch (Exception e) {
            listener.onResponse(new RemovePluginsResponse(false, "Failed to remove plugins: " + e.getMessage(), new ArrayList<>()));
        }
    }

    /**
     * Validates if a plugin is being used by any indices
     * @param pluginName the name of the plugin to validate
     * @param state the current cluster state
     * @return list of indices that use the plugin
     */
    private List<String> validatePluginUsage(String pluginName, ClusterState state) {
        List<String> affectedIndices = new ArrayList<>();
        
        try {
            // Check each index for plugin usage in mappings
            for (String indexName : state.metadata().getConcreteAllIndices()) {
                if (indexUsesPlugin(indexName, pluginName, state)) {
                    affectedIndices.add(indexName);
                }
            }
        } catch (Exception e) {
            logger.warn("Error validating plugin usage for {}: {}", pluginName, e.getMessage());
        }
        
        return affectedIndices;
    }

    /**
     * Checks if an index uses a specific plugin by comparing index settings against analysis registry
     * @param indexName the index to check
     * @param pluginName the plugin name to look for
     * @param state the cluster state
     * @return true if the index uses the plugin
     */
    private boolean indexUsesPlugin(String indexName, String pluginName, ClusterState state) {
        try {
            var indexMetadata = state.metadata().index(indexName);
            if (indexMetadata == null) {
                return false;
            }

            // Get the plugin instance to determine what components it provides
            var pluginTuple = pluginsService.getPluginByName(pluginName);
            if (pluginTuple == null || !(pluginTuple.v2() instanceof AnalysisPlugin)) {
                // For non-analysis plugins, fall back to generic name matching
                return indexMetadata.getSettings().toString().toLowerCase().contains(pluginName.toLowerCase());
            }

            AnalysisPlugin analysisPlugin = (AnalysisPlugin) pluginTuple.v2();
            Set<String> pluginComponents = getPluginAnalysisComponents(analysisPlugin);
            
            if (pluginComponents.isEmpty()) {
                return false;
            }

            // Check index settings for usage of plugin components
            return indexUsesAnalysisComponents(indexMetadata, pluginComponents);
            
        } catch (Exception e) {
            logger.debug("Error checking plugin usage for index {}: {}", indexName, e.getMessage());
            return false;
        }
    }

    /**
     * Get all analysis components (analyzers, tokenizers, filters, etc.) provided by a plugin
     */
    private Set<String> getPluginAnalysisComponents(AnalysisPlugin plugin) {
        Set<String> components = new HashSet<>();
        
        try {
            // Get analyzers provided by the plugin
            Map<String, ?> analyzers = plugin.getAnalyzers();
            if (analyzers != null) {
                components.addAll(analyzers.keySet());
            }

            // Get tokenizers provided by the plugin
            Map<String, ?> tokenizers = plugin.getTokenizers();
            if (tokenizers != null) {
                components.addAll(tokenizers.keySet());
            }

            // Get token filters provided by the plugin
            Map<String, ?> tokenFilters = plugin.getTokenFilters();
            if (tokenFilters != null) {
                components.addAll(tokenFilters.keySet());
            }

            // Get character filters provided by the plugin
            Map<String, ?> charFilters = plugin.getCharFilters();
            if (charFilters != null) {
                components.addAll(charFilters.keySet());
            }


        } catch (Exception e) {
            logger.debug("Error getting plugin components: {}", e.getMessage());
        }
        
        return components;
    }

    /**
     * Check if index settings use any of the specified analysis components
     */
    private boolean indexUsesAnalysisComponents(org.opensearch.cluster.metadata.IndexMetadata indexMetadata, Set<String> components) {
        try {
            var settings = indexMetadata.getSettings();
            
            // Check analysis settings in index
            for (String key : settings.keySet()) {
                if (key.startsWith("index.analysis.")) {
                    String value = settings.get(key);
                    if (value != null) {
                        // Check if any plugin component is referenced in the analysis settings
                        for (String component : components) {
                            if (value.equals(component) || value.contains(component)) {
                                logger.debug("Index {} uses plugin component {} in setting {}", 
                                    indexMetadata.getIndex().getName(), component, key);
                                return true;
                            }
                        }
                    }
                }
            }

            // Also check mapping metadata for field analyzers
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata != null) {
                return checkMappingForAnalysisComponents(mappingMetadata, components);
            }

        } catch (Exception e) {
            logger.debug("Error checking analysis components usage: {}", e.getMessage());
        }
        
        return false;
    }

    /**
     * Check mapping metadata for usage of analysis components
     */
    private boolean checkMappingForAnalysisComponents(MappingMetadata mappingMetadata, Set<String> components) {
        try {
            Map<String, Object> sourceAsMap = mappingMetadata.sourceAsMap();
            return checkMapForAnalysisComponents(sourceAsMap, components);
        } catch (Exception e) {
            logger.debug("Error checking mapping for analysis components: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Recursively check a map (mapping structure) for analysis component usage
     */
    @SuppressWarnings("unchecked")
    private boolean checkMapForAnalysisComponents(Map<String, Object> map, Set<String> components) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            // Check for analyzer, search_analyzer, normalizer fields
            if (("analyzer".equals(key) || "search_analyzer".equals(key) || "normalizer".equals(key)) && value instanceof String) {
                if (components.contains((String) value)) {
                    return true;
                }
            }
            
            // Recursively check nested objects
            if (value instanceof Map) {
                if (checkMapForAnalysisComponents((Map<String, Object>) value, components)) {
                    return true;
                }
            }
        }
        return false;
    }
}
