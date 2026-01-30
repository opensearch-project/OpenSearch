/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.loadplugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
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

/**
 * Transport action for loading plugins
 *
 * @opensearch.internal
 */
public class TransportLoadPluginsAction extends TransportClusterManagerNodeAction<LoadPluginsRequest, LoadPluginsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLoadPluginsAction.class);
    private final PluginsService pluginsService;
    private final AnalysisRegistry analysisRegistry;

    @Inject
    public TransportLoadPluginsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        PluginsService pluginsService,
        AnalysisRegistry analysisRegistry
    ) {
        super(
            LoadPluginsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            LoadPluginsRequest::new,
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
    protected LoadPluginsResponse read(StreamInput in) throws IOException {
        return new LoadPluginsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(LoadPluginsRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        LoadPluginsRequest request,
        ClusterState state,
        ActionListener<LoadPluginsResponse> listener
    ) throws Exception {
        try {
            List<String> loadedPlugins = new ArrayList<>();
            
            // Load plugins dynamically using PluginsService
            if (request.getPluginPath() != null) {
                // Load plugin from specified path
                try {
                    // First check if plugin is already loaded to prevent duplicates
                    String pluginPath = request.getPluginPath();
                    String potentialPluginName = java.nio.file.Paths.get(pluginPath).getFileName().toString();
                    
                    // Check if plugin is already loaded by checking both directory name and actual plugin names
                    boolean pluginAlreadyLoaded = false;
                    String existingPluginName = null;
                    
                    // Check by directory name first
                    if (pluginsService.getPluginByName(potentialPluginName) != null) {
                        pluginAlreadyLoaded = true;
                        existingPluginName = potentialPluginName;
                    } else {
                        // Also check if any plugin with this directory name is already loaded
                        // by checking all loaded plugins
                        for (org.opensearch.plugins.PluginInfo pluginInfo : pluginsService.info().getPluginInfos()) {
                            if (pluginInfo.getName().equals(potentialPluginName)) {
                                pluginAlreadyLoaded = true;
                                existingPluginName = pluginInfo.getName();
                                break;
                            }
                        }
                    }
                    
                    if (pluginAlreadyLoaded) {
                        // Plugin already loaded, just mark as loaded and skip actual loading
                        loadedPlugins.add(existingPluginName);
                        pluginsService.markPluginAsLoaded(existingPluginName);
                        logger.info("Plugin [{}] is already loaded, skipping duplicate load", existingPluginName);
                    } else {
                        Plugin plugin = pluginsService.loadPluginDynamically(java.nio.file.Paths.get(request.getPluginPath()));
                        String pluginClassName = plugin.getClass().getSimpleName();
                        loadedPlugins.add(pluginClassName);
                        
                        // Mark plugin as loaded via the load API
                        // We need to find the actual plugin name from PluginInfo, not just the class name
                        String pluginName = findPluginNameByClass(plugin.getClass().getName());
                        if (pluginName != null) {
                            pluginsService.markPluginAsLoaded(pluginName);
                        } else {
                            // Fallback to class name if we can't find the plugin info name
                            pluginsService.markPluginAsLoaded(pluginClassName);
                        }
                        
                        // If it's an analysis plugin and refresh is requested, add to analysis registry
                        if (request.isRefreshAnalysis() && plugin instanceof AnalysisPlugin) {
                            analysisRegistry.addAnalysisPlugin((AnalysisPlugin) plugin);
                        }
                    }
                } catch (Exception e) {
                    throw new Exception("Failed to load plugin from path [" + request.getPluginPath() + "]: " + e.getMessage(), e);
                }
            } else if (request.getPluginName() != null) {
                // Load plugin by name from plugins directory
                // Note: This assumes a standard plugins directory structure
                // In a production implementation, you'd get the plugins directory from settings
                String pluginsDir = System.getProperty("opensearch.path.plugins", "plugins");
                try {
                    Plugin plugin = pluginsService.loadPluginByName(request.getPluginName(), java.nio.file.Paths.get(pluginsDir));
                    String pluginClassName = plugin.getClass().getSimpleName();
                    loadedPlugins.add(pluginClassName);
                    
                    // Mark plugin as loaded via the load API
                    // We need to find the actual plugin name from PluginInfo, not just the class name
                    String pluginName = findPluginNameByClass(plugin.getClass().getName());
                    if (pluginName != null) {
                        pluginsService.markPluginAsLoaded(pluginName);
                    } else {
                        // Fallback to class name if we can't find the plugin info name
                        pluginsService.markPluginAsLoaded(pluginClassName);
                    }
                    
                    // If it's an analysis plugin and refresh is requested, add to analysis registry
                    if (request.isRefreshAnalysis() && plugin instanceof AnalysisPlugin) {
                        analysisRegistry.addAnalysisPlugin((AnalysisPlugin) plugin);
                    }
                } catch (Exception e) {
                    throw new Exception("Failed to load plugin by name [" + request.getPluginName() + "]: " + e.getMessage(), e);
                }
            } else {
                // If no specific plugin is requested, just refresh analysis registry with existing plugins
                if (request.isRefreshAnalysis()) {
                    List<AnalysisPlugin> analysisPlugins = pluginsService.filterPlugins(AnalysisPlugin.class);
                    for (AnalysisPlugin plugin : analysisPlugins) {
                        analysisRegistry.addAnalysisPlugin(plugin);
                        loadedPlugins.add(plugin.getClass().getSimpleName());
                    }
                }
            }
            
            String message = loadedPlugins.isEmpty() 
                ? "No plugins loaded" 
                : "Successfully loaded " + loadedPlugins.size() + " plugin(s)";
            
            listener.onResponse(new LoadPluginsResponse(true, message, loadedPlugins));
        } catch (Exception e) {
            listener.onResponse(new LoadPluginsResponse(false, "Failed to load plugins: " + e.getMessage(), new ArrayList<>()));
        }
    }
    
    /**
     * Find the plugin name from PluginInfo by matching the class name
     */
    private String findPluginNameByClass(String className) {
        try {
            // Get all plugin info and find the one with matching class name
            for (org.opensearch.plugins.PluginInfo pluginInfo : pluginsService.info().getPluginInfos()) {
                if (className.equals(pluginInfo.getClassname())) {
                    return pluginInfo.getName();
                }
            }
        } catch (Exception e) {
            // If we can't find it, return null and fallback to class name
        }
        return null;
    }
}
