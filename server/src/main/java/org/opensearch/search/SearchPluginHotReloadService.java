/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.plugins.SearchPlugin.QuerySpec;
import org.opensearch.plugins.SearchPlugin.AggregationSpec;
import org.opensearch.plugins.SearchPlugin.SuggesterSpec;
import org.opensearch.plugins.SearchPlugin.ScoreFunctionSpec;
import org.opensearch.plugins.SearchPlugin.RescorerSpec;
import org.opensearch.plugins.SearchPlugin.SortSpec;
import org.opensearch.plugins.SearchPlugin.PipelineAggregationSpec;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for hot reloading search plugins
 * Coordinates between PluginsService, SearchModule, and IndicesService
 * 
 * @opensearch.internal
 */
@ExperimentalApi
public class SearchPluginHotReloadService {
    
    private static final Logger logger = LogManager.getLogger(SearchPluginHotReloadService.class);
    
    private final PluginsService pluginsService;
    private final IndicesService indicesService;
    private final SearchModule searchModule;
    
    // Track dynamically loaded search plugins
    private final ConcurrentHashMap<String, SearchPlugin> loadedSearchPlugins;
    
    @Inject
    public SearchPluginHotReloadService(
        PluginsService pluginsService,
        IndicesService indicesService,
        SearchModule searchModule
    ) {
        this.pluginsService = pluginsService;
        this.indicesService = indicesService;
        this.searchModule = searchModule;
        this.loadedSearchPlugins = new ConcurrentHashMap<>();
    }
    
    /**
     * Load a search plugin and register its components dynamically
     */
    public SearchPluginLoadResult loadSearchPlugin(String pluginPath, boolean refreshSearch) {
        try {
            logger.info("Loading search plugin from path: {}", pluginPath);
            
            // Check if plugin path is valid
            Path path = Paths.get(pluginPath);
            if (!path.toFile().exists()) {
                return new SearchPluginLoadResult(
                    false,
                    "Plugin path does not exist: " + pluginPath,
                    null
                );
            }
            
            // Load plugin using existing PluginsService infrastructure
            Plugin plugin = pluginsService.loadPluginDynamically(path);
            String pluginName = plugin.getClass().getSimpleName();
            
            // Mark plugin as loaded in PluginsService
            pluginsService.markPluginAsLoaded(pluginName);
            
            // If it's a search plugin, extract and register search components
            if (plugin instanceof SearchPlugin) {
                SearchPlugin searchPlugin = (SearchPlugin) plugin;
                
                // Track the loaded search plugin
                loadedSearchPlugins.put(pluginName, searchPlugin);
                
                if (refreshSearch) {
                    // Register search components directly
                    registerSearchComponents(pluginName, searchPlugin);
                    
                    logger.info("Successfully registered search components for plugin: {}", pluginName);
                }
                
                return new SearchPluginLoadResult(
                    true,
                    "Successfully loaded search plugin: " + pluginName,
                    pluginName
                );
            } else {
                // Plugin loaded but no search components
                return new SearchPluginLoadResult(
                    true,
                    "Plugin loaded but contains no search components: " + pluginName,
                    pluginName
                );
            }
            
        } catch (Exception e) {
            logger.error("Failed to load search plugin from path: {}", pluginPath, e);
            return new SearchPluginLoadResult(
                false,
                "Failed to load plugin: " + e.getMessage(),
                null
            );
        }
    }
    
    /**
     * Register search components from a plugin using SearchModule dynamic tracking
     */
    private void registerSearchComponents(String pluginName, SearchPlugin plugin) {
        for (QuerySpec<?> spec : plugin.getQueries()) {
            searchModule.addQuerySpec(pluginName, spec);
        }
        
        for (AggregationSpec spec : plugin.getAggregations()) {
            searchModule.addAggregationSpec(pluginName, spec);
        }
        
        logger.info("Registered {} search components for plugin: {}", 
            plugin.getQueries().size() + plugin.getAggregations().size(), pluginName);
        
        updateIndicesServiceRegistries();
    }
    
    /**
     * Update the registries in IndicesService with merged components using SearchModule
     */
    private void updateIndicesServiceRegistries() {
        try {
            NamedXContentRegistry mergedXContentRegistry = searchModule.buildUpdatedXContentRegistry();
            NamedWriteableRegistry mergedWriteableRegistry = searchModule.buildUpdatedWriteableRegistry();
            
            indicesService.updateXContentRegistry(mergedXContentRegistry);
            indicesService.updateNamedWriteableRegistry(mergedWriteableRegistry);
            
            logger.info("Updated registries with merged search plugin components");
        } catch (Exception e) {
            logger.error("Failed to update registries", e);
            throw new RuntimeException("Registry update failed", e);
        }
    }
    
    /**
     * Remove a search plugin and its components
     */
    public SearchPluginLoadResult removeSearchPlugin(String pluginName) {
        try {
            SearchPlugin searchPlugin = loadedSearchPlugins.remove(pluginName);
            if (searchPlugin == null) {
                return new SearchPluginLoadResult(
                    false,
                    "Search plugin not found: " + pluginName,
                    null
                );
            }
            
            // For POC: Simply remove from PluginsService
            // In production, we'd also remove from registries
            boolean removed = pluginsService.unloadPluginDynamically(pluginName);
            
            return new SearchPluginLoadResult(
                removed,
                removed ? "Successfully removed search plugin: " + pluginName : "Failed to remove plugin",
                pluginName
            );
        } catch (Exception e) {
            logger.error("Failed to remove search plugin: {}", pluginName, e);
            return new SearchPluginLoadResult(
                false,
                "Failed to remove plugin: " + e.getMessage(),
                null
            );
        }
    }
    
    /**
     * Get list of loaded search plugins
     */
    public List<String> getLoadedSearchPlugins() {
        return new ArrayList<>(loadedSearchPlugins.keySet());
    }
    
    /**
     * Result object for search plugin operations
     */
    @ExperimentalApi
    public static class SearchPluginLoadResult {
        private final boolean success;
        private final String message;
        private final String pluginName;
        
        public SearchPluginLoadResult(boolean success, String message, String pluginName) {
            this.success = success;
            this.message = message;
            this.pluginName = pluginName;
        }
        
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public String getPluginName() { return pluginName; }
    }
}
