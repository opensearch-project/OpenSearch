/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.loadsearchplugins;

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
import org.opensearch.search.SearchPluginHotReloadService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport action for loading search plugins
 *
 * @opensearch.internal
 */
public class TransportLoadSearchPluginsAction extends TransportClusterManagerNodeAction<LoadSearchPluginsRequest, LoadSearchPluginsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLoadSearchPluginsAction.class);
    private final SearchPluginHotReloadService hotReloadService;

    @Inject
    public TransportLoadSearchPluginsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SearchPluginHotReloadService hotReloadService
    ) {
        super(
            LoadSearchPluginsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            LoadSearchPluginsRequest::new,
            indexNameExpressionResolver
        );
        this.hotReloadService = hotReloadService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected LoadSearchPluginsResponse read(StreamInput in) throws IOException {
        return new LoadSearchPluginsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(LoadSearchPluginsRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        LoadSearchPluginsRequest request,
        ClusterState state,
        ActionListener<LoadSearchPluginsResponse> listener
    ) throws Exception {
        try {
            List<String> loadedPlugins = new ArrayList<>();
            
            // Load search plugin using the hot reload service
            if (request.getPluginPath() != null) {
                try {
                    SearchPluginHotReloadService.SearchPluginLoadResult result = 
                        hotReloadService.loadSearchPlugin(
                            request.getPluginPath(), 
                            request.isRefreshSearch()
                        );
                    
                    if (result.isSuccess()) {
                        if (result.getPluginName() != null) {
                            loadedPlugins.add(result.getPluginName());
                        }
                        
                        listener.onResponse(new LoadSearchPluginsResponse(
                            true,
                            result.getMessage(),
                            loadedPlugins
                        ));
                    } else {
                        listener.onResponse(new LoadSearchPluginsResponse(
                            false,
                            result.getMessage(),
                            loadedPlugins
                        ));
                    }
                } catch (Exception e) {
                    logger.error("Failed to load search plugin from path: {}", request.getPluginPath(), e);
                    listener.onResponse(new LoadSearchPluginsResponse(
                        false,
                        "Failed to load plugin: " + e.getMessage(),
                        new ArrayList<>()
                    ));
                }
            } else if (request.getPluginName() != null) {
                // For now, we only support loading by path
                // Plugin by name would require scanning plugins directory
                listener.onResponse(new LoadSearchPluginsResponse(
                    false,
                    "Loading by plugin name not yet supported. Use plugin_path parameter.",
                    new ArrayList<>()
                ));
            } else {
                // No plugin specified
                if (request.isRefreshSearch()) {
                    // Refresh existing search plugins
                    List<String> refreshedPlugins = refreshExistingSearchPlugins();
                    listener.onResponse(new LoadSearchPluginsResponse(
                        true,
                        "Refreshed " + refreshedPlugins.size() + " existing search plugin(s)",
                        refreshedPlugins
                    ));
                } else {
                    listener.onResponse(new LoadSearchPluginsResponse(
                        false,
                        "No plugin specified. Provide plugin_path or plugin_name parameter.",
                        new ArrayList<>()
                    ));
                }
            }
            
        } catch (Exception e) {
            logger.error("Unexpected error in search plugin loading", e);
            listener.onResponse(new LoadSearchPluginsResponse(
                false,
                "Unexpected error: " + e.getMessage(),
                new ArrayList<>()
            ));
        }
    }
    
    /**
     * Refresh existing search plugins (re-register their components)
     */
    private List<String> refreshExistingSearchPlugins() {
        try {
            return hotReloadService.getLoadedSearchPlugins();
        } catch (Exception e) {
            logger.warn("Failed to refresh existing search plugins", e);
            return new ArrayList<>();
        }
    }
}
