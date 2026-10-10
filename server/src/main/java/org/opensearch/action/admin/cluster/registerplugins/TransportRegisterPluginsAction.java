/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.registerplugins;

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
import org.opensearch.plugins.PluginsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport action for registering plugins
 *
 * @opensearch.internal
 */
public class TransportRegisterPluginsAction extends TransportClusterManagerNodeAction<RegisterPluginsRequest, RegisterPluginsResponse> {

    private final PluginsService pluginsService;
    private final AnalysisRegistry analysisRegistry;

    @Inject
    public TransportRegisterPluginsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        PluginsService pluginsService,
        AnalysisRegistry analysisRegistry
    ) {
        super(
            RegisterPluginsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RegisterPluginsRequest::new,
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
    protected RegisterPluginsResponse read(StreamInput in) throws IOException {
        return new RegisterPluginsResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(RegisterPluginsRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        RegisterPluginsRequest request,
        ClusterState state,
        ActionListener<RegisterPluginsResponse> listener
    ) throws Exception {
        try {
            List<String> registeredComponents = new ArrayList<>();
            
            if (request.isRegisterAnalysis()) {
                // Register analysis registry with all loaded analysis plugins
                List<AnalysisPlugin> analysisPlugins = pluginsService.filterPlugins(AnalysisPlugin.class);
                for (AnalysisPlugin plugin : analysisPlugins) {
                    analysisRegistry.addAnalysisPlugin(plugin);
                    String pluginClassName = plugin.getClass().getSimpleName();
                    registeredComponents.add("Analysis: " + pluginClassName);
                    
                    // Mark plugin as registered via the register API
                    // We need to find the actual plugin name from PluginInfo, not just the class name
                    String pluginName = findPluginNameByClass(plugin.getClass().getName());
                    if (pluginName != null) {
                        pluginsService.markPluginAsActive(pluginName);
                    } else {
                        // Fallback to class name if we can't find the plugin info name
                        pluginsService.markPluginAsActive(pluginClassName);
                    }
                }
            }
            
            listener.onResponse(new RegisterPluginsResponse(true, "Analysis registry registered successfully", registeredComponents));
        } catch (Exception e) {
            listener.onResponse(new RegisterPluginsResponse(false, "Failed to register plugins: " + e.getMessage(), new ArrayList<>()));
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
