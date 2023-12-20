/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.action.ActionRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugin.insights.core.listener.SearchQueryLatencyListener;
import org.opensearch.plugin.insights.core.service.TopQueriesByLatencyService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.resthandler.top_queries.RestTopQueriesAction;
import org.opensearch.plugin.insights.rules.transport.top_queries.TransportTopQueriesAction;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin class for Query Insights.
 */
public class QueryInsightsPlugin extends Plugin implements ActionPlugin, SearchPlugin {
    /**
     * Default constructor
     */
    public QueryInsightsPlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // create top n queries service
        TopQueriesByLatencyService topQueriesByLatencyService = new TopQueriesByLatencyService(threadPool, clusterService, client);
        // top n queries listener
        SearchQueryLatencyListener searchQueryLatencyListener = new SearchQueryLatencyListener(clusterService, topQueriesByLatencyService);
        return List.of(topQueriesByLatencyService, searchQueryLatencyListener);
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestTopQueriesAction());
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionPlugin.ActionHandler<>(TopQueriesAction.INSTANCE, TransportTopQueriesAction.class));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            // Settings for top N queries
            QueryInsightsSettings.TOP_N_LATENCY_QUERIES_ENABLED,
            QueryInsightsSettings.TOP_N_LATENCY_QUERIES_SIZE,
            QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE
        );
    }
}
