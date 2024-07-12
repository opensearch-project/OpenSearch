/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights;

import org.opensearch.action.ActionRequest;
import org.opensearch.client.node.PluginAwareNodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugin.insights.core.listener.QueryInsightsListener;
import org.opensearch.plugin.insights.core.service.QueryInsightsService;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.resthandler.top_queries.RestTopQueriesAction;
import org.opensearch.plugin.insights.rules.transport.top_queries.TransportTopQueriesAction;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin class for Query Insights.
 */
public class QueryInsightsPlugin extends Plugin implements ActionPlugin {
    /**
     * Default constructor
     */
    public QueryInsightsPlugin() {}

    @Override
    public Collection<Object> createComponents(
        final PluginAwareNodeClient client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // create top n queries service
        final QueryInsightsService queryInsightsService = new QueryInsightsService(clusterService.getClusterSettings(), threadPool, client);
        return List.of(queryInsightsService, new QueryInsightsListener(clusterService, queryInsightsService));
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(final Settings settings) {
        return List.of(
            new ScalingExecutorBuilder(
                QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR,
                1,
                Math.min((OpenSearchExecutors.allocatedProcessors(settings) + 1) / 2, QueryInsightsSettings.MAX_THREAD_COUNT),
                TimeValue.timeValueMinutes(5)
            )
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
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
            QueryInsightsSettings.TOP_N_LATENCY_QUERIES_WINDOW_SIZE,
            QueryInsightsSettings.TOP_N_LATENCY_EXPORTER_SETTINGS,
            QueryInsightsSettings.TOP_N_CPU_QUERIES_ENABLED,
            QueryInsightsSettings.TOP_N_CPU_QUERIES_SIZE,
            QueryInsightsSettings.TOP_N_CPU_QUERIES_WINDOW_SIZE,
            QueryInsightsSettings.TOP_N_CPU_EXPORTER_SETTINGS,
            QueryInsightsSettings.TOP_N_MEMORY_QUERIES_ENABLED,
            QueryInsightsSettings.TOP_N_MEMORY_QUERIES_SIZE,
            QueryInsightsSettings.TOP_N_MEMORY_QUERIES_WINDOW_SIZE,
            QueryInsightsSettings.TOP_N_MEMORY_EXPORTER_SETTINGS
        );
    }
}
