/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.fe.ppl.action.RestUnifiedPPLAction;
import org.opensearch.fe.ppl.action.TransportUnifiedPPLAction;
import org.opensearch.fe.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.fe.ppl.action.UnifiedQueryService;
import org.opensearch.fe.ppl.planner.DefaultEngineExecutor;
import org.opensearch.fe.ppl.planner.EngineCapabilities;
import org.opensearch.fe.ppl.planner.EngineExecutor;
import org.opensearch.fe.ppl.planner.PushDownPlanner;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.AnalyticsFrontEndPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.QueryPlanExecutor;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * PPL/SQL query front-end plugin.
 */
public class PPLFrontEndPlugin extends Plugin implements AnalyticsFrontEndPlugin, ActionPlugin {

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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        QueryPlanExecutor executor
    ) {
        if (executor == null) {
            throw new IllegalStateException("executor must not be null");
        }

        // Internally wrap server-level QueryPlanExecutor in Calcite-typed DefaultEngineExecutor
        EngineExecutor engineExecutor = new DefaultEngineExecutor(executor);

        PushDownPlanner pushDownPlanner = new PushDownPlanner(EngineCapabilities.defaultCapabilities(), engineExecutor);
        UnifiedQueryService unifiedQueryService = new UnifiedQueryService(pushDownPlanner);
        return List.of(unifiedQueryService);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(UnifiedPPLExecuteAction.INSTANCE, TransportUnifiedPPLAction.class));
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
        return List.of(new RestUnifiedPPLAction());
    }
}
