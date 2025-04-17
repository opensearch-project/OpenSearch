/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Module;
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
import org.opensearch.plugin.wlm.action.CreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.GetWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportCreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportDeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportGetWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.TransportUpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.action.UpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestCreateWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestDeleteWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestGetWorkloadGroupAction;
import org.opensearch.plugin.wlm.rest.RestUpdateWorkloadGroupAction;
import org.opensearch.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.InMemoryRuleProcessingService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin class for WorkloadManagement
 */
public class WorkloadManagementPlugin extends Plugin implements ActionPlugin {

    private AutoTaggingActionFilter autoTaggingActionFilter;

    /**
     * Default constructor
     */
    public WorkloadManagementPlugin() {}

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
        InMemoryRuleProcessingService ruleProcessingService = new InMemoryRuleProcessingService(null, null); // TODO: change this post
                                                                                                             // Ruirui's get API PR
        autoTaggingActionFilter = new AutoTaggingActionFilter(ruleProcessingService, threadPool);
        return Collections.emptyList();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(autoTaggingActionFilter);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateWorkloadGroupAction.INSTANCE, TransportCreateWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetWorkloadGroupAction.INSTANCE, TransportGetWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(DeleteWorkloadGroupAction.INSTANCE, TransportDeleteWorkloadGroupAction.class),
            new ActionPlugin.ActionHandler<>(UpdateWorkloadGroupAction.INSTANCE, TransportUpdateWorkloadGroupAction.class)
        );
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
        return List.of(
            new RestCreateWorkloadGroupAction(),
            new RestGetWorkloadGroupAction(),
            new RestDeleteWorkloadGroupAction(),
            new RestUpdateWorkloadGroupAction()
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(WorkloadGroupPersistenceService.MAX_QUERY_GROUP_COUNT);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new WorkloadManagementPluginModule());
    }
}
