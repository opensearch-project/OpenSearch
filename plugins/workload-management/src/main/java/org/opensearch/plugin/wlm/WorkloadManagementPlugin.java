/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.action.ActionRequest;
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
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugin.wlm.querygroup.action.CreateQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.DeleteQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.GetQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.TransportCreateQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.TransportDeleteQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.TransportGetQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.TransportUpdateQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.UpdateQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.rest.RestCreateQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.rest.RestDeleteQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.rest.RestGetQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.rest.RestUpdateQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.service.QueryGroupPersistenceService;
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.plugin.wlm.rule.action.GetWlmRuleAction;
import org.opensearch.plugin.wlm.rule.action.TransportGetWlmRuleAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.rest.RestGetRuleAction;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Plugin class for WorkloadManagement
 */
public class WorkloadManagementPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {
    /**
     * The name of the index where rules are stored.
     */
    public static final String INDEX_NAME = ".wlm_rules";
    /**
     * The maximum number of rules allowed per GET request.
     */
    public static final int MAX_RULES_PER_PAGE = 50;

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
        return List.of(new IndexStoredRulePersistenceService(INDEX_NAME, client, QueryGroupFeatureType.INSTANCE, MAX_RULES_PER_PAGE));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateQueryGroupAction.INSTANCE, TransportCreateQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetQueryGroupAction.INSTANCE, TransportGetQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(DeleteQueryGroupAction.INSTANCE, TransportDeleteQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(UpdateQueryGroupAction.INSTANCE, TransportUpdateQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetWlmRuleAction.INSTANCE, TransportGetWlmRuleAction.class)
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(new SystemIndexDescriptor(INDEX_NAME, "System index used for storing rules"));
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
            new RestCreateQueryGroupAction(),
            new RestGetQueryGroupAction(),
            new RestDeleteQueryGroupAction(),
            new RestUpdateQueryGroupAction(),
            new RestGetRuleAction(
                "get_rule",
                List.of(new RestHandler.Route(GET, "_wlm/rule/"), new RestHandler.Route(GET, "_wlm/rule/{_id}")),
                QueryGroupFeatureType.INSTANCE,
                GetWlmRuleAction.INSTANCE
            )
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(QueryGroupPersistenceService.MAX_QUERY_GROUP_COUNT);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new WorkloadManagementPluginModule());
    }
}
