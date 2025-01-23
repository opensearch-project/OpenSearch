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
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
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
import org.opensearch.plugin.wlm.rule.action.CreateRuleAction;
import org.opensearch.plugin.wlm.rule.action.TransportCreateRuleAction;
import org.opensearch.plugin.wlm.rule.rest.RestCreateRuleAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.plugin.wlm.rule.service.RulePersistenceService.RULE_INDEX;

/**
 * Plugin class for WorkloadManagement
 */
public class WorkloadManagementPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

    /**
     * Default constructor
     */
    public WorkloadManagementPlugin() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateQueryGroupAction.INSTANCE, TransportCreateQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetQueryGroupAction.INSTANCE, TransportGetQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(DeleteQueryGroupAction.INSTANCE, TransportDeleteQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(UpdateQueryGroupAction.INSTANCE, TransportUpdateQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(CreateRuleAction.INSTANCE, TransportCreateRuleAction.class)
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        List<SystemIndexDescriptor> descriptors = List.of(
            new SystemIndexDescriptor(RULE_INDEX, "System index used for storing rules")
        );
        return descriptors;
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
            new RestCreateRuleAction()
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
