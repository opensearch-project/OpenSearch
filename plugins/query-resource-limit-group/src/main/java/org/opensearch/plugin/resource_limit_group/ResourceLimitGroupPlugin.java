/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.resource_limit_group.rest.RestCreateResourceLimitGroupAction;
import org.opensearch.plugin.resource_limit_group.rest.RestDeleteResourceLimitGroupAction;
import org.opensearch.plugin.resource_limit_group.rest.RestGetResourceLimitGroupAction;
import org.opensearch.plugin.resource_limit_group.rest.RestUpdateResourceLimitGroupAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin class for Resource Limit Group
 */
public class ResourceLimitGroupPlugin extends Plugin implements ActionPlugin {

    /**
     * Default constructor
     */
    public ResourceLimitGroupPlugin() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateResourceLimitGroupAction.INSTANCE, TransportCreateResourceLimitGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetResourceLimitGroupAction.INSTANCE, TransportGetResourceLimitGroupAction.class),
            new ActionPlugin.ActionHandler<>(UpdateResourceLimitGroupAction.INSTANCE, TransportUpdateResourceLimitGroupAction.class),
            new ActionPlugin.ActionHandler<>(DeleteResourceLimitGroupAction.INSTANCE, TransportDeleteResourceLimitGroupAction.class)
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
            new RestCreateResourceLimitGroupAction(),
            new RestGetResourceLimitGroupAction(),
            new RestUpdateResourceLimitGroupAction(),
            new RestDeleteResourceLimitGroupAction()
        );
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new ResourceLimitGroupPluginModule());
    }
}
