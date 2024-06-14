/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.qg;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.qg.rest.RestCreateQueryGroupAction;
import org.opensearch.plugin.qg.rest.RestDeleteQueryGroupAction;
import org.opensearch.plugin.qg.rest.RestGetQueryGroupAction;
import org.opensearch.plugin.qg.rest.RestUpdateQueryGroupAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin class for QueryGroup
 */
public class QueryGroupPlugin extends Plugin implements ActionPlugin {

    /**
     * Default constructor
     */
    public QueryGroupPlugin() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionPlugin.ActionHandler<>(CreateQueryGroupAction.INSTANCE, TransportCreateQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(GetQueryGroupAction.INSTANCE, TransportGetQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(UpdateQueryGroupAction.INSTANCE, TransportUpdateQueryGroupAction.class),
            new ActionPlugin.ActionHandler<>(DeleteQueryGroupAction.INSTANCE, TransportDeleteQueryGroupAction.class)
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
            new RestCreateQueryGroupAction(),
            new RestGetQueryGroupAction(),
            new RestUpdateQueryGroupAction(),
            new RestDeleteQueryGroupAction()
        );
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new QueryGroupPluginModule());
    }
}
