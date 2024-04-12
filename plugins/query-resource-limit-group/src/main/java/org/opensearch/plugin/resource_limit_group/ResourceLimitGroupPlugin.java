package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugin.resource_limit_group.rest.SampleRestAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

public class ResourceLimitGroupPlugin extends Plugin implements ActionPlugin {
    /**
     * Actions added by this plugin.
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionPlugin.ActionHandler<>(SampleAction.INSTANCE, SampleTransportAction.class));
    }

    /**
     * Rest handlers added by this plugin.
     *
     * @param settings
     * @param restController
     * @param clusterSettings
     * @param indexScopedSettings
     * @param settingsFilter
     * @param indexNameExpressionResolver
     * @param nodesInCluster
     */
    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
        return List.of(new SampleRestAction());
    }
}
