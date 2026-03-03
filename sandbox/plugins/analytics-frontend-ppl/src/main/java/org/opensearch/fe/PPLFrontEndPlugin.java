/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe;

import org.opensearch.action.ActionRequest;
import org.opensearch.analytics.spi.AnalyticsFrontEndPlugin;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.fe.action.RestUnifiedPPLAction;
import org.opensearch.fe.action.TransportUnifiedPPLAction;
import org.opensearch.fe.action.UnifiedPPLExecuteAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.util.List;
import java.util.function.Supplier;

/**
 * PPL/SQL query front-end plugin.
 *
 * <p>Implements {@link AnalyticsFrontEndPlugin} for SPI-based
 * discovery by the query engine. The {@code QueryPlanExecutor} and {@code SchemaProvider}
 * are received by {@link TransportUnifiedPPLAction} via Guice injection.
 */
public class PPLFrontEndPlugin extends Plugin implements AnalyticsFrontEndPlugin, ActionPlugin {

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
