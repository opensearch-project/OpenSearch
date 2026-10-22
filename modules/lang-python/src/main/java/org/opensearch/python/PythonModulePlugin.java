/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.python.action.PythonExecuteAction;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 *
 * Registers Python as a plugin.
 *
 * PythonModulePlugin is a plugin for OpenSearch that adds support for Python as a scripting language.
 * This class extends the {@link Plugin} class and implements the {@link ScriptPlugin} interface.
 *
 * <p>
 * This plugin allows users to write and execute scripts in Python within OpenSearch.
 * </p>
 *
 */
public class PythonModulePlugin extends Plugin implements ScriptPlugin, ActionPlugin {
    public PythonModulePlugin() {}

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new PythonScriptEngine();
    }

    /**
     * Actions added by this plugin.
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(PythonExecuteAction.INSTANCE, PythonExecuteAction.TransportAction.class));
        return actions;
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
        return List.of(new PythonExecuteAction.RestAction());
    }
}
