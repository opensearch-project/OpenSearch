/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.painless;

import org.opensearch.action.ActionRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
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
import org.opensearch.painless.action.PainlessContextAction;
import org.opensearch.painless.action.PainlessExecuteAction;
import org.opensearch.painless.spi.PainlessExtension;
import org.opensearch.painless.spi.Whitelist;
import org.opensearch.painless.spi.WhitelistLoader;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.DerivedFieldScript;
import org.opensearch.script.IngestScript;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.pipeline.MovingFunctionScript;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Registers Painless as a plugin.
 */
public final class PainlessPlugin extends Plugin implements ScriptPlugin, ExtensiblePlugin, ActionPlugin {

    private static final Map<ScriptContext<?>, List<Whitelist>> allowlists;

    /*
     * Contexts from Core that need custom allowlists can add them to the map below.
     * Allowlist resources should be added as appropriately named, separate files
     * under Painless' resources
     */
    static {
        Map<ScriptContext<?>, List<Whitelist>> map = new HashMap<>();

        // Moving Function Pipeline Agg
        List<Whitelist> movFn = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        movFn.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.opensearch.aggs.movfn.txt"));
        map.put(MovingFunctionScript.CONTEXT, movFn);

        // Functions used for scoring docs
        List<Whitelist> scoreFn = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        scoreFn.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.opensearch.score.txt"));
        map.put(ScoreScript.CONTEXT, scoreFn);

        // Functions available to ingest pipelines
        List<Whitelist> ingest = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        ingest.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.opensearch.ingest.txt"));
        map.put(IngestScript.CONTEXT, ingest);

        // Functions available to derived fields
        List<Whitelist> derived = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        derived.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.opensearch.derived.txt"));
        map.put(DerivedFieldScript.CONTEXT, derived);

        allowlists = map;
    }

    private final SetOnce<PainlessScriptEngine> painlessScriptEngine = new SetOnce<>();

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        Map<ScriptContext<?>, List<Whitelist>> contextsWithAllowlists = new HashMap<>();
        for (ScriptContext<?> context : contexts) {
            // we might have a context that only uses the base allowlists, so would not have been filled in by reloadSPI
            List<Whitelist> contextAllowlists = allowlists.get(context);
            if (contextAllowlists == null) {
                contextAllowlists = new ArrayList<>(Whitelist.BASE_WHITELISTS);
            }
            contextsWithAllowlists.put(context, contextAllowlists);
        }
        painlessScriptEngine.set(new PainlessScriptEngine(settings, contextsWithAllowlists));
        return painlessScriptEngine.get();
    }

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
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // this is a hack to bind the painless script engine in guice (all components are added to guice), so that
        // the painless context api. this is a temporary measure until transport actions do no require guice
        return Collections.singletonList(painlessScriptEngine.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(CompilerSettings.REGEX_ENABLED, CompilerSettings.REGEX_LIMIT_FACTOR);
    }

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        loader.loadExtensions(PainlessExtension.class)
            .stream()
            .flatMap(extension -> extension.getContextWhitelists().entrySet().stream())
            .forEach(entry -> {
                List<Whitelist> existing = allowlists.computeIfAbsent(entry.getKey(), c -> new ArrayList<>(Whitelist.BASE_WHITELISTS));
                existing.addAll(entry.getValue());
            });
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Collections.singletonList(PainlessExecuteAction.PainlessTestScript.CONTEXT);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(PainlessExecuteAction.INSTANCE, PainlessExecuteAction.TransportAction.class));
        actions.add(new ActionHandler<>(PainlessContextAction.INSTANCE, PainlessContextAction.TransportAction.class));
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
        List<RestHandler> handlers = new ArrayList<>();
        handlers.add(new PainlessExecuteAction.RestAction());
        handlers.add(new PainlessContextAction.RestAction());
        return handlers;
    }
}
