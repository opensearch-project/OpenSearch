/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.dsl.action.DslExecuteAction;
import org.opensearch.dsl.action.SearchActionFilter;
import org.opensearch.dsl.action.TransportDslExecuteAction;
import org.opensearch.dsl.settings.DslGateInputs;
import org.opensearch.dsl.settings.DslQuerySettings;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin entry point. Registers {@link SearchActionFilter} to intercept _search requests,
 * {@link TransportDslExecuteAction} to handle DSL-to-Calcite conversion and execution, the
 * {@link DslQuerySettings} operator knobs, and {@link DslGateInputs}, the reader for the
 * cross-plugin inputs the sub-plan fan-out decision needs.
 */
public class DslQueryExecutorPlugin extends Plugin implements ActionPlugin {

    private SearchActionFilter searchActionFilter;
    // Held as well as returned: the holder is a component (injected into TransportDslExecuteAction)
    // and the field keeps it reachable for a later direct reader, the same way getActionFilters()
    // below reads searchActionFilter.
    private DslQuerySettings dslQuerySettings;

    /** Creates a new plugin instance. */
    public DslQueryExecutorPlugin() {}

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
        this.searchActionFilter = new SearchActionFilter((NodeClient) client);
        // Fan-out additions: the settings holder carries dsl.query.max_parallel_sub_plans, and
        // DslGateInputs is the only production construction site of the cross-plugin gate-input reader.
        this.dslQuerySettings = new DslQuerySettings(clusterService);
        return List.of(dslQuerySettings, new DslGateInputs(clusterService.getClusterSettings()));
    }

    @Override
    public List<Setting<?>> getSettings() {
        // The fan-out width knob has to be registered here or it is unsettable in opensearch.yml, a 400
        // on _cluster/settings, and unresolvable by key from a sibling plugin's classloader — which is how
        // DslGateInputs reads the analytics plugins' gate inputs across that boundary.
        return DslQuerySettings.all();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(DslExecuteAction.INSTANCE, TransportDslExecuteAction.class));
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return searchActionFilter != null ? List.of(searchActionFilter) : List.of();
    }
}
