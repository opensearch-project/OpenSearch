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
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.dsl.action.DslExecuteAction;
import org.opensearch.dsl.action.SearchActionFilter;
import org.opensearch.dsl.action.TransportDslExecuteAction;
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
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Plugin entry point. Registers {@link SearchActionFilter} to intercept _search requests
 * and {@link TransportDslExecuteAction} to handle DSL-to-Calcite conversion and execution.
 */
public class DslQueryExecutorPlugin extends Plugin implements ActionPlugin {

    private SearchActionFilter searchActionFilter;

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
        return Collections.emptyList();
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
