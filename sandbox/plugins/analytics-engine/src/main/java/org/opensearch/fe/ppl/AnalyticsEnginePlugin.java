/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.AnalyticsBackEndPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.QueryEnginePlugin;
import org.opensearch.plugins.QueryPlanExecutor;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Java-based Distributed Query Executor implementation.
 * Evolves from the former query-engine module plugin. Implements the {@link QueryEnginePlugin}
 * interface as the first concrete DQE implementation.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Create the {@link PlanExecutor} as the {@link QueryPlanExecutor} implementation</li>
 *   <li>Expose via {@link #getExecutor()}
 * </ul>
 */
public class AnalyticsEnginePlugin extends Plugin implements QueryEnginePlugin, ActionPlugin {

    private PlanExecutor queryExecutorService;

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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Collection<AnalyticsBackEndPlugin> backEnds
    ) {
        this.queryExecutorService = new PlanExecutor();

        // TODO - do something with back-ends/engine implementations
        return List.of(queryExecutorService);
    }

    @Override
    public QueryPlanExecutor getExecutor() {
        return queryExecutorService;
    }
}
