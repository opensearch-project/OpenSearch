/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * Query execution plugin interface.
 * Node.java discovers implementations via {@code filterPlugins(QueryEnginePlugin.class)}.
 * The plugin receives back-end plugins in {@link #createComponents} and creates the {@link QueryPlanExecutor}. Node.java then passes the plugin
 * to front-end plugins so they can pull the executor and capabilities.
 *
 * <p>The plugin provides two accessors after {@code createComponents} completes:
 * <ul>
 *   <li>{@link #getExecutor()} — returns the server-level {@link QueryPlanExecutor}
 *       (Object types) for front-end plugins to consume</li>
 * </ul>
 *
 * @opensearch.internal
 */
public interface QueryEnginePlugin {

    /**
     * Creates plugin components. Receives the standard bootstrap parameters
     * plus discovered back-end plugins from Node.java.
     * Aggregates back-end capabilities and creates the {@link QueryPlanExecutor}.
     *
     * @param client                      A client to make requests to the system
     * @param clusterService              A service to allow watching and updating cluster state
     * @param threadPool                  A service to allow retrieving an executor to run an async action
     * @param resourceWatcherService      A service to watch for changes to node local files
     * @param scriptService               A service to allow running scripts on the local node
     * @param xContentRegistry            the registry for extensible xContent parsing
     * @param environment                 the environment for path and setting configurations
     * @param nodeEnvironment             the node environment
     * @param namedWriteableRegistry      the registry for NamedWriteable object parsing
     * @param indexNameExpressionResolver A service that resolves expression to index and alias names
     * @param repositoriesServiceSupplier A supplier for the repositories service
     * @param backEnds                    the discovered back-end plugins providing engine capabilities
     * @return a collection of components created by this plugin
     */
    default Collection<Object> createComponents(
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
        return Collections.emptyList();
    }

    /**
     * Returns the {@link QueryPlanExecutor} created during {@link #createComponents}.
     * Called by front-end plugins to get the server-level executor (Object types).
     *
     * @return the QueryPlanExecutor instance, or null if createComponents has not been called
     */
    QueryPlanExecutor getExecutor();
}
