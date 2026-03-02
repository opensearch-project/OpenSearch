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
 * Front-end plugin interface for query languages.
 * Each front-end owns REST endpoints, parsing, planning, compilation, and execution
 * for a specific query language. Receives the {@link QueryEnginePlugin} in {@link #createComponents}
 * to pull the executor and capabilities.
 *
 * Follows the {@link TelemetryAwarePlugin} pattern: Node.java discovers plugins
 * implementing this interface via {@code filterPlugins(AnalyticsFrontEndPlugin.class)},
 * then calls {@link #createComponents} passing the DQE. The returned components
 * are added to the plugin components collection.
 *
 * @opensearch.internal
 */
public interface AnalyticsFrontEndPlugin {

    /**
     * Creates and returns front-end components that depend on a {@link QueryEnginePlugin}.
     * Called by Node.java during bootstrap after the DQE's createComponents has completed.
     * The front-end pulls {@code DQEExecutor} and {@code EngineCapabilities} from the DQE
     * and wires them into its internal pipeline.
     *
     * @param client A client to make requests to the system
     * @param clusterService A service to allow watching and updating cluster state
     * @param threadPool A service to allow retrieving an executor to run an async action
     * @param resourceWatcherService A service to watch for changes to node local files
     * @param scriptService A service to allow running scripts on the local node
     * @param xContentRegistry the registry for extensible xContent parsing
     * @param environment the environment for path and setting configurations
     * @param nodeEnvironment the node environment
     * @param namedWriteableRegistry the registry for NamedWriteable object parsing
     * @param indexNameExpressionResolver A service that resolves expression to index and alias names
     * @param repositoriesServiceSupplier A supplier for the repositories service
     * @param planExecutor the QueryPlanExecutor implementation
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
        QueryPlanExecutor planExecutor
    ) {
        return Collections.emptyList();
    }
}
