/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.util.concurrent.ContextSwitcher;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * Plugin that provides the telemetry registries to build component with telemetry and also provide a way to
 * pass telemetry registries to the implementing plugins for adding instrumentation in the code.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface TelemetryAwarePlugin {

    /**
     * Returns components added by this plugin.
     * <p>
     * Any components returned that implement {@link LifecycleComponent} will have their lifecycle managed.
     * Note: To aid in the migration away from guice, all objects returned as components will be bound in guice
     * to themselves.
     *
     * @param client A client to make requests to the system
     * @param clusterService A service to allow watching and updating cluster state
     * @param threadPool A service to allow retrieving an executor to run an async action
     * @param resourceWatcherService A service to watch for changes to node local files
     * @param scriptService A service to allow running scripts on the local node
     * @param xContentRegistry the registry for extensible xContent parsing
     * @param environment the environment for path and setting configurations
     * @param nodeEnvironment the node environment used coordinate access to the data paths
     * @param namedWriteableRegistry the registry for {@link NamedWriteable} object parsing
     * @param indexNameExpressionResolver A service that resolves expression to index and alias names
     * @param repositoriesServiceSupplier A supplier for the service that manages snapshot repositories; will return null when this method
     *                                   is called, but will return the repositories service once the node is initialized.
     * @param tracer the tracer to add tracing instrumentation.
     * @param metricsRegistry the registry for metrics instrumentation.
     * @param contextSwitcher A client for switching to plugin system context
     */
    default Collection<Object> createComponents(
        NodeClient client,
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
        Tracer tracer,
        MetricsRegistry metricsRegistry,
        ContextSwitcher contextSwitcher
    ) {
        return Collections.emptyList();
    }
}
