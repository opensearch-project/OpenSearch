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
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Unified SPI for back-end storage and query engines.
 * <p>
 * Each implementation provides reader lifecycle management (via
 * {@link #createReaderManager}) and declares supported data formats.
 * The type parameter {@code R} carries the reader type, eliminating
 * unsafe casts at the boundary.
 * <p>
 * Plugins that also support the analytics query path should additionally
 * implement {@code SearchExecEngineProvider} from the analytics framework.
 *
 * @param <R> the reader type produced by this backend's reader manager
 * @opensearch.internal
 */
// TODO: Rename to something more meaningful (e.g. EngineReaderPlugin, BackEndReaderPlugin)
// to avoid confusion with AnalyticsSearchBackendPlugin.
public interface SearchBackEndPlugin<R> {

    /** Unique backend name (e.g., "datafusion", "lucene"). */
    String name();

    // TODO: Revisit whether this should return List<DataFormat> or List<String>.
    /** Returns the names of data formats this backend can read and query. */
    List<String> getSupportedFormats();

    /**
     * Creates a reader manager for the given settings.
     *
     * @param settings the reader manager initialization settings
     * @return the reader manager
     * @throws IOException if reader creation fails
     */
    EngineReaderManager<?> createReaderManager(ReaderManagerConfig settings) throws IOException;

    // TODO: Evaluate restricting this method to only pass DataFormatRegistry and moving common
    // parameters (Client, ClusterService, Environment, etc.) to Plugin.createComponents.
    /**
     * Returns components added by this plugin.
     * Any components returned that implement {@link org.opensearch.common.lifecycle.LifecycleComponent}
     * will have their lifecycle managed. Components are bound in Guice alongside those from
     * {@link Plugin#createComponents}.
     *
     * @param client A client to make requests to the system
     * @param clusterService A service to allow watching and updating cluster state
     * @param threadPool A service to allow retrieving an executor to run an async action
     * @param resourceWatcherService A service to watch for changes to node local files
     * @param scriptService A service to allow running scripts on the local node
     * @param xContentRegistry the registry for extensible xContent parsing
     * @param environment the environment for path and setting configurations
     * @param nodeEnvironment the node environment used coordinate access to the data paths
     * @param namedWriteableRegistry the registry for {@link org.opensearch.core.common.io.stream.NamedWriteable} object parsing
     * @param indexNameExpressionResolver A service that resolves expression to index and alias names
     * @param repositoriesServiceSupplier A supplier for the service that manages snapshot repositories
     * @param dataFormatRegistry the data format registry for resolving format names to DataFormat objects
     * @return components to bind in Guice, or empty
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
        DataFormatRegistry dataFormatRegistry
    ) {
        return Collections.emptyList();
    }

    /**
     * Returns a supplier for native task cancellation stats, or {@code null} if not available.
     * <p>
     * The server calls this supplier on each {@code _nodes/stats} request to fetch
     * native task cancellation counters from the execution engine.
     *
     * @return a supplier of native task cancellation stats, or null
     */
    default @Nullable Supplier<AnalyticsBackendTaskCancellationStats> getAnalyticsBackendTaskCancellationStats() {
        return null;
    }

    /**
     * Returns a supplier for native memory stats, or {@code null} if not available.
     * <p>
     * The server calls this supplier on each {@code _nodes/stats} request to fetch
     * jemalloc memory metrics from the native layer.
     *
     * @return a supplier of native memory stats, or null
     */
    default @Nullable Supplier<AnalyticsBackendNativeMemoryStats> getAnalyticsBackendNativeMemoryStats() {
        return null;
    }
}
