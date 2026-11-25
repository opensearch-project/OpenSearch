/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.Nullable;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugin.stats.AnalyticsBackendTaskCancellationStats;

import java.io.IOException;
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
