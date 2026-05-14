/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

/**
 * Plugin extension point for native runtime statistics.
 * <p>
 * Plugins or modules that embed a native runtime (e.g. the native-bridge module)
 * implement this interface so {@code Node.java} can discover them via
 * {@code filterPlugins(NativeStatsProvider.class)} and query native memory stats.
 * <p>
 * This replaces the former two-interface indirection ({@code NativeMemoryService} and
 * {@code NativeMemoryServiceProvider}), collapsing it into a single extension point.
 *
 * @opensearch.internal
 */
public interface NativeStatsProvider {

    /**
     * Returns current native memory stats, or {@code null} if unavailable.
     *
     * @return a snapshot of native memory statistics, or null
     */
    NativeMemoryStats memoryStats();
}
