/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.Nullable;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;

import java.util.function.Supplier;

/**
 * SPI for plugins that own a native allocator and contribute its stats to
 * {@code _nodes/stats}. Implemented by the plugin that constructs the node-level
 * Arrow allocator (today: {@code ArrowBasePlugin}).
 *
 * <p>Mirrors the supplier-based contribution pattern used by
 * {@link SearchBackEndPlugin#getAnalyticsBackendNativeMemoryStats}: the plugin
 * exposes a {@link Supplier} that the server invokes on each {@code _nodes/stats}
 * call. The supplier closes over the live allocator and produces a fresh
 * {@link NativeAllocatorPoolStats} snapshot.
 *
 * @opensearch.api
 */
public interface ArrowAllocatorPlugin {

    /**
     * Returns a supplier that produces a fresh {@link NativeAllocatorPoolStats}
     * snapshot of the plugin's allocator on each invocation, or {@code null} if
     * no allocator stats should be contributed.
     */
    @Nullable
    default Supplier<NativeAllocatorPoolStats> getNativeAllocatorStatsSupplier() {
        return null;
    }
}
