/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.plugins.PluginsService;

import java.util.function.Supplier;

import static org.opensearch.node.Node.NATIVE_MEMORY_REFRESH_INTERVAL_SETTING;

/**
 * Builds a cached {@link Supplier} of {@link NativeMemoryStats} from discovered
 * {@link NativeStatsProvider} plugins.
 *
 * @opensearch.internal
 */
public final class NativeMemoryStatsFactory {

    private NativeMemoryStatsFactory() {}

    /**
     * Discovers a {@link NativeStatsProvider} plugin and returns a cached supplier
     * that refreshes at the configured interval. Returns a null-returning supplier
     * if no provider is available.
     */
    public static Supplier<NativeMemoryStats> create(PluginsService pluginsService, Settings settings) {
        final NativeStatsProvider nativeStatsProvider = pluginsService.filterPlugins(NativeStatsProvider.class)
            .stream()
            .findFirst()
            .orElse(null);

        if (nativeStatsProvider == null) {
            return () -> null;
        }

        TimeValue refreshInterval = NATIVE_MEMORY_REFRESH_INTERVAL_SETTING.get(settings);
        NativeMemoryStats initialStats = nativeStatsProvider.memoryStats();
        if (initialStats == null) {
            initialStats = new NativeMemoryStats(-1, -1);
        }
        SingleObjectCache<NativeMemoryStats> cache = new SingleObjectCache<NativeMemoryStats>(refreshInterval, initialStats) {
            @Override
            protected NativeMemoryStats refresh() {
                NativeMemoryStats stats = nativeStatsProvider.memoryStats();
                return stats != null ? stats : new NativeMemoryStats(-1, -1);
            }
        };
        return cache::getOrRefresh;
    }
}
