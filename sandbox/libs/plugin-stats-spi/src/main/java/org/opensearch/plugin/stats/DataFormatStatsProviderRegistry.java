/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * JVM-wide registry of {@link DataFormatStatsProvider} instances.
 *
 * <p>Each data-format plugin registers its provider here on construction. The registry
 * acts as a thin shared lookup so engines can self-register their per-shard trackers
 * with the right provider via {@link #get(String)}.
 *
 * <p>The SPI library is loaded by composite-engine's classloader and shared with
 * dependent plugins via {@code extendedPlugins}, so a single registry instance is
 * shared across all plugins in a node JVM.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DataFormatStatsProviderRegistry {

    public static final DataFormatStatsProviderRegistry INSTANCE = new DataFormatStatsProviderRegistry();

    private final ConcurrentMap<String, DataFormatStatsProvider<?>> providers = new ConcurrentHashMap<>();

    private DataFormatStatsProviderRegistry() {}

    /** Registers a provider. First writer wins for a given format name. */
    public void register(DataFormatStatsProvider<?> provider) {
        providers.putIfAbsent(provider.formatName(), provider);
    }

    /** Returns the provider for a format, or {@code null} if no plugin has registered it. */
    public DataFormatStatsProvider<?> get(String formatName) {
        return providers.get(formatName);
    }

    /** Returns an unmodifiable snapshot of all registered providers. */
    public Collection<DataFormatStatsProvider<?>> all() {
        return Collections.unmodifiableCollection(providers.values());
    }
}
