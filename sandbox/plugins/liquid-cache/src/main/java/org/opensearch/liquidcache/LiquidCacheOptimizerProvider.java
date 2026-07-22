/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.liquidcache;

import org.opensearch.analytics.spi.NativeQueryOptimizerProvider;
import org.opensearch.common.settings.Settings;

/**
 * {@link NativeQueryOptimizerProvider} implementation. Discovered by the
 * DataFusion back-end; delegates to the injected {@link LiquidCachePlugin}.
 *
 * @opensearch.internal
 */
public class LiquidCacheOptimizerProvider implements NativeQueryOptimizerProvider {

    private final LiquidCachePlugin plugin;

    public LiquidCacheOptimizerProvider(LiquidCachePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return "liquid_cache";
    }

    @Override
    public long createNativeOptimizer(Settings settings) {
        return plugin.createNativeOptimizer(settings);
    }
}
