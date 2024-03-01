/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.ICache;

import java.util.Map;

/**
 * Plugin to extend cache related classes
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CachePlugin {

    /**
     * Returns a map of cacheStoreType and a factory via which objects can be created on demand.
     * For example:
     * If there are two implementations of this plugin, lets say A and B, each may return below which can be
     * aggregated by fetching all plugins.
     *
     * A: Map.of(DISK, new ADiskCache.Factor(),
     *             ON_HEAP, new AOnHeapCache.Factor())
     *
     * B: Map.of(ON_HEAP, new ADiskCache.Factor())
     *
     * @return Map of cacheStoreType and an associated factory.
     */
    Map<String, ICache.Factory> getCacheFactoryMap();

    String getName();
}
