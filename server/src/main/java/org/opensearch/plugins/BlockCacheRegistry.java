/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;

/**
 * Lookup interface for registered {@link BlockCache} instances by name.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BlockCacheRegistry {

    /**
     * Returns the registered {@link BlockCache} whose {@link BlockCache#cacheName()} equals
     * {@code name}, or {@link Optional#empty()} if no such cache is registered.
     *
     * @param name the cache name; see constants in {@link BuiltInBlockCaches}
     * @return the matching cache, or empty
     */
    Optional<BlockCache> get(String name);
}
