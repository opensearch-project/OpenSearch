/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.store.config.StoreAwareCacheConfig;
import org.opensearch.common.cache.store.enums.CacheStoreType;

/**
 * Represents a cache with a specific type of store like onHeap, disk etc.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StoreAwareCache<K, V> extends ICache<K, V> {
    CacheStoreType getTierType();

    /**
     * Provides a way to create a new cache.
     */
    interface Factory {
        <K, V> StoreAwareCache<K, V> create(StoreAwareCacheConfig<K, V> storeAwareCacheConfig);

        String getCacheName();
    }
}
