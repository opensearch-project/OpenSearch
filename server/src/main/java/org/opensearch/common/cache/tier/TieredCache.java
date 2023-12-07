/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.store.Cache;
import org.opensearch.common.cache.store.enums.CacheStoreType;

/**
 * This represents a cache comprising of multiple tiers/layers.
 * @param <K> Type of key
 * @param <V> Type of value
 */
public interface TieredCache<K, V> extends Cache<K, V> {
    Iterable<K> cacheKeys(CacheStoreType type);

    void refresh(CacheStoreType type);
}
