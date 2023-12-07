/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.cache.store.enums.CacheStoreType;

/**
 * Represents a store aware cache value.
 * @param <V> Type of value.
 */
public class StoreAwareCacheValue<V> {
    private final V value;
    private final CacheStoreType source;

    public StoreAwareCacheValue(V value, CacheStoreType source) {
        this.value = value;
        this.source = source;
    }

    public V getValue() {
        return value;
    }

    public CacheStoreType getSource() {
        return source;
    }
}
