/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.cache.RemovalListener;

import java.util.Collections;

public class DummyDiskCachingTier<K, V> implements CachingTier<K, V> {

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public void put(K key, V value) {}

    @Override
    public V computeIfAbsent(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return null;
    }

    @Override
    public void invalidate(K key) {}

    @Override
    public V compute(K key, TieredCacheLoader<K, V> loader) throws Exception {
        return null;
    }

    @Override
    public void setRemovalListener(RemovalListener<K, V> removalListener) {}

    @Override
    public void invalidateAll() {}

    @Override
    public Iterable<K> keys() {
        return Collections::emptyIterator;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public TierType getTierType() {
        return TierType.DISK;
    }
}
