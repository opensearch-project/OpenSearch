/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

import org.opensearch.common.cache.RemovalNotification;

/**
 * This can be used to listen to tiered caching events
 * @param <K> Type of key
 * @param <V> Type of value
 */
public interface TieredCacheEventListener<K, V> {

    void onMiss(K key, TierType tierType);

    void onRemoval(RemovalNotification<K, V> notification);

    void onHit(K key, V value, TierType tierType);

    void onCached(K key, V value, TierType tierType);
}
