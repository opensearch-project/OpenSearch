/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier;

/**
 * Used to load value in tiered cache if not present.
 * @param <K> Type of key
 * @param <V> Type of value
 */
public interface TieredCacheLoader<K, V> {
    V load(K key) throws Exception;

    boolean isLoaded();
}
