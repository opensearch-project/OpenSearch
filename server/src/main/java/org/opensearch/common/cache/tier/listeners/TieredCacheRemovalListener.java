/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier.listeners;

import org.opensearch.common.cache.tier.TieredCacheRemovalNotification;

/**
 * Listener for removing an element from tiered cache
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface TieredCacheRemovalListener<K, V> {
    void onRemoval(TieredCacheRemovalNotification<K, V> notification);
}
