/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;

import java.util.function.Supplier;

/**
 * Generic cache wrapper backed by {@link SingleObjectCache} that refreshes
 * its value from a {@link Supplier} when the configured interval expires.
 *
 * @param <S> the type of the cached value
 * @opensearch.internal
 */
public class ServiceCache<S> {
    private final Supplier<S> supplier;
    private final TimeValue refreshInterval;
    private volatile SingleObjectCache<S> cache;

    /**
     * Creates a new ServiceCache. The supplier is NOT called eagerly — the first
     * call to {@link #getOrRefresh()} triggers the initial load. This avoids
     * startup failures if the supplier (e.g., JNI native bridge) is not yet ready
     * when the cache is constructed.
     */
    public ServiceCache(Supplier<S> supplier, TimeValue refreshInterval) {
        this.supplier = supplier;
        this.refreshInterval = refreshInterval;
    }

    /**
     * Returns the cached value, refreshing it if the interval has expired.
     * On the first call, initializes the cache from the supplier.
     *
     * @return the cached value, or null if the supplier returns null
     */
    public S getOrRefresh() {
        if (cache == null) {
            synchronized (this) {
                if (cache == null) {
                    S initial = supplier.get();
                    if (initial == null) {
                        return null;
                    }
                    cache = new SingleObjectCache<S>(refreshInterval, initial) {
                        @Override
                        protected S refresh() {
                            return supplier.get();
                        }
                    };
                }
            }
        }
        return cache.getOrRefresh();
    }
}
