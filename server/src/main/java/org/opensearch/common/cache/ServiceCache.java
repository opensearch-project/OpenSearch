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
    private final SingleObjectCache<S> cache;

    public ServiceCache(Supplier<S> supplier, TimeValue refreshInterval) {
        this.cache = new SingleObjectCache<S>(refreshInterval, supplier.get()) {
            @Override
            protected S refresh() {
                return supplier.get();
            }
        };
    }

    /**
     * Returns the cached value, refreshing it if the interval has expired.
     */
    public S getOrRefresh() {
        return cache.getOrRefresh();
    }
}
