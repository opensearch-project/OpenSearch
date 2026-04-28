/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.opensearch.analytics.spi.IndexFilterProvider;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-segment collector bookkeeping, keyed by small positive ints for
 * cheap FFM marshaling.
 *
 * <p>This is the hot-path registry: {@code collectDocs} and
 * {@code releaseCollector} upcalls only touch this map. The provider
 * reference is captured at collector-creation time in
 * {@link CollectorHandle}, so no second map lookup is needed.
 */
public final class CollectorRegistry {

    private final ConcurrentHashMap<Integer, CollectorHandle> collectors = new ConcurrentHashMap<>();
    private final AtomicInteger nextKey = new AtomicInteger(1);

    /** Creates an empty collector registry. */
    public CollectorRegistry() {}

    int registerCollector(IndexFilterProvider provider, int innerCollectorKey) {
        int key = nextKey.getAndIncrement();
        collectors.put(key, new CollectorHandle(provider, innerCollectorKey));
        return key;
    }

    CollectorHandle collector(int key) {
        return collectors.get(key);
    }

    void unregisterCollector(int key) {
        collectors.remove(key);
    }

    /**
     * Maps an outer collector key to the provider instance + the
     * provider's own inner collector key.
     */
    record CollectorHandle(IndexFilterProvider provider, int innerCollectorKey) {}
}
