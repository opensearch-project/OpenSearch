/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.indexfilter;

import org.opensearch.analytics.spi.IndexFilterProvider;
import org.opensearch.analytics.spi.IndexFilterProviderFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Internal bookkeeping for live {@link IndexFilterProvider}s and the
 * collectors they produce, keyed by small positive ints for cheap FFM
 * marshaling.
 *
 * <p>Entries are added dynamically when the native engine upcalls
 * {@code createProvider} / {@code createCollector}; they're removed when it
 * upcalls {@code releaseProvider} / {@code releaseCollector}. Callers do not
 * register providers directly — the registered {@link IndexFilterProviderFactory}
 * produces them on demand.
 */
public final class ProviderRegistry {

    private static volatile IndexFilterProviderFactory FACTORY;

    private static final ConcurrentHashMap<Integer, IndexFilterProvider> PROVIDERS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, CollectorHandle> COLLECTORS = new ConcurrentHashMap<>();
    private static final AtomicInteger PROVIDER_IDS = new AtomicInteger(1);
    private static final AtomicInteger COLLECTOR_IDS = new AtomicInteger(1);

    private ProviderRegistry() {}

    /** Set once at startup. Throws if called more than once. */
    public static void setFactory(IndexFilterProviderFactory factory) {
        if (FACTORY != null) {
            throw new IllegalStateException("IndexFilterProviderFactory already set");
        }
        FACTORY = factory;
    }

    /**
     * Package-private reset for tests that need to install a different factory.
     * Not part of the public contract — production code uses {@link #setFactory}
     * exactly once at plugin startup.
     */
    static void resetFactoryForTesting() {
        FACTORY = null;
    }

    static IndexFilterProviderFactory factory() {
        return FACTORY;
    }

    // ── Provider lifecycle ────────────────────────────────────────────

    static int registerProvider(IndexFilterProvider provider) {
        int key = PROVIDER_IDS.getAndIncrement();
        PROVIDERS.put(key, provider);
        return key;
    }

    static IndexFilterProvider provider(int key) {
        return PROVIDERS.get(key);
    }

    static IndexFilterProvider unregisterProvider(int key) {
        return PROVIDERS.remove(key);
    }

    // ── Collector lifecycle ───────────────────────────────────────────

    static int registerCollector(int providerKey, int innerCollectorKey) {
        int key = COLLECTOR_IDS.getAndIncrement();
        COLLECTORS.put(key, new CollectorHandle(providerKey, innerCollectorKey));
        return key;
    }

    static CollectorHandle collector(int key) {
        return COLLECTORS.get(key);
    }

    static void unregisterCollector(int key) {
        COLLECTORS.remove(key);
    }

    /** Maps an outer collector key back to the provider + the provider's own inner key. */
    record CollectorHandle(int providerKey, int innerCollectorKey) {}
}
