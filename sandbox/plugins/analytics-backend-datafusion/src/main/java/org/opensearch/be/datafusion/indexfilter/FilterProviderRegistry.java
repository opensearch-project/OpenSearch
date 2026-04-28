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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Query-level provider lifecycle: deserialize query bytes into an
 * {@link IndexFilterProvider}, track it by int key, close it when done.
 *
 * <p>Called once per query (cold path). Separated from the per-segment
 * collector registry so the hot path ({@code collectDocs}) never touches
 * the provider map.
 */
public final class FilterProviderRegistry {

    private final AtomicReference<IndexFilterProviderFactory> factory = new AtomicReference<>();
    private final ConcurrentHashMap<Integer, IndexFilterProvider> providers = new ConcurrentHashMap<>();
    private final AtomicInteger nextKey = new AtomicInteger(1);
    private final CollectorRegistry collectors;

    /**
     * Creates a provider registry wired to the given collector registry.
     * {@code createCollector} delegates to the provider then registers
     * the result in {@code collectors}.
     */
    public FilterProviderRegistry(CollectorRegistry collectors) {
        this.collectors = collectors;
    }

    /**
     * Set the factory that deserializes query bytes into providers.
     * Safe to call once; throws on double-set.
     */
    public void setFactory(IndexFilterProviderFactory f) {
        if (f == null) {
            throw new IllegalArgumentException("factory must not be null");
        }
        if (factory.compareAndSet(null, f) == false) {
            throw new IllegalStateException("IndexFilterProviderFactory already set");
        }
    }

    IndexFilterProviderFactory factory() {
        return factory.get();
    }

    /**
     * Create a provider from the factory and register it.
     *
     * @return provider key {@code >= 1}, or {@code -1} on failure
     */
    int createProvider(byte[] queryBytes) {
        IndexFilterProviderFactory f = factory.get();
        if (f == null) {
            return -1;
        }
        IndexFilterProvider provider = f.create(queryBytes);
        if (provider == null) {
            return -1;
        }
        int key = nextKey.getAndIncrement();
        providers.put(key, provider);
        return key;
    }

    /**
     * Look up a registered provider by key.
     */
    IndexFilterProvider provider(int key) {
        return providers.get(key);
    }

    /**
     * Unregister and close a provider. Returns silently if key is unknown.
     */
    void releaseProvider(int key) throws IOException {
        IndexFilterProvider provider = providers.remove(key);
        if (provider != null) {
            provider.close();
        }
    }

    /**
     * Look up the provider for {@code providerKey}, ask it to create a
     * collector for the given segment range, and register the result in
     * the {@link CollectorRegistry}.
     *
     * @return outer collector key {@code >= 1}, or {@code -1} on failure
     */
    int createCollector(int providerKey, int segmentOrd, int minDoc, int maxDoc) {
        IndexFilterProvider provider = providers.get(providerKey);
        if (provider == null) {
            return -1;
        }
        int inner = provider.createCollector(segmentOrd, minDoc, maxDoc);
        if (inner < 0) {
            return -1;
        }
        return collectors.registerCollector(provider, inner);
    }
}
