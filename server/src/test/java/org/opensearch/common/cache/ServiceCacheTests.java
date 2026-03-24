/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.unit.TimeValue;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Property-based tests for ServiceCache using jqwik.
 * Tests the caching invariant: supplier called once within TTL,
 * called again after TTL expires.
 *
 * Feature: metrics-arch-migration, Property 8: ServiceCache caching invariant
 */
public class ServiceCacheTests {

    /**
     * Property 8: ServiceCache caching invariant
     *
     * For any Supplier and refresh interval, calling ServiceCache.getOrRefresh()
     * twice within the refresh interval returns the same object instance (supplier
     * not called again), and calling it after the interval expires invokes the
     * supplier again to produce a fresh value.
     *
     * The underlying SingleObjectCache initializes lastRefreshTimestamp to 0, so
     * the first getOrRefresh() call always triggers a refresh. After that first
     * call establishes the refresh timestamp, subsequent calls within TTL return
     * the cached value without invoking the supplier.
     *
     * Validates: Requirements 5.3, 5.4
     */
    @Property(tries = 100)
    void cachingInvariant(@ForAll @IntRange(min = 100, max = 300) int ttlMs) throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);

        // Supplier that returns a new unique String each time and tracks invocations
        ServiceCache<String> cache = new ServiceCache<>(
            () -> "value-" + callCount.incrementAndGet(),
            TimeValue.timeValueMillis(ttlMs)
        );

        // Constructor calls supplier once eagerly for the initial value
        assertEquals(1, callCount.get(), "Constructor should call supplier once for initial value");

        // First getOrRefresh() — triggers a refresh because lastRefreshTimestamp starts at 0
        // This establishes the refresh timestamp for subsequent TTL-based caching
        String first = cache.getOrRefresh();
        int callsAfterFirst = callCount.get();
        // The first call refreshes (supplier called again), so callCount is now 2
        assertEquals(2, callsAfterFirst, "First getOrRefresh triggers refresh since cache timestamp starts at 0");
        assertEquals("value-2", first, "First getOrRefresh should return the refreshed value");

        // Second call within TTL — should return same cached object, no new supplier call
        String second = cache.getOrRefresh();
        assertSame(first, second, "Second call within TTL should return same object instance");
        assertEquals(callsAfterFirst, callCount.get(), "Supplier should not be called again within TTL");

        // Sleep past TTL to expire the cache
        Thread.sleep(ttlMs + 50);

        // Call after TTL expires — supplier should be called again
        String third = cache.getOrRefresh();
        assertEquals(callsAfterFirst + 1, callCount.get(), "Supplier should be called again after TTL expires");
        assertEquals("value-3", third, "Value after TTL expiry should be from the next supplier call");
    }
}
