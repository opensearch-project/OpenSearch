/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.plugin.stats.NativeMemoryStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Property-based tests for {@link SingleObjectCache} TTL consistency with NativeMemoryStats.
 * <p>
 * <b>Feature: native-stats-refactor, Property 1: Cache returns consistent value within TTL</b>
 * <p>
 * For any NativeStatsProvider and any TTL duration, all calls to SingleObjectCache.getOrRefresh()
 * within the TTL window SHALL return the same NativeMemoryStats instance (reference equality),
 * and a call after TTL expiry SHALL trigger a refresh.
 * <p>
 * <b>Validates: Requirements 7.2, 7.3, 7.4</b>
 */
public class NativeMemoryCachePropertyTests extends OpenSearchTestCase {

    /**
     * Property 1: Cache returns consistent value within TTL.
     * <p>
     * Within the TTL window, all calls to getOrRefresh() return the same reference.
     * <p>
     * <b>Validates: Requirements 7.2, 7.3, 7.4</b>
     */
    public void testCacheReturnsConsistentValueWithinTTL() {
        for (int iteration = 0; iteration < 100; iteration++) {
            // Generate a random TTL between 1 second and 60 seconds
            long ttlMillis = randomLongBetween(1000, 60000);
            TimeValue ttl = TimeValue.timeValueMillis(ttlMillis);

            // Generate random initial stats
            long allocatedBytes = randomLongBetween(0, Long.MAX_VALUE);
            long residentBytes = randomLongBetween(0, Long.MAX_VALUE);
            NativeMemoryStats initialStats = new NativeMemoryStats(allocatedBytes, residentBytes);

            AtomicInteger refreshCount = new AtomicInteger(0);

            SingleObjectCache<NativeMemoryStats> cache = new SingleObjectCache<NativeMemoryStats>(ttl, initialStats) {
                @Override
                protected NativeMemoryStats refresh() {
                    refreshCount.incrementAndGet();
                    return new NativeMemoryStats(randomLongBetween(0, Long.MAX_VALUE), randomLongBetween(0, Long.MAX_VALUE));
                }
            };

            // Force the cache to think it was just refreshed by calling getOrRefresh once
            // (the initial lastRefreshTimestamp is 0, so first call always refreshes)
            NativeMemoryStats firstResult = cache.getOrRefresh();
            int refreshesAfterInit = refreshCount.get();

            // Multiple calls within TTL should return the same reference
            int callCount = randomIntBetween(2, 10);
            for (int i = 0; i < callCount; i++) {
                NativeMemoryStats result = cache.getOrRefresh();
                assertSame(
                    "Within TTL, getOrRefresh() should return the same reference on iteration " + iteration + ", call " + i,
                    firstResult,
                    result
                );
            }

            // No additional refreshes should have occurred
            assertEquals(
                "No additional refreshes should occur within TTL on iteration " + iteration,
                refreshesAfterInit,
                refreshCount.get()
            );
        }
    }

    /**
     * Property 1 (expiry path): After TTL expiry, getOrRefresh() triggers a refresh.
     * <p>
     * Uses a zero-millisecond TTL to guarantee expiry on every call.
     * <p>
     * <b>Validates: Requirements 7.2, 7.3, 7.4</b>
     */
    public void testCacheRefreshesAfterTTLExpiry() {
        for (int iteration = 0; iteration < 100; iteration++) {
            long allocatedBytes = randomLongBetween(0, Long.MAX_VALUE);
            long residentBytes = randomLongBetween(0, Long.MAX_VALUE);
            NativeMemoryStats initialStats = new NativeMemoryStats(allocatedBytes, residentBytes);

            AtomicInteger refreshCount = new AtomicInteger(0);

            // TTL of 0ms means every call triggers a refresh
            SingleObjectCache<NativeMemoryStats> cache = new SingleObjectCache<NativeMemoryStats>(
                TimeValue.timeValueMillis(0),
                initialStats
            ) {
                @Override
                protected NativeMemoryStats refresh() {
                    refreshCount.incrementAndGet();
                    return new NativeMemoryStats(randomLongBetween(0, Long.MAX_VALUE), randomLongBetween(0, Long.MAX_VALUE));
                }
            };

            // Each call should trigger a refresh since TTL is 0
            int callCount = randomIntBetween(2, 5);
            for (int i = 0; i < callCount; i++) {
                cache.getOrRefresh();
            }

            // With TTL=0, every call should trigger a refresh
            assertTrue(
                "With TTL=0, refresh should be called at least "
                    + callCount
                    + " times on iteration "
                    + iteration
                    + ", got "
                    + refreshCount.get(),
                refreshCount.get() >= callCount
            );
        }
    }

    /**
     * Tests that the cache correctly stores and returns NativeMemoryStats values
     * with various random field values.
     * <p>
     * <b>Validates: Requirements 7.2, 7.3, 7.4</b>
     */
    public void testCacheStoresCorrectValues() {
        for (int iteration = 0; iteration < 100; iteration++) {
            long allocatedBytes = randomLongBetween(-1, Long.MAX_VALUE);
            long residentBytes = randomLongBetween(-1, Long.MAX_VALUE);
            NativeMemoryStats expectedStats = new NativeMemoryStats(allocatedBytes, residentBytes);

            SingleObjectCache<NativeMemoryStats> cache = new SingleObjectCache<NativeMemoryStats>(
                TimeValue.timeValueSeconds(60),
                expectedStats
            ) {
                @Override
                protected NativeMemoryStats refresh() {
                    return expectedStats;
                }
            };

            NativeMemoryStats result = cache.getOrRefresh();
            assertEquals("allocatedBytes should match on iteration " + iteration, allocatedBytes, result.getAllocatedBytes());
            assertEquals("residentBytes should match on iteration " + iteration, residentBytes, result.getResidentBytes());
        }
    }
}
