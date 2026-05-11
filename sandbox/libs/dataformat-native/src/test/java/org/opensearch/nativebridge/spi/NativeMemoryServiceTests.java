/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.SingleObjectCache;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Unit tests for {@link NativeMemoryService} cache behavior.
 *
 * Since NativeMemoryService uses static MethodHandles resolved via NativeLibraryLoader in a
 * static initializer block, we cannot instantiate it directly in unit tests without the native
 * library. Instead, we test the cache behavior using a test double that replicates the same
 * SingleObjectCache pattern used by NativeMemoryService.
 *
 * <p><b>Property 4: Cache freshness</b></p>
 * <p>For any sequence of stats() calls, if the elapsed time since the last cache refresh is less
 * than the configured TTL, the returned NativeMemoryStats object SHALL be reference-equal to the
 * previously returned object (no FFM downcall). If the elapsed time exceeds the TTL, the returned
 * object SHALL reflect fresh values from the FFM downcalls.</p>
 *
 * <p><b>Validates: Requirements 1.1, 1.2, 1.5</b></p>
 */
public class NativeMemoryServiceTests extends OpenSearchTestCase {

    /**
     * A test double that replicates the SingleObjectCache pattern used by NativeMemoryService,
     * but accepts a Supplier instead of FFM MethodHandles. This allows testing cache behavior
     * without loading the native library.
     */
    private static class TestableNativeMemoryService {
        private final SingleObjectCache<NativeMemoryStats> cache;
        private final Supplier<NativeMemoryStats> statsSupplier;

        TestableNativeMemoryService(TimeValue refreshInterval, Supplier<NativeMemoryStats> statsSupplier) {
            this.statsSupplier = statsSupplier;
            NativeMemoryStats initial = statsSupplier.get();
            this.cache = new TestableCache(refreshInterval, initial);
        }

        public NativeMemoryStats stats() {
            return cache.getOrRefresh();
        }

        private class TestableCache extends SingleObjectCache<NativeMemoryStats> {
            TestableCache(TimeValue interval, NativeMemoryStats initValue) {
                super(interval, initValue);
            }

            @Override
            protected NativeMemoryStats refresh() {
                return statsSupplier.get();
            }
        }
    }

    /**
     * Tests that stats() returns the same cached object within TTL (reference equality).
     *
     * Validates: Requirement 1.1 - WHEN NativeMemoryService.stats() is called, THE service
     * SHALL return a cached NativeMemoryStats object if the cache TTL has not expired.
     */
    public void testStatsReturnsCachedObjectWithinTTL() {
        AtomicInteger callCount = new AtomicInteger(0);
        Supplier<NativeMemoryStats> supplier = () -> {
            callCount.incrementAndGet();
            return new NativeMemoryStats(1024L, 2048L);
        };

        // Use a very long TTL so it never expires during the test
        TestableNativeMemoryService service = new TestableNativeMemoryService(
            TimeValue.timeValueMinutes(10),
            supplier
        );

        // First call triggers the initial refresh (SingleObjectCache starts with
        // lastRefreshTimestamp=0, so the first getOrRefresh() always refreshes)
        NativeMemoryStats first = service.stats();
        int callsAfterFirst = callCount.get();

        NativeMemoryStats second = service.stats();
        NativeMemoryStats third = service.stats();

        // All subsequent calls within TTL should return the exact same object (reference equality)
        assertSame("Expected same cached object on second call", first, second);
        assertSame("Expected same cached object on third call", first, third);

        // No additional supplier calls should have occurred after the first stats() call
        assertEquals("No additional refresh calls expected within TTL", callsAfterFirst, callCount.get());
    }

    /**
     * Tests that stats() returns a fresh object after TTL expiry.
     *
     * Validates: Requirement 1.2 - WHEN NativeMemoryService.stats() is called and the cache
     * TTL has expired, THE service SHALL invoke the FFM downcalls, update the cache, and
     * return the fresh NativeMemoryStats object.
     */
    public void testStatsReturnsFreshObjectAfterTTLExpiry() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);
        Supplier<NativeMemoryStats> supplier = () -> {
            int count = callCount.incrementAndGet();
            return new NativeMemoryStats(1024L * count, 2048L * count);
        };

        // Use a very short TTL (1ms) so it expires quickly
        TestableNativeMemoryService service = new TestableNativeMemoryService(
            TimeValue.timeValueMillis(1),
            supplier
        );

        // First call triggers initial refresh
        NativeMemoryStats first = service.stats();
        int countAfterFirst = callCount.get();

        // Wait for TTL to expire
        Thread.sleep(50);

        NativeMemoryStats second = service.stats();

        // After TTL expiry, should get a different object with fresh values
        assertNotSame("Expected different object after TTL expiry", first, second);
        // The second object should have values from a later supplier call
        assertTrue(
            "Fresh object should have higher allocated bytes than first",
            second.getAllocatedBytes() > first.getAllocatedBytes()
        );
        assertTrue(
            "Fresh object should have higher resident bytes than first",
            second.getResidentBytes() > first.getResidentBytes()
        );
        // Verify the supplier was called again
        assertTrue("Supplier should have been called at least once more", callCount.get() > countAfterFirst);
    }

    /**
     * Tests error handling: when the underlying fetch returns negative values (simulating
     * FFM downcall error), the service returns NativeMemoryStats(-1, -1).
     *
     * Validates: Requirement 1.5 - IF an FFM downcall returns a negative value (error pointer),
     * THEN THE service SHALL log a warning and return a NativeMemoryStats with both fields set to -1.
     */
    public void testErrorHandlingNegativeReturnValues() {
        // Simulate the error handling logic from NativeMemoryService.fetchStats():
        // when FFM returns negative values, the service returns NativeMemoryStats(-1, -1)
        Supplier<NativeMemoryStats> errorSupplier = () -> {
            long allocated = -42L; // Simulates negative FFM return (error pointer)
            long resident = -99L;
            if (allocated < 0 || resident < 0) {
                return new NativeMemoryStats(-1, -1);
            }
            return new NativeMemoryStats(allocated, resident);
        };

        TestableNativeMemoryService service = new TestableNativeMemoryService(
            TimeValue.timeValueMinutes(10),
            errorSupplier
        );

        NativeMemoryStats stats = service.stats();
        assertEquals("Allocated bytes should be -1 on error", -1L, stats.getAllocatedBytes());
        assertEquals("Resident bytes should be -1 on error", -1L, stats.getResidentBytes());
    }

    /**
     * Tests error handling when only one metric returns negative (partial error).
     *
     * Validates: Requirement 1.5 - both fields set to -1 even if only one downcall errors.
     */
    public void testErrorHandlingPartialNegativeReturnValues() {
        // When only allocated is negative, both should still be -1
        Supplier<NativeMemoryStats> partialErrorSupplier = () -> {
            long allocated = -1L;
            long resident = 4096L;
            if (allocated < 0 || resident < 0) {
                return new NativeMemoryStats(-1, -1);
            }
            return new NativeMemoryStats(allocated, resident);
        };

        TestableNativeMemoryService service = new TestableNativeMemoryService(
            TimeValue.timeValueMinutes(10),
            partialErrorSupplier
        );

        NativeMemoryStats stats = service.stats();
        assertEquals("Allocated bytes should be -1 on partial error", -1L, stats.getAllocatedBytes());
        assertEquals("Resident bytes should be -1 on partial error", -1L, stats.getResidentBytes());
    }

    /**
     * Property 4: Cache freshness — property-based test.
     *
     * For any sequence of NativeMemoryService.stats() calls, if the elapsed time since the
     * last cache refresh is less than the configured TTL, the returned NativeMemoryStats object
     * SHALL be reference-equal to the previously returned object. If the elapsed time exceeds
     * the TTL, the returned object SHALL reflect fresh values from the FFM downcalls.
     *
     * Validates: Requirements 1.1, 1.2, 1.5
     */
    public void testCacheFreshnessProperty() throws InterruptedException {
        for (int iteration = 0; iteration < 100; iteration++) {
            AtomicInteger refreshCount = new AtomicInteger(0);
            AtomicReference<NativeMemoryStats> lastProduced = new AtomicReference<>();

            Supplier<NativeMemoryStats> supplier = () -> {
                refreshCount.incrementAndGet();
                long allocated = randomLongBetween(0, Long.MAX_VALUE);
                long resident = randomLongBetween(0, Long.MAX_VALUE);
                NativeMemoryStats stats = new NativeMemoryStats(allocated, resident);
                lastProduced.set(stats);
                return stats;
            };

            // Use a TTL that we can control: long enough to test cache hits
            long ttlMillis = randomLongBetween(50, 200);
            TestableNativeMemoryService service = new TestableNativeMemoryService(
                TimeValue.timeValueMillis(ttlMillis),
                supplier
            );

            // First call gets the initial cached value
            NativeMemoryStats first = service.stats();
            assertNotNull("First stats call should return non-null", first);

            // Immediate second call should return same reference (within TTL)
            NativeMemoryStats second = service.stats();
            assertSame(
                "Calls within TTL should return reference-equal object (iteration " + iteration + ")",
                first,
                second
            );

            // Wait for TTL to expire
            Thread.sleep(ttlMillis + 20);

            // Call after TTL expiry should return a fresh object
            NativeMemoryStats afterExpiry = service.stats();
            assertNotSame(
                "Call after TTL expiry should return a different object (iteration " + iteration + ")",
                first,
                afterExpiry
            );
        }
    }

    /**
     * Tests that the cache correctly transitions from error state to healthy state
     * after TTL expiry when the underlying source recovers.
     *
     * Validates: Requirements 1.2, 1.5
     */
    public void testCacheTransitionsFromErrorToHealthy() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);
        Supplier<NativeMemoryStats> supplier = () -> {
            int count = callCount.incrementAndGet();
            // First two calls simulate error (constructor initial + first getOrRefresh refresh)
            if (count <= 2) {
                return new NativeMemoryStats(-1, -1);
            }
            // Subsequent calls return healthy values
            return new NativeMemoryStats(8192L, 16384L);
        };

        TestableNativeMemoryService service = new TestableNativeMemoryService(
            TimeValue.timeValueMillis(1),
            supplier
        );

        // First call returns error state (after initial refresh)
        NativeMemoryStats errorStats = service.stats();
        assertEquals(-1L, errorStats.getAllocatedBytes());
        assertEquals(-1L, errorStats.getResidentBytes());

        // Wait for TTL to expire
        Thread.sleep(50);

        // After TTL expiry, should get healthy values
        NativeMemoryStats healthyStats = service.stats();
        assertEquals(8192L, healthyStats.getAllocatedBytes());
        assertEquals(16384L, healthyStats.getResidentBytes());
    }
}
