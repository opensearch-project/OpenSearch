/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Unit tests for {@link NativeMemoryUsageService}.
 *
 * <p>The service is a process-wide singleton. Each test must reset state in
 * {@link #setUp()}/{@link #tearDown()} so suppliers installed by a prior test
 * don't leak into the next.
 */
public class NativeMemoryUsageServiceTests extends OpenSearchTestCase {

    private NativeMemoryUsageService service;

    @Before
    public void resetServiceBefore() {
        service = NativeMemoryUsageService.getInstance();
        service.resetForTesting();
    }

    @After
    public void resetServiceAfter() {
        service.resetForTesting();
    }

    public void testGetInstanceReturnsSameSingleton() {
        // Singleton contract: every call returns the same instance.
        assertSame(NativeMemoryUsageService.getInstance(), NativeMemoryUsageService.getInstance());
    }

    public void testDefaultBudgetIsZero() {
        assertEquals(0L, service.getBudgetBytes());
    }

    public void testDefaultHasSnapshotProviderIsFalse() {
        // Until a backend opts in, the predicate must report false.
        assertFalse(service.hasSnapshotProvider());
    }

    public void testDefaultSnapshotIsEmpty() {
        // currentBytes for any id with the default supplier must be 0.
        assertEquals(0L, service.currentBytes(1L));
        assertEquals(0L, service.currentBytes(randomNonNegativeLong()));
    }

    public void testSetSnapshotSupplierFlipsHasSnapshotProvider() {
        Supplier<Map<Long, Long>> supplier = Collections::emptyMap;
        service.setSnapshotSupplier(supplier);
        assertTrue(service.hasSnapshotProvider());
    }

    public void testSetSnapshotSupplierWithNullIsNoOp() {
        Supplier<Map<Long, Long>> supplier = Collections::emptyMap;
        service.setSnapshotSupplier(supplier);
        assertTrue(service.hasSnapshotProvider());
        // null must NOT erase a working installation.
        service.setSnapshotSupplier(null);
        assertTrue(service.hasSnapshotProvider());
    }

    public void testSetBudgetSupplierWithNullIsNoOp() {
        service.setBudgetSupplier(() -> 1024L);
        assertEquals(1024L, service.getBudgetBytes());
        service.setBudgetSupplier(null);
        // Previous supplier must still be active.
        assertEquals(1024L, service.getBudgetBytes());
    }

    public void testGetBudgetBytesClampsNegative() {
        service.setBudgetSupplier(() -> -42L);
        // Negative values from a misbehaving supplier must NOT flip threshold math; clamp to 0.
        assertEquals(0L, service.getBudgetBytes());
    }

    public void testGetBudgetBytesPropagatesPositive() {
        long pool = 4L * 1024L * 1024L * 1024L;
        service.setBudgetSupplier(() -> pool);
        assertEquals(pool, service.getBudgetBytes());
    }

    public void testRefreshLoadsSnapshot() {
        Map<Long, Long> snapshot = new HashMap<>();
        snapshot.put(10L, 100L);
        snapshot.put(20L, 200L);
        service.setSnapshotSupplier(() -> snapshot);

        service.refresh();
        assertEquals(100L, service.currentBytes(10L));
        assertEquals(200L, service.currentBytes(20L));
        assertEquals(0L, service.currentBytes(30L));
        assertEquals(2, service.snapshotView().size());
    }

    public void testRefreshNullSnapshotFallsBackToEmpty() {
        // A misbehaving backend that returns null must NOT propagate NPE through the read path.
        service.setSnapshotSupplier(() -> null);
        service.refresh();
        assertEquals(0L, service.currentBytes(1L));
        assertTrue(service.snapshotView().isEmpty());
    }

    public void testRefreshSwallowsRuntimeExceptionFromSupplier() {
        // A throwing supplier (e.g. transient FFM error) must not break the SBP scheduler tick.
        service.setSnapshotSupplier(() -> { throw new RuntimeException("boom"); });
        service.refresh();
        assertEquals(0L, service.currentBytes(1L));
        assertTrue(service.snapshotView().isEmpty());
    }

    public void testRefreshReplacesPreviousSnapshot() {
        // First refresh loads {1: 100}; second refresh loads {2: 200}. The map reference must
        // swap atomically — entry 1 must no longer be present after the second refresh.
        Map<Long, Long> first = Map.of(1L, 100L);
        Map<Long, Long> second = Map.of(2L, 200L);
        AtomicInteger calls = new AtomicInteger();
        service.setSnapshotSupplier(() -> calls.getAndIncrement() == 0 ? first : second);

        service.refresh();
        assertEquals(100L, service.currentBytes(1L));
        assertEquals(0L, service.currentBytes(2L));

        service.refresh();
        assertEquals(0L, service.currentBytes(1L));
        assertEquals(200L, service.currentBytes(2L));
    }

    public void testRefreshCallsSupplierExactlyOnce() {
        // The SBP service refreshes once per tick. This must translate to exactly one supplier
        // invocation per refresh — for the DataFusion backend that maps to one FFM round-trip.
        AtomicInteger calls = new AtomicInteger();
        service.setSnapshotSupplier(() -> {
            calls.incrementAndGet();
            return Collections.emptyMap();
        });
        service.refresh();
        assertEquals(1, calls.get());

        // A subsequent currentBytes call must NOT trigger another supplier invocation.
        service.currentBytes(1L);
        service.currentBytes(2L);
        assertEquals(1, calls.get());

        service.refresh();
        assertEquals(2, calls.get());
    }

    public void testCurrentBytesReturnsZeroForUnknownTaskId() {
        service.setSnapshotSupplier(() -> Map.of(1L, 100L));
        service.refresh();
        assertEquals(100L, service.currentBytes(1L));
        assertEquals(0L, service.currentBytes(99L));
    }

    public void testResetForTestingClearsState() {
        service.setSnapshotSupplier(() -> Map.of(1L, 100L));
        service.setBudgetSupplier(() -> 1024L);
        service.refresh();
        assertTrue(service.hasSnapshotProvider());
        assertEquals(1024L, service.getBudgetBytes());
        assertEquals(100L, service.currentBytes(1L));

        service.resetForTesting();

        assertFalse(service.hasSnapshotProvider());
        assertEquals(0L, service.getBudgetBytes());
        assertEquals(0L, service.currentBytes(1L));
        assertTrue(service.snapshotView().isEmpty());
    }

    public void testConcurrentRefreshAndRead() throws Exception {
        // Reader threads must always see a consistent snapshot — never an in-flight mutation.
        // We alternate between two non-overlapping snapshots and assert that every read either
        // finds the value from snapshot A OR from snapshot B, never something that would imply
        // a partially-published map (e.g. a NullPointerException from an internal HashMap).
        Map<Long, Long> snapA = Map.of(1L, 100L);
        Map<Long, Long> snapB = Map.of(1L, 200L);
        AtomicInteger toggle = new AtomicInteger();
        service.setSnapshotSupplier(() -> toggle.getAndIncrement() % 2 == 0 ? snapA : snapB);

        int readerCount = 4;
        int iterationsPerReader = 1_000;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(readerCount + 1);

        Thread refresher = new Thread(() -> {
            try {
                start.await();
                for (int i = 0; i < iterationsPerReader; i++) {
                    service.refresh();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        }, "nm-service-refresher");
        refresher.start();

        Thread[] readers = new Thread[readerCount];
        for (int r = 0; r < readerCount; r++) {
            readers[r] = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterationsPerReader; i++) {
                        long bytes = service.currentBytes(1L);
                        // Every observation must be 0 (between refreshes, transient), 100 (snapA),
                        // or 200 (snapB). Anything else implies inconsistent publication.
                        if (bytes != 0L && bytes != 100L && bytes != 200L) {
                            throw new AssertionError("inconsistent snapshot value: " + bytes);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }, "nm-service-reader-" + r);
            readers[r].start();
        }

        start.countDown();
        assertTrue("threads did not finish in time", done.await(30, TimeUnit.SECONDS));
        for (Thread reader : readers) {
            reader.join();
        }
        refresher.join();
    }

    public void testBudgetSupplierInvokedEachCall() {
        // The service does NOT cache the budget — every getBudgetBytes call routes through the
        // supplier. This matters because the operator may flip the backend pool size at runtime.
        AtomicInteger calls = new AtomicInteger();
        LongSupplier supplier = () -> {
            calls.incrementAndGet();
            return 64L;
        };
        service.setBudgetSupplier(supplier);
        service.getBudgetBytes();
        service.getBudgetBytes();
        service.getBudgetBytes();
        assertEquals(3, calls.get());
    }
}
