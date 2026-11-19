/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ArrowScanRegistryTests extends OpenSearchTestCase {

    public void testRejectsScansBeyondTheLimit() throws Exception {
        ArrowScanRegistry registry = new ArrowScanRegistry(2, 1);
        Releasable first = registry.acquireScanSlot(TimeValue.ZERO);
        Releasable second = registry.acquireScanSlot(TimeValue.ZERO);
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> registry.acquireScanSlot(TimeValue.timeValueMillis(50))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("maximum of [2] open scans"));
        first.close();
        first.close();
        Releasable third = registry.acquireScanSlot(TimeValue.ZERO);
        expectThrows(OpenSearchRejectedExecutionException.class, () -> registry.acquireScanSlot(TimeValue.ZERO));
        second.close();
        third.close();
    }

    public void testWaitingScanGetsAFreedSlot() throws Exception {
        ArrowScanRegistry registry = new ArrowScanRegistry(1, 1);
        Releasable held = registry.acquireScanSlot(TimeValue.ZERO);
        AtomicReference<Releasable> acquired = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            try {
                acquired.set(registry.acquireScanSlot(TimeValue.timeValueSeconds(30)));
            } catch (Exception e) {
                throw new AssertionError(e);
            } finally {
                done.countDown();
            }
        });
        assertFalse(done.await(100, TimeUnit.MILLISECONDS));
        held.close();
        assertTrue(done.await(10, TimeUnit.SECONDS));
        acquired.get().close();
    }

    public void testRaisingTheLimitAdmitsWaitingScans() throws Exception {
        ArrowScanRegistry registry = new ArrowScanRegistry(1, 1);
        Releasable held = registry.acquireScanSlot(TimeValue.ZERO);
        CountDownLatch done = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            try (Releasable ignored = registry.acquireScanSlot(TimeValue.timeValueSeconds(30))) {
                done.countDown();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        assertFalse(done.await(100, TimeUnit.MILLISECONDS));
        registry.setMaxOpenScans(2);
        assertTrue(done.await(10, TimeUnit.SECONDS));
        held.close();
    }

    public void testBatchSlotsAreSeparateFromScanSlots() throws Exception {
        ArrowScanRegistry registry = new ArrowScanRegistry(1, 1);
        try (Releasable scan = registry.acquireScanSlot(TimeValue.ZERO); Releasable batch = registry.acquireBatchSlot(Long.MAX_VALUE)) {
            CountDownLatch done = new CountDownLatch(1);
            Thread.ofVirtual().start(() -> {
                try (Releasable ignored = registry.acquireBatchSlot(Long.MAX_VALUE)) {
                    done.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertFalse("a second batch waits for the first", done.await(100, TimeUnit.MILLISECONDS));
            batch.close();
            assertTrue(done.await(10, TimeUnit.SECONDS));
        }
    }

    public void testBatchSlotWaitIsBounded() throws Exception {
        ArrowScanRegistry registry = new ArrowScanRegistry(1, 1);
        try (Releasable batch = registry.acquireBatchSlot(0)) {
            assertNotNull(batch);
            assertNull("no slot frees up in time", registry.acquireBatchSlot(TimeUnit.MILLISECONDS.toNanos(50)));
        }
        try (Releasable batch = registry.acquireBatchSlot(0)) {
            assertNotNull("the slot is free again", batch);
        }
    }
}
