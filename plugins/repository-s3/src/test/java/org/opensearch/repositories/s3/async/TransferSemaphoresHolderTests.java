/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mockito;

public class TransferSemaphoresHolderTests extends OpenSearchTestCase {

    public void testAllocation() {
        int availablePermits = randomIntBetween(5, 20);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int highPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - highPermits;
        TransferSemaphoresHolder transferSemaphoresHolder = new TransferSemaphoresHolder(
            availablePermits,
            priorityAllocation,
            1,
            TimeUnit.NANOSECONDS
        );
        assertEquals(highPermits, transferSemaphoresHolder.getHighPriorityPermits());
        assertEquals(lowPermits, transferSemaphoresHolder.getLowPriorityPermits());
    }

    public void testLowPriorityEventPermitAcquisition() throws InterruptedException {
        int availablePermits = randomIntBetween(5, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int highPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - highPermits;
        TransferSemaphoresHolder transferSemaphoresHolder = new TransferSemaphoresHolder(
            availablePermits,
            priorityAllocation,
            1,
            TimeUnit.NANOSECONDS
        );

        List<TransferSemaphoresHolder.TypeSemaphore> semaphores = new ArrayList<>();
        int highPermitsEligibleForLowEvents = highPermits - (int) (highPermits * 0.4);

        int lowAcquisitionsExpected = (highPermitsEligibleForLowEvents + lowPermits);
        for (int i = 0; i < lowAcquisitionsExpected; i++) {
            TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.LOW,
                requestContext
            );
            semaphores.add(acquiredSemaphore);
            if (i >= lowPermits) {
                assertEquals("high", acquiredSemaphore.getType());
            } else {
                assertEquals("low", acquiredSemaphore.getType());
            }
        }

        for (int i = 0; i < highPermits - highPermitsEligibleForLowEvents; i++) {
            TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.HIGH,
                requestContext
            );
            assertEquals("high", acquiredSemaphore.getType());
            semaphores.add(acquiredSemaphore);
        }

        TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
        TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
            WritePriority.LOW,
            requestContext
        );
        assertNull(acquiredSemaphore);

        assertEquals(availablePermits, semaphores.size());
        semaphores.forEach(Semaphore::release);
        assertEquals(highPermits, transferSemaphoresHolder.getHighPriorityPermits());
        assertEquals(lowPermits, transferSemaphoresHolder.getLowPriorityPermits());

    }

    public void testHighPermitEventAcquisition() throws InterruptedException {
        int availablePermits = randomIntBetween(5, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int highPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - highPermits;
        TransferSemaphoresHolder transferSemaphoresHolder = new TransferSemaphoresHolder(
            availablePermits,
            priorityAllocation,
            1,
            TimeUnit.NANOSECONDS
        );

        List<TransferSemaphoresHolder.TypeSemaphore> semaphores = new ArrayList<>();
        List<TransferSemaphoresHolder.TypeSemaphore> lowSemaphores = new ArrayList<>();
        int highAcquisitionsExpected = highPermits + lowPermits;
        TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
        for (int i = 0; i < highAcquisitionsExpected; i++) {
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.HIGH,
                requestContext
            );
            semaphores.add(acquiredSemaphore);
            if (i >= highPermits) {
                assertEquals("low", acquiredSemaphore.getType());
                lowSemaphores.add(acquiredSemaphore);
            } else {
                assertEquals("high", acquiredSemaphore.getType());
            }
        }
        assertEquals(availablePermits, semaphores.size());

        int lowAcquired = lowPermits;
        lowSemaphores.get(0).release();

        requestContext = transferSemaphoresHolder.createRequestContext();
        TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
            WritePriority.LOW,
            requestContext
        );
        semaphores.add(acquiredSemaphore);
        lowSemaphores.add(acquiredSemaphore);
        while (lowAcquired > 1) {
            requestContext = transferSemaphoresHolder.createRequestContext();
            acquiredSemaphore = transferSemaphoresHolder.acquirePermit(WritePriority.HIGH, requestContext);
            assertNull(acquiredSemaphore);
            lowAcquired--;
        }

        semaphores.forEach(Semaphore::release);
        assertEquals(highPermits, transferSemaphoresHolder.getHighPriorityPermits());
        assertEquals(lowPermits, transferSemaphoresHolder.getLowPriorityPermits());
    }

    private static class TestTransferSemaphoresHolder extends TransferSemaphoresHolder {
        AtomicInteger highWaitCount = new AtomicInteger();
        AtomicInteger lowWaitCount = new AtomicInteger();

        /**
         * Constructor to create semaphores holder.
         */
        public TestTransferSemaphoresHolder(
            int availablePermits,
            double priorityPermitAllocation,
            int acquireWaitDuration,
            TimeUnit timeUnit
        ) throws InterruptedException {
            super(availablePermits, priorityPermitAllocation, acquireWaitDuration, timeUnit);
            TypeSemaphore executingHighSemaphore = highPrioritySemaphore;
            TypeSemaphore executingLowSemaphore = lowPrioritySemaphore;

            this.highPrioritySemaphore = Mockito.spy(highPrioritySemaphore);
            this.lowPrioritySemaphore = Mockito.spy(lowPrioritySemaphore);
            Mockito.doAnswer(invocation -> {
                highWaitCount.incrementAndGet();
                return false;
            }).when(highPrioritySemaphore).tryAcquire(Mockito.anyLong(), Mockito.any(TimeUnit.class));
            Mockito.doAnswer(invocation -> executingHighSemaphore.availablePermits()).when(highPrioritySemaphore).availablePermits();
            Mockito.doAnswer(invocation -> executingHighSemaphore.tryAcquire()).when(highPrioritySemaphore).tryAcquire();

            Mockito.doAnswer(invocation -> {
                lowWaitCount.incrementAndGet();
                return false;
            }).when(lowPrioritySemaphore).tryAcquire(Mockito.anyLong(), Mockito.any(TimeUnit.class));
            Mockito.doAnswer(invocation -> executingLowSemaphore.availablePermits()).when(lowPrioritySemaphore).availablePermits();
            Mockito.doAnswer(invocation -> executingLowSemaphore.tryAcquire()).when(lowPrioritySemaphore).tryAcquire();
        }
    }

    public void testHighSemaphoreAcquiredWait() throws InterruptedException {
        int availablePermits = randomIntBetween(10, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int highPermits = (int) (availablePermits * priorityAllocation);
        TestTransferSemaphoresHolder transferSemaphoresHolder = new TestTransferSemaphoresHolder(
            availablePermits,
            priorityAllocation,
            5,
            TimeUnit.MINUTES
        );

        TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
        TransferSemaphoresHolder.TypeSemaphore lowSemaphore = transferSemaphoresHolder.acquirePermit(WritePriority.LOW, requestContext);
        assertEquals("low", lowSemaphore.getType());
        for (int i = 0; i < highPermits; i++) {
            requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.HIGH,
                requestContext
            );
            assertEquals("high", acquiredSemaphore.getType());
        }

        TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
            WritePriority.HIGH,
            requestContext
        );
        assertNull(acquiredSemaphore);
        assertEquals(1, transferSemaphoresHolder.highWaitCount.get());
        assertEquals(0, transferSemaphoresHolder.lowWaitCount.get());
    }

    public void testLowSemaphoreAcquiredWait() throws InterruptedException {
        int availablePermits = randomIntBetween(10, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int highPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - highPermits;
        TestTransferSemaphoresHolder transferSemaphoresHolder = new TestTransferSemaphoresHolder(
            availablePermits,
            priorityAllocation,
            5,
            TimeUnit.MINUTES
        );

        TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
        int highPermitsEligibleForLowEvents = highPermits - (int) (highPermits * 0.4);
        for (int i = 0; i < highPermitsEligibleForLowEvents; i++) {
            TransferSemaphoresHolder.TypeSemaphore lowSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.HIGH,
                requestContext
            );
            assertEquals("high", lowSemaphore.getType());
        }

        for (int i = 0; i < lowPermits; i++) {
            requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.LOW,
                requestContext
            );
            assertEquals("low", acquiredSemaphore.getType());
        }

        TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
            WritePriority.LOW,
            requestContext
        );
        assertNull(acquiredSemaphore);
        assertEquals(1, transferSemaphoresHolder.lowWaitCount.get());
        assertEquals(0, transferSemaphoresHolder.highWaitCount.get());
    }

}
