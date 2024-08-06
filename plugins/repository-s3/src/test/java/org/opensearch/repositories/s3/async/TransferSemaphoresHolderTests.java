/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.repositories.s3.GenericStatsMetricPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.Mockito;

import static org.opensearch.repositories.s3.async.TransferSemaphoresHolder.TypeSemaphore.PermitType;

public class TransferSemaphoresHolderTests extends OpenSearchTestCase {

    public void testAllocation() {
        int availablePermits = randomIntBetween(5, 20);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int normalPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - normalPermits;
        GenericStatsMetricPublisher genericStatsPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
        TransferSemaphoresHolder transferSemaphoresHolder = new TransferSemaphoresHolder(
            normalPermits,
            lowPermits,
            1,
            TimeUnit.NANOSECONDS,
            genericStatsPublisher
        );
        assertEquals(normalPermits, transferSemaphoresHolder.getNormalPriorityPermits());
        assertEquals(lowPermits, transferSemaphoresHolder.getLowPriorityPermits());
        assertEquals(0, genericStatsPublisher.getAcquiredNormalPriorityPermits());
        assertEquals(0, genericStatsPublisher.getAcquiredLowPriorityPermits());
    }

    public void testLowPriorityEventPermitAcquisition() throws InterruptedException {
        int availablePermits = randomIntBetween(5, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int normalPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - normalPermits;
        GenericStatsMetricPublisher genericStatsPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
        TransferSemaphoresHolder transferSemaphoresHolder = new TransferSemaphoresHolder(
            normalPermits,
            lowPermits,
            1,
            TimeUnit.NANOSECONDS,
            genericStatsPublisher
        );

        List<TransferSemaphoresHolder.TypeSemaphore> semaphores = new ArrayList<>();
        int normalPermitsEligibleForLowEvents = normalPermits - (int) (normalPermits * 0.4);

        int lowAcquisitionsExpected = (normalPermitsEligibleForLowEvents + lowPermits);
        for (int i = 0; i < lowAcquisitionsExpected; i++) {
            TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.LOW,
                requestContext
            );
            semaphores.add(acquiredSemaphore);
            if (i >= lowPermits) {
                assertEquals(PermitType.NORMAL, acquiredSemaphore.getType());
            } else {
                assertEquals(PermitType.LOW, acquiredSemaphore.getType());
            }
        }

        for (int i = 0; i < normalPermits - normalPermitsEligibleForLowEvents; i++) {
            TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.NORMAL,
                requestContext
            );
            assertEquals(PermitType.NORMAL, acquiredSemaphore.getType());
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
        assertEquals(normalPermits, transferSemaphoresHolder.getNormalPriorityPermits());
        assertEquals(lowPermits, transferSemaphoresHolder.getLowPriorityPermits());
        assertEquals(0, genericStatsPublisher.getAcquiredNormalPriorityPermits());
        assertEquals(0, genericStatsPublisher.getAcquiredLowPriorityPermits());

    }

    public void testNormalPermitEventAcquisition() throws InterruptedException {
        int availablePermits = randomIntBetween(5, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int normalPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - normalPermits;
        GenericStatsMetricPublisher genericStatsPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
        TransferSemaphoresHolder transferSemaphoresHolder = new TransferSemaphoresHolder(
            normalPermits,
            lowPermits,
            1,
            TimeUnit.NANOSECONDS,
            genericStatsPublisher
        );

        List<TransferSemaphoresHolder.TypeSemaphore> semaphores = new ArrayList<>();
        List<TransferSemaphoresHolder.TypeSemaphore> lowSemaphores = new ArrayList<>();
        int normalAcquisitionsExpected = normalPermits + lowPermits;
        TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
        for (int i = 0; i < normalAcquisitionsExpected; i++) {
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.NORMAL,
                requestContext
            );
            semaphores.add(acquiredSemaphore);
            if (i >= normalPermits) {
                assertEquals(PermitType.LOW, acquiredSemaphore.getType());
                lowSemaphores.add(acquiredSemaphore);
            } else {
                assertEquals(PermitType.NORMAL, acquiredSemaphore.getType());
            }
        }
        assertEquals(availablePermits, semaphores.size());

        int lowAcquired = lowPermits;

        Semaphore removedLowSemaphore = lowSemaphores.remove(0);
        removedLowSemaphore.release();
        semaphores.remove(removedLowSemaphore);

        requestContext = transferSemaphoresHolder.createRequestContext();
        TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
            WritePriority.LOW,
            requestContext
        );
        semaphores.add(acquiredSemaphore);
        lowSemaphores.add(acquiredSemaphore);
        while (lowAcquired > 1) {
            requestContext = transferSemaphoresHolder.createRequestContext();
            acquiredSemaphore = transferSemaphoresHolder.acquirePermit(WritePriority.NORMAL, requestContext);
            assertNull(acquiredSemaphore);
            lowAcquired--;
        }

        semaphores.forEach(Semaphore::release);
        assertEquals(normalPermits, transferSemaphoresHolder.getNormalPriorityPermits());
        assertEquals(lowPermits, transferSemaphoresHolder.getLowPriorityPermits());
        assertEquals(0, genericStatsPublisher.getAcquiredNormalPriorityPermits());
        assertEquals(0, genericStatsPublisher.getAcquiredLowPriorityPermits());
    }

    private static class TestTransferSemaphoresHolder extends TransferSemaphoresHolder {
        AtomicInteger normalWaitCount = new AtomicInteger();
        AtomicInteger lowWaitCount = new AtomicInteger();

        /**
         * Constructor to create semaphores holder.
         */
        public TestTransferSemaphoresHolder(
            int normalPermits,
            int lowPermits,
            int acquireWaitDuration,
            TimeUnit timeUnit,
            GenericStatsMetricPublisher genericStatsMetricPublisher
        ) throws InterruptedException {
            super(normalPermits, lowPermits, acquireWaitDuration, timeUnit, genericStatsMetricPublisher);
            TypeSemaphore executingNormalSemaphore = normalPrioritySemaphore;
            TypeSemaphore executingLowSemaphore = lowPrioritySemaphore;

            this.normalPrioritySemaphore = Mockito.spy(normalPrioritySemaphore);
            this.lowPrioritySemaphore = Mockito.spy(lowPrioritySemaphore);
            Mockito.doAnswer(invocation -> {
                normalWaitCount.incrementAndGet();
                return false;
            }).when(normalPrioritySemaphore).tryAcquire(Mockito.anyLong(), Mockito.any(TimeUnit.class));
            Mockito.doAnswer(invocation -> executingNormalSemaphore.availablePermits()).when(normalPrioritySemaphore).availablePermits();
            Mockito.doAnswer(invocation -> executingNormalSemaphore.tryAcquire()).when(normalPrioritySemaphore).tryAcquire();

            Mockito.doAnswer(invocation -> {
                lowWaitCount.incrementAndGet();
                return false;
            }).when(lowPrioritySemaphore).tryAcquire(Mockito.anyLong(), Mockito.any(TimeUnit.class));
            Mockito.doAnswer(invocation -> executingLowSemaphore.availablePermits()).when(lowPrioritySemaphore).availablePermits();
            Mockito.doAnswer(invocation -> executingLowSemaphore.tryAcquire()).when(lowPrioritySemaphore).tryAcquire();
        }
    }

    public void testNormalSemaphoreAcquiredWait() throws InterruptedException {
        int availablePermits = randomIntBetween(10, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int normalPermits = (int) (availablePermits * priorityAllocation);
        GenericStatsMetricPublisher genericStatsPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
        TestTransferSemaphoresHolder transferSemaphoresHolder = new TestTransferSemaphoresHolder(
            normalPermits,
            availablePermits - normalPermits,
            5,
            TimeUnit.MINUTES,
            genericStatsPublisher
        );

        TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
        TransferSemaphoresHolder.TypeSemaphore lowSemaphore = transferSemaphoresHolder.acquirePermit(WritePriority.LOW, requestContext);
        assertEquals(PermitType.LOW, lowSemaphore.getType());
        for (int i = 0; i < normalPermits; i++) {
            requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.NORMAL,
                requestContext
            );
            assertEquals(PermitType.NORMAL, acquiredSemaphore.getType());
        }

        TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
            WritePriority.NORMAL,
            requestContext
        );
        assertNull(acquiredSemaphore);
        assertEquals(1, transferSemaphoresHolder.normalWaitCount.get());
        assertEquals(0, transferSemaphoresHolder.lowWaitCount.get());
    }

    public void testLowSemaphoreAcquiredWait() throws InterruptedException {
        int availablePermits = randomIntBetween(10, 50);
        double priorityAllocation = randomDoubleBetween(0.1, 0.9, true);
        int normalPermits = (int) (availablePermits * priorityAllocation);
        int lowPermits = availablePermits - normalPermits;
        GenericStatsMetricPublisher genericStatsPublisher = new GenericStatsMetricPublisher(10000L, 10, 10000L, 10);
        TestTransferSemaphoresHolder transferSemaphoresHolder = new TestTransferSemaphoresHolder(
            normalPermits,
            lowPermits,
            5,
            TimeUnit.MINUTES,
            genericStatsPublisher
        );

        TransferSemaphoresHolder.RequestContext requestContext = transferSemaphoresHolder.createRequestContext();
        int normalPermitsEligibleForLowEvents = normalPermits - (int) (normalPermits * 0.4);
        for (int i = 0; i < normalPermitsEligibleForLowEvents; i++) {
            TransferSemaphoresHolder.TypeSemaphore lowSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.NORMAL,
                requestContext
            );
            assertEquals(PermitType.NORMAL, lowSemaphore.getType());
        }

        for (int i = 0; i < lowPermits; i++) {
            requestContext = transferSemaphoresHolder.createRequestContext();
            TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
                WritePriority.LOW,
                requestContext
            );
            assertEquals(PermitType.LOW, acquiredSemaphore.getType());
        }

        TransferSemaphoresHolder.TypeSemaphore acquiredSemaphore = transferSemaphoresHolder.acquirePermit(
            WritePriority.LOW,
            requestContext
        );
        assertNull(acquiredSemaphore);
        assertEquals(1, transferSemaphoresHolder.lowWaitCount.get());
        assertEquals(0, transferSemaphoresHolder.normalWaitCount.get());
    }

}
