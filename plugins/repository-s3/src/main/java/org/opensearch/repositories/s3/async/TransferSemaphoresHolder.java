/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.repositories.s3.GenericStatsMetricPublisher;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Transfer semaphore holder for controlled transfer of data to remote.
 */
public class TransferSemaphoresHolder {
    private static final Logger log = LogManager.getLogger(TransferSemaphoresHolder.class);
    // For tests
    protected TypeSemaphore lowPrioritySemaphore;
    protected TypeSemaphore normalPrioritySemaphore;
    private final int normalPriorityPermits;
    private final int lowPriorityPermits;
    private final int acquireWaitDuration;
    private final TimeUnit acquireWaitDurationUnit;

    /**
     * Constructor to create semaphores holder.
     */
    public TransferSemaphoresHolder(
        int normalPriorityPermits,
        int lowPriorityPermits,
        int acquireWaitDuration,
        TimeUnit timeUnit,
        GenericStatsMetricPublisher genericStatsPublisher
    ) {

        this.normalPriorityPermits = normalPriorityPermits;
        this.lowPriorityPermits = lowPriorityPermits;
        this.normalPrioritySemaphore = new TypeSemaphore(
            normalPriorityPermits,
            TypeSemaphore.PermitType.NORMAL,
            genericStatsPublisher::updateNormalPermits
        );
        this.lowPrioritySemaphore = new TypeSemaphore(
            lowPriorityPermits,
            TypeSemaphore.PermitType.LOW,
            genericStatsPublisher::updateLowPermits
        );
        this.acquireWaitDuration = acquireWaitDuration;
        this.acquireWaitDurationUnit = timeUnit;
    }

    /**
     * Overridden semaphore to identify transfer semaphores among all other semaphores for triaging.
     */
    public static class TypeSemaphore extends Semaphore {
        private final PermitType permitType;
        private final Consumer<Boolean> permitChangeConsumer;

        public enum PermitType {
            NORMAL,
            LOW;
        }

        public TypeSemaphore(int permits, PermitType permitType, Consumer<Boolean> permitChangeConsumer) {
            super(permits);
            this.permitType = permitType;
            this.permitChangeConsumer = permitChangeConsumer;
        }

        public PermitType getType() {
            return permitType;
        }

        @Override
        public boolean tryAcquire() {
            boolean acquired = super.tryAcquire();
            if (acquired) {
                permitChangeConsumer.accept(true);
            }
            return acquired;
        }

        @Override
        public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            boolean acquired = super.tryAcquire(timeout, unit);
            if (acquired) {
                permitChangeConsumer.accept(true);
            }
            return acquired;
        }

        @Override
        public void release() {
            super.release();
            permitChangeConsumer.accept(false);
        }
    }

    /**
     * For multiple part requests of a single file, request context object will be set with the decision if low
     * priority permits can also be utilized in high priority transfers of parts of the file. If high priority get fully
     * consumed then low priority permits will be acquired for transfer.
     *
     * If a low priority transfer request comes in and a high priority transfer is in progress then till current
     * high priority transfer finishes, low priority transfer may have to compete. This is an acceptable side effect
     * because low priority transfers are generally heavy and it is ok to have slow progress in the beginning.
     *
     */
    public static class RequestContext {

        private final boolean lowPriorityPermitsConsumable;

        private RequestContext(boolean lowPriorityPermitsConsumable) {
            this.lowPriorityPermitsConsumable = lowPriorityPermitsConsumable;
        }

    }

    public RequestContext createRequestContext() {
        return new RequestContext(this.lowPrioritySemaphore.availablePermits() == lowPriorityPermits);
    }

    /**
     * Acquire permit based on the availability and based on the transfer priority.
     * A high priority event can acquire a low priority semaphore if all low permits are available.
     * A low priority event can acquire a high priority semaphore if at least 40% of high permits are available. We
     * reserve this bandwidth to ensure that high priority events never wait for permits in case of ongoing low priority
     * transfers.
     */
    public TypeSemaphore acquirePermit(WritePriority writePriority, RequestContext requestContext) throws InterruptedException {
        log.debug(
            () -> "Acquire permit request for transfer type: "
                + writePriority
                + ". Available high priority permits: "
                + normalPrioritySemaphore.availablePermits()
                + " and low priority permits: "
                + lowPrioritySemaphore.availablePermits()
        );
        // Try acquiring low priority permit or high priority permit immediately if available.
        // Otherwise, we wait for low priority permit.
        if (Objects.requireNonNull(writePriority) == WritePriority.LOW) {
            if (lowPrioritySemaphore.tryAcquire()) {
                return lowPrioritySemaphore;
            } else if (normalPrioritySemaphore.availablePermits() > 0.4 * normalPriorityPermits && normalPrioritySemaphore.tryAcquire()) {
                return normalPrioritySemaphore;
            } else if (lowPrioritySemaphore.tryAcquire(acquireWaitDuration, acquireWaitDurationUnit)) {
                return lowPrioritySemaphore;
            }
            return null;
        }

        // Try acquiring high priority permit or low priority permit immediately if available.
        // Otherwise, we wait for high priority permit.
        if (normalPrioritySemaphore.tryAcquire()) {
            return normalPrioritySemaphore;
        } else if (requestContext.lowPriorityPermitsConsumable && lowPrioritySemaphore.tryAcquire()) {
            return lowPrioritySemaphore;
        } else if (normalPrioritySemaphore.tryAcquire(acquireWaitDuration, acquireWaitDurationUnit)) {
            return normalPrioritySemaphore;
        }
        return null;
    }

    /**
     * Used in tests.
     */
    public int getNormalPriorityPermits() {
        return normalPriorityPermits;
    }

    /**
     * Used in tests.
     */
    public int getLowPriorityPermits() {
        return lowPriorityPermits;
    }
}
