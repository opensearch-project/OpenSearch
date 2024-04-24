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

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Transfer semaphore holder for controlled transfer of data to remote.
 */
public class TransferSemaphoresHolder {
    private static final Logger log = LogManager.getLogger(TransferSemaphoresHolder.class);
    // For tests
    protected TypeSemaphore lowPrioritySemaphore;
    protected TypeSemaphore highPrioritySemaphore;
    private final int highPriorityPermits;
    private final int lowPriorityPermits;
    private final int acquireWaitDuration;
    private final TimeUnit acquireWaitDurationUnit;

    /**
     * Constructor to create semaphores holder.
     */
    public TransferSemaphoresHolder(int availablePermits, double priorityPermitAllocation, int acquireWaitDuration, TimeUnit timeUnit) {

        this.highPriorityPermits = (int) (priorityPermitAllocation * availablePermits);
        this.highPrioritySemaphore = new TypeSemaphore(highPriorityPermits, "high");
        this.lowPriorityPermits = availablePermits - highPriorityPermits;
        this.lowPrioritySemaphore = new TypeSemaphore(lowPriorityPermits, "low");
        this.acquireWaitDuration = acquireWaitDuration;
        this.acquireWaitDurationUnit = timeUnit;
    }

    /**
     * Overridden semaphore to identify transfer semaphores among all other semaphores for triaging.
     */
    public static class TypeSemaphore extends Semaphore {
        private final String type;

        public TypeSemaphore(int permits, String type) {
            super(permits);
            this.type = type;
        }

        public String getType() {
            return type;
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
                + highPrioritySemaphore.availablePermits()
                + " and low priority permits: "
                + lowPrioritySemaphore.availablePermits()
        );
        // Try acquiring low priority permit or high priority permit immediately if available.
        // Otherwise, we wait for low priority permit.
        if (Objects.requireNonNull(writePriority) == WritePriority.LOW) {
            if (lowPrioritySemaphore.tryAcquire()) {
                return lowPrioritySemaphore;
            } else if (highPrioritySemaphore.availablePermits() > 0.4 * highPriorityPermits && highPrioritySemaphore.tryAcquire()) {
                return highPrioritySemaphore;
            } else if (lowPrioritySemaphore.tryAcquire(acquireWaitDuration, acquireWaitDurationUnit)) {
                return lowPrioritySemaphore;
            }
            return null;
        }

        // Try acquiring high priority permit or low priority permit immediately if available.
        // Otherwise, we wait for high priority permit.
        if (highPrioritySemaphore.tryAcquire()) {
            return highPrioritySemaphore;
        } else if (requestContext.lowPriorityPermitsConsumable && lowPrioritySemaphore.tryAcquire()) {
            return lowPrioritySemaphore;
        } else if (highPrioritySemaphore.tryAcquire(acquireWaitDuration, acquireWaitDurationUnit)) {
            return highPrioritySemaphore;
        }
        return null;
    }

    /**
     * Used in tests.
     */
    public int getHighPriorityPermits() {
        return highPriorityPermits;
    }

    /**
     * Used in tests.
     */
    public int getLowPriorityPermits() {
        return lowPriorityPermits;
    }

    /**
     * Used in tests.
     */
    public int getAvailableHighPriorityPermits() {
        return highPrioritySemaphore.availablePermits();
    }

    /**
     * Used in tests.
     */
    public int getAvailableLowPriorityPermits() {
        return lowPrioritySemaphore.availablePermits();
    }
}
