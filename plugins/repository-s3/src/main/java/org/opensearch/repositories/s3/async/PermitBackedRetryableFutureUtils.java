/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.core.exception.SdkException;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.unit.TimeValue;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Async wrapper over a completable future backed by transfer permits which provides natural backpressure in case
 * of transfer bursts. Additionally, it retries futures (with exp backoff) which fail due to S3 exception or
 * when permit couldn't be acquired within timeout period.
 *
 * @param <FutureType> Type of future response
 */
public class PermitBackedRetryableFutureUtils<FutureType> {

    // Package access for testing.
    Semaphore lowPrioritySemaphore;
    Semaphore highPrioritySemaphore;
    private final int lowPriorityPermits;
    private final int highPriorityPermits;
    private final int maxRetryAttempts;
    private static final int RETRY_BASE_INTERVAL_MILLIS = 1_000;
    private final AtomicBoolean lowPriorityTransferProgress;

    private final ExecutorService remoteTransferRetryPool;
    private final ScheduledExecutorService scheduler;

    /**
     *
     * @param maxRetryAttempts max number of retries
     * @param availablePermits Total available permits for transfer
     * @param priorityPermitAllocation Allocation bandwidth of priority permits. Rest will be allocated to low
     *                                 priority permits.
     */
    public PermitBackedRetryableFutureUtils(
        int maxRetryAttempts,
        int availablePermits,
        double priorityPermitAllocation,
        ExecutorService remoteTransferRetryPool,
        ScheduledExecutorService scheduler
    ) {
        this.maxRetryAttempts = maxRetryAttempts;
        this.highPriorityPermits = (int) (priorityPermitAllocation * availablePermits);
        this.highPrioritySemaphore = new TypeSemaphore(highPriorityPermits, "high");
        this.lowPriorityPermits = availablePermits - highPriorityPermits;
        this.lowPrioritySemaphore = new TypeSemaphore(lowPriorityPermits, "low");
        this.lowPriorityTransferProgress = new AtomicBoolean();
        this.remoteTransferRetryPool = remoteTransferRetryPool;
        this.scheduler = scheduler;
    }

    /**
     * Available low priority permits
     * @return available low priority permits
     */
    public int getAvailableLowPriorityPermits() {
        return lowPrioritySemaphore.availablePermits();
    }

    /**
     * Available high priority permits
     * @return available high priority permits
     */
    public int getAvailableHighPriorityPermits() {
        return highPrioritySemaphore.availablePermits();
    }

    /**
     * Named semaphore for debugging purpose
     */
    static class TypeSemaphore extends Semaphore {
        private final String type;

        public TypeSemaphore(int permits, String type) {
            super(permits);
            this.type = type;
        }

        @Override
        public String toString() {
            String toStr = super.toString();
            return toStr + " , type = " + type;
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
     * Custom exception to distinguish retryable futures.
     */
    static class RetryableException extends CompletionException {
        private final Iterator<TimeValue> retryBackoffDelayIterator;

        public RetryableException(Iterator<TimeValue> retryBackoffDelayIterator, String message, Throwable cause) {
            super(message, cause);
            this.retryBackoffDelayIterator = retryBackoffDelayIterator;
        }

        public RetryableException(Iterator<TimeValue> retryBackoffDelayIterator) {
            this.retryBackoffDelayIterator = retryBackoffDelayIterator;
        }
    }

    /**
     * DelayedExecutor and TaskSubmitter are copied from CompletableFuture. Duplicate classes are needed because
     * scheduler used by these cannot be overriden and we need a way to manage it from outside.
     */
    private static final class DelayedExecutor implements Executor {
        private final long delay;
        private final TimeUnit unit;
        private final Executor executor;
        private final ScheduledExecutorService scheduler;

        DelayedExecutor(long delay, TimeUnit unit, Executor executor, ScheduledExecutorService scheduler) {
            this.delay = delay;
            this.unit = unit;
            this.executor = executor;
            this.scheduler = scheduler;
        }

        public void execute(Runnable r) {
            scheduler.schedule(new TaskSubmitter(executor, r), delay, unit);
        }
    }

    private static final class TaskSubmitter implements Runnable {
        final Executor executor;
        final Runnable action;

        TaskSubmitter(Executor executor, Runnable action) {
            this.executor = executor;
            this.action = action;
        }

        public void run() {
            executor.execute(action);
        }
    }

    /**
     *
     * @param futureSupplier Supplier of the completable future
     * @param writePriority Priority of transfer
     * @param requestContext Request context object to set the decisions pertaining to transfer before transfers are
     *                       initiated.
     *
     * @return completable future backed by permits and retryable future.
     */
    public CompletableFuture<FutureType> createPermitBackedRetryableFuture(
        Supplier<CompletableFuture<FutureType>> futureSupplier,
        WritePriority writePriority,
        RequestContext requestContext
    ) {
        Iterator<TimeValue> retryBackoffDelayIterator = BackoffPolicy.exponentialBackoff(
            TimeValue.timeValueMillis(RETRY_BASE_INTERVAL_MILLIS),
            maxRetryAttempts
        ).iterator();
        Supplier<CompletableFuture<FutureType>> permitBackedFutureSupplier = createPermitBackedFutureSupplier(
            retryBackoffDelayIterator,
            requestContext.lowPriorityPermitsConsumable,
            futureSupplier,
            writePriority
        );

        CompletableFuture<FutureType> permitBackedFuture;
        try {
            permitBackedFuture = permitBackedFutureSupplier.get();
        } catch (RetryableException re) {
            // We need special handling when an exception occurs during first future creation itself.
            permitBackedFuture = retry(re, permitBackedFutureSupplier, retryBackoffDelayIterator);
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        }

        return flatten(
            permitBackedFuture.thenApply(CompletableFuture::completedFuture)
                .exceptionally(t -> retry(t, permitBackedFutureSupplier, retryBackoffDelayIterator))
        );
    }

    private static <FutureType> CompletableFuture<FutureType> flatten(
        CompletableFuture<CompletableFuture<FutureType>> completableCompletable
    ) {
        return completableCompletable.thenCompose(Function.identity());
    }

    private CompletableFuture<FutureType> retry(
        Throwable ex,
        Supplier<CompletableFuture<FutureType>> futureSupplier,
        Iterator<TimeValue> retryBackoffDelayIterator
    ) {
        if (!(ex instanceof RetryableException)) {
            return CompletableFuture.failedFuture(ex);
        }

        RetryableException retryableException = (RetryableException) ex;
        if (!retryBackoffDelayIterator.hasNext()) {
            return CompletableFuture.failedFuture(ex);
        }

        return flatten(
            flatten(
                CompletableFuture.supplyAsync(
                    futureSupplier,
                    new DelayedExecutor(
                        retryableException.retryBackoffDelayIterator.next().millis(),
                        TimeUnit.MILLISECONDS,
                        remoteTransferRetryPool,
                        scheduler
                    )
                )
            ).thenApply(CompletableFuture::completedFuture).exceptionally(t -> {
                if (t instanceof RetryableException) {
                    ex.addSuppressed(t);
                    return retry(ex, futureSupplier, retryBackoffDelayIterator);
                } else {
                    ex.addSuppressed(t);
                    return CompletableFuture.failedFuture(ex);
                }
            })
        );
    }

    // Package access for testing
    Semaphore acquirePermit(WritePriority writePriority, boolean isLowPriorityPermitsConsumable) throws InterruptedException {
        // Try acquiring low priority permit or high priority permit immediately if available.
        // Otherwise, we wait for low priority permit.
        if (Objects.requireNonNull(writePriority) == WritePriority.LOW) {
            if (lowPrioritySemaphore.tryAcquire()) {
                return lowPrioritySemaphore;
            } else if (highPrioritySemaphore.availablePermits() > 0.4 * highPriorityPermits && highPrioritySemaphore.tryAcquire()) {
                return highPrioritySemaphore;
            } else if (lowPrioritySemaphore.tryAcquire(5, TimeUnit.MINUTES)) {
                return lowPrioritySemaphore;
            }
            return null;
        }

        // Try acquiring high priority permit or low priority permit immediately if available.
        // Otherwise, we wait for high priority permit.
        if (highPrioritySemaphore.tryAcquire()) {
            return highPrioritySemaphore;
        } else if (isLowPriorityPermitsConsumable && lowPrioritySemaphore.tryAcquire()) {
            return lowPrioritySemaphore;
        } else if (highPrioritySemaphore.tryAcquire(5, TimeUnit.MINUTES)) {
            return highPrioritySemaphore;
        }
        return null;
    }

    private Supplier<CompletableFuture<FutureType>> createPermitBackedFutureSupplier(
        Iterator<TimeValue> retryBackoffDelayIterator,
        boolean lowPriorityPermitsConsumable,
        Supplier<CompletableFuture<FutureType>> futureSupplier,
        WritePriority writePriority
    ) {
        return () -> {
            Semaphore semaphore;
            try {
                semaphore = acquirePermit(writePriority, lowPriorityPermitsConsumable);
                if (semaphore == null) {
                    throw new RetryableException(retryBackoffDelayIterator);
                }
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }

            CompletableFuture<FutureType> future;
            try {
                future = futureSupplier.get();
            } catch (Exception ex) {
                // Exception in future creation. Can't retry this.
                semaphore.release();
                throw new RuntimeException(ex);
            }

            return future.handle((resp, t) -> {
                try {
                    if (t != null) {
                        Throwable ex = ExceptionsHelper.unwrap(t, SdkException.class);
                        if (ex != null) {
                            throw new RetryableException(retryBackoffDelayIterator, t.getMessage(), t);
                        }
                        throw new CompletionException(t);
                    }
                    return resp;
                } finally {
                    semaphore.release();
                }
            });
        };
    }

}
