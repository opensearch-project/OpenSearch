/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.repositories.s3.SocketAccess;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class PermitBackedRetryableFutureUtilsTests extends OpenSearchTestCase {
    private ExecutorService testExecutor;
    private ScheduledExecutorService scheduler;

    @Before
    public void setup() {
        this.testExecutor = Executors.newFixedThreadPool(30);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void cleanUp() {
        testExecutor.shutdown();
        scheduler.shutdown();
    }

    public void testFutureExecAndPermitRelease() throws InterruptedException, ExecutionException {
        PermitBackedRetryableFutureUtils permitBasedRetryableFutureUtils = new PermitBackedRetryableFutureUtils(
            3,
            Runtime.getRuntime().availableProcessors(),
            0.7,
            testExecutor,
            scheduler
        );
        int maxHighPermits = permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits();
        int lowPermits = Runtime.getRuntime().availableProcessors() - maxHighPermits;
        assertEquals((int) (0.7 * Runtime.getRuntime().availableProcessors()), maxHighPermits);
        assertEquals(
            Runtime.getRuntime().availableProcessors() - maxHighPermits,
            permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits()
        );

        Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
            assertEquals(maxHighPermits - 1, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
            assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
            return "success";
        }, testExecutor));

        PermitBackedRetryableFutureUtils.RequestContext requestContext = permitBasedRetryableFutureUtils.createRequestContext();
        CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
            supplier,
            WritePriority.HIGH,
            requestContext
        );
        resultFuture.get();

        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
    }

    static class TestPermitBackedRetryableFutureUtils extends PermitBackedRetryableFutureUtils {

        private final AtomicBoolean highPermitsFullyConsumed = new AtomicBoolean();
        private final AtomicBoolean lowPermitsFullyConsumed = new AtomicBoolean();
        private final AtomicBoolean lowPermitConsumedForHighPriority = new AtomicBoolean();
        private final AtomicBoolean highPermitsConsumedForLowPriority = new AtomicBoolean();
        private final AtomicBoolean waitedForPermit = new AtomicBoolean();
        private final Runnable onWaitForPermit;

        /**
         * @param maxRetryAttempts         max number of retries
         * @param availablePermits         Total available permits for transfer
         * @param priorityPermitAllocation Allocation bandwidth of priority permits. Rest will be allocated to low
         *                                 priority permits.
         */
        public TestPermitBackedRetryableFutureUtils(
            int maxRetryAttempts,
            int availablePermits,
            double priorityPermitAllocation,
            ExecutorService remoteTransferRetry,
            ScheduledExecutorService scheduler
        ) {
            super(maxRetryAttempts, availablePermits, priorityPermitAllocation, remoteTransferRetry, scheduler);
            this.onWaitForPermit = null;

        }

        static class TestSemaphore extends TypeSemaphore {
            private final Runnable onWaitForPermit;
            private final Supplier<Boolean> preAcquireFailure;

            public TestSemaphore(int permits, String type, Runnable onWaitForPermit, Supplier<Boolean> preAcquireFailure) {
                super(permits, type);
                this.onWaitForPermit = onWaitForPermit;
                this.preAcquireFailure = preAcquireFailure;
            }

            @Override
            public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
                if (preAcquireFailure != null && preAcquireFailure.get()) {
                    return false;
                }
                onWaitForPermit.run();
                return super.tryAcquire(timeout, unit);
            }

            @Override
            public boolean tryAcquire() {
                if (preAcquireFailure != null && preAcquireFailure.get()) {
                    return false;
                }
                return super.tryAcquire();
            }
        }

        public TestPermitBackedRetryableFutureUtils(
            int maxRetryAttempts,
            int availablePermits,
            double priorityPermitAllocation,
            Runnable onWaitForPermit,
            Supplier<Boolean> preAcquireFailure,
            ExecutorService remoteTransferRetry,
            ScheduledExecutorService scheduler
        ) {
            super(maxRetryAttempts, availablePermits, priorityPermitAllocation, remoteTransferRetry, scheduler);
            this.onWaitForPermit = () -> {
                waitedForPermit.set(true);
                onWaitForPermit.run();
            };
            this.highPrioritySemaphore = new TestSemaphore(
                highPrioritySemaphore.availablePermits(),
                "high",
                this.onWaitForPermit,
                preAcquireFailure
            );
            this.lowPrioritySemaphore = new TestSemaphore(
                lowPrioritySemaphore.availablePermits(),
                "low",
                this.onWaitForPermit,
                preAcquireFailure
            );
        }

        Semaphore acquirePermit(WritePriority writePriority, boolean isLowPriorityPermitsConsumable) throws InterruptedException {
            TypeSemaphore semaphore = (TypeSemaphore) super.acquirePermit(writePriority, isLowPriorityPermitsConsumable);
            if (semaphore == null) {
                return null;
            }
            if (semaphore.getType().equals("high")) {
                if (getAvailableHighPriorityPermits() == 0) {
                    highPermitsFullyConsumed.set(true);
                }
                if (writePriority == WritePriority.LOW) {
                    highPermitsConsumedForLowPriority.set(true);
                }
            } else if (semaphore.getType().equals("low")) {
                if (getAvailableLowPriorityPermits() == 0) {
                    lowPermitsFullyConsumed.set(true);
                }
                if (writePriority == WritePriority.HIGH) {
                    lowPermitConsumedForHighPriority.set(true);
                }
            }
            return semaphore;
        }
    }

    public void testLowPermitConsumptionForHighTask() throws InterruptedException, ExecutionException {
        TestPermitBackedRetryableFutureUtils permitBasedRetryableFutureUtils = new TestPermitBackedRetryableFutureUtils(
            3,
            Runtime.getRuntime().availableProcessors(),
            0.7,
            testExecutor,
            scheduler
        );
        int maxHighPermits = permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits();
        int lowPermits = Runtime.getRuntime().availableProcessors() - maxHighPermits;

        PermitBackedRetryableFutureUtils.RequestContext requestContext = permitBasedRetryableFutureUtils.createRequestContext();
        List<CompletableFuture<String>> futures = new ArrayList<>();
        CountDownLatch delayedLatch = new CountDownLatch(1);
        for (int reqIdx = 0; reqIdx < (maxHighPermits + lowPermits); reqIdx++) {
            Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
                try {
                    // Keep the permit acquired
                    delayedLatch.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return "success";
            }, testExecutor));
            CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
                supplier,
                WritePriority.HIGH,
                requestContext
            );
            futures.add(resultFuture);
        }
        // Now release all permits
        delayedLatch.countDown();

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).get();
        assertTrue(permitBasedRetryableFutureUtils.lowPermitsFullyConsumed.get());
        assertTrue(permitBasedRetryableFutureUtils.highPermitsFullyConsumed.get());
        assertTrue(permitBasedRetryableFutureUtils.lowPermitConsumedForHighPriority.get());
        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
    }

    public void testOnlyHighPermitsAcquiredWhenLowTaskInProgress() throws ExecutionException, InterruptedException {
        CountDownLatch delayedLatch = new CountDownLatch(1);
        TestPermitBackedRetryableFutureUtils permitBasedRetryableFutureUtils = new TestPermitBackedRetryableFutureUtils(
            3,
            Runtime.getRuntime().availableProcessors(),
            0.7,
            delayedLatch::countDown,
            null,
            testExecutor,
            scheduler
        );
        int maxHighPermits = permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits();
        int lowPermits = Runtime.getRuntime().availableProcessors() - maxHighPermits;

        PermitBackedRetryableFutureUtils.RequestContext lowRequestContext = permitBasedRetryableFutureUtils.createRequestContext();
        List<CompletableFuture<String>> futures = new ArrayList<>();

        Supplier<CompletableFuture<String>> lowSupplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
            try {
                // Keep the permit acquired
                delayedLatch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return "success";
        }, testExecutor));
        CompletableFuture<String> lowResultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
            lowSupplier,
            WritePriority.LOW,
            lowRequestContext
        );
        futures.add(lowResultFuture);

        PermitBackedRetryableFutureUtils.RequestContext requestContext = permitBasedRetryableFutureUtils.createRequestContext();
        for (int reqIdx = 0; reqIdx < maxHighPermits; reqIdx++) {
            Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
                try {
                    // Keep the permit acquired
                    delayedLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return "success";
            }, testExecutor));
            CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
                supplier,
                WritePriority.HIGH,
                requestContext
            );
            futures.add(resultFuture);
        }

        Thread t = new Thread(() -> {
            Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(
                () -> CompletableFuture.supplyAsync(() -> "success", testExecutor)
            );

            CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
                supplier,
                WritePriority.HIGH,
                requestContext
            );
            futures.add(resultFuture);
        });
        t.start();
        t.join();

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).get();
        assertTrue(permitBasedRetryableFutureUtils.waitedForPermit.get());
        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
    }

    public void testHighPermitsConsumedForLowTasks() throws ExecutionException, InterruptedException {
        CountDownLatch delayedLatch = new CountDownLatch(1);
        TestPermitBackedRetryableFutureUtils permitBasedRetryableFutureUtils = new TestPermitBackedRetryableFutureUtils(
            3,
            Runtime.getRuntime().availableProcessors(),
            0.7,
            delayedLatch::countDown,
            null,
            testExecutor,
            scheduler
        );
        int maxHighPermits = permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits();
        int lowPermits = Runtime.getRuntime().availableProcessors() - maxHighPermits;

        PermitBackedRetryableFutureUtils.RequestContext requestContext = permitBasedRetryableFutureUtils.createRequestContext();
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for (int reqIdx = 0; reqIdx < lowPermits; reqIdx++) {
            Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
                try {
                    // Keep the permit acquired
                    delayedLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return "success";
            }, testExecutor));
            CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
                supplier,
                WritePriority.LOW,
                requestContext
            );
            futures.add(resultFuture);
        }

        Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
            try {
                // Keep the permit acquired
                delayedLatch.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return "success";
        }, testExecutor));
        CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
            supplier,
            WritePriority.LOW,
            requestContext
        );
        futures.add(resultFuture);
        delayedLatch.countDown();

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).get();
        assertTrue(permitBasedRetryableFutureUtils.highPermitsConsumedForLowPriority.get());
        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
    }

    public void testFutureRetryOnSemaphoreFailure() throws ExecutionException, InterruptedException {
        int retryCount = 3;
        // Multiply by 3 as there are 3 ways in which permit can be acquired.
        AtomicInteger failureCount = new AtomicInteger((retryCount - 1) * 3);
        AtomicBoolean exhaustRetries = new AtomicBoolean();
        TestPermitBackedRetryableFutureUtils permitBasedRetryableFutureUtils = new TestPermitBackedRetryableFutureUtils(
            retryCount,
            Runtime.getRuntime().availableProcessors(),
            0.7,
            null,
            () -> {
                if (failureCount.get() > 0 || exhaustRetries.get()) {
                    failureCount.decrementAndGet();
                    return true;
                }
                return false;
            },
            testExecutor,
            scheduler
        );
        int maxHighPermits = permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits();
        int lowPermits = Runtime.getRuntime().availableProcessors() - maxHighPermits;

        PermitBackedRetryableFutureUtils.RequestContext requestContext = permitBasedRetryableFutureUtils.createRequestContext();
        List<CompletableFuture<String>> futures = new ArrayList<>();
        Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(
            () -> CompletableFuture.supplyAsync(() -> "success", testExecutor)
        );
        CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
            supplier,
            WritePriority.HIGH,
            requestContext
        );
        futures.add(resultFuture);

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).get();

        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
        // Reached here so future executed successfully after retries.

        exhaustRetries.set(true);
        resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(supplier, WritePriority.HIGH, requestContext);
        resultFuture.whenComplete((r, t) -> {
            assertNotNull(t);
            assertTrue(t instanceof PermitBackedRetryableFutureUtils.RetryableException);
        });
        CompletableFuture<String> finalResultFuture = resultFuture;
        assertThrows(Exception.class, finalResultFuture::get);
    }

    public void testFutureRetryOnExecFailure() throws ExecutionException, InterruptedException {
        int retryCount = 3;
        AtomicInteger failureCount = new AtomicInteger(retryCount);
        AtomicBoolean exhaustRetries = new AtomicBoolean();
        TestPermitBackedRetryableFutureUtils permitBasedRetryableFutureUtils = new TestPermitBackedRetryableFutureUtils(
            retryCount,
            Runtime.getRuntime().availableProcessors(),
            0.7,
            null,
            null,
            testExecutor,
            scheduler
        );
        int maxHighPermits = permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits();
        int lowPermits = Runtime.getRuntime().availableProcessors() - maxHighPermits;

        PermitBackedRetryableFutureUtils.RequestContext requestContext = permitBasedRetryableFutureUtils.createRequestContext();
        List<CompletableFuture<String>> futures = new ArrayList<>();
        Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
            if (failureCount.get() > 0 || exhaustRetries.get()) {
                failureCount.decrementAndGet();
                throw ApiCallAttemptTimeoutException.builder().build();
            }
            return "success";
        }, testExecutor));
        CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
            supplier,
            WritePriority.HIGH,
            requestContext
        );
        futures.add(resultFuture);

        CompletableFutureUtils.allOfExceptionForwarded(futures.toArray(CompletableFuture[]::new)).get();

        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
        // Reached here so future executed successfully after retries.

        exhaustRetries.set(true);
        resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(supplier, WritePriority.HIGH, requestContext);
        resultFuture.whenComplete((r, t) -> {
            assertNotNull(t);
            assertTrue(t instanceof PermitBackedRetryableFutureUtils.RetryableException);
        });
        CompletableFuture<String> finalResultFuture = resultFuture;
        assertThrows(Exception.class, finalResultFuture::get);

        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
    }

    public void testNonRetryableFailure() throws ExecutionException, InterruptedException {
        // Throw only once to ensure no retries for unknown exception.
        AtomicInteger failureCount = new AtomicInteger(1);
        AtomicBoolean exhaustRetries = new AtomicBoolean();
        TestPermitBackedRetryableFutureUtils permitBasedRetryableFutureUtils = new TestPermitBackedRetryableFutureUtils(
            3,
            Runtime.getRuntime().availableProcessors(),
            0.7,
            null,
            null,
            testExecutor,
            scheduler
        );
        int maxHighPermits = permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits();
        int lowPermits = Runtime.getRuntime().availableProcessors() - maxHighPermits;

        PermitBackedRetryableFutureUtils.RequestContext requestContext = permitBasedRetryableFutureUtils.createRequestContext();
        Supplier<CompletableFuture<String>> supplier = () -> SocketAccess.doPrivileged(() -> CompletableFuture.supplyAsync(() -> {
            if (failureCount.get() > 0) {
                failureCount.decrementAndGet();
                throw new RuntimeException("Generic exception");
            }
            return "success";
        }, testExecutor));

        CompletableFuture<String> resultFuture = permitBasedRetryableFutureUtils.createPermitBackedRetryableFuture(
            supplier,
            WritePriority.HIGH,
            requestContext
        );
        resultFuture.whenComplete((r, t) -> {
            assertNotNull(t);
            assertFalse(t instanceof PermitBackedRetryableFutureUtils.RetryableException);
        });
        assertThrows(Exception.class, resultFuture::get);

        assertEquals(maxHighPermits, permitBasedRetryableFutureUtils.getAvailableHighPriorityPermits());
        assertEquals(lowPermits, permitBasedRetryableFutureUtils.getAvailableLowPriorityPermits());
    }
}
