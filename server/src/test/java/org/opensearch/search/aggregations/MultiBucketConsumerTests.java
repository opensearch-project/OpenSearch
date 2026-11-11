/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import org.mockito.Mockito;

import static org.opensearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class MultiBucketConsumerTests extends OpenSearchTestCase {

    public void testMultiConsumerAcceptWhenCBTripped() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            new LongAdder(),
            true,
            1
        );
        // exception is thrown upfront since the circuit breaker has already tripped
        expectThrows(CircuitBreakingException.class, () -> multiBucketConsumer.accept(0));
        Mockito.verify(breaker, Mockito.times(0)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
    }

    public void testMultiConsumerAcceptToTripCB() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        LongAdder callCount = new LongAdder();
        for (int i = 0; i < 1024; i++) {
            callCount.increment();
        }
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            callCount,
            false,
            2
        );
        // circuit breaker check is performed as the value of call count would be 1025 which is still in range
        Mockito.when(breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets")).thenThrow(CircuitBreakingException.class);
        expectThrows(CircuitBreakingException.class, () -> multiBucketConsumer.accept(0));
        Mockito.verify(breaker, Mockito.times(1)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
    }

    public void testMultiConsumerAccept() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        LongAdder callCount = new LongAdder();
        for (int i = 0; i < 1100; i++) {
            callCount.increment();
        }
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            callCount,
            false,
            1
        );
        // no exception is thrown as the call count value is not in the expected range and CB is not checked
        multiBucketConsumer.accept(0);
        Mockito.verify(breaker, Mockito.times(0)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
    }

    public void testAcceptZerosCountsEveryCall() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        LongAdder callCount = new LongAdder();
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            callCount,
            false,
            1
        );
        multiBucketConsumer.acceptZeros(1000);
        assertEquals(1000, callCount.sum());
        assertEquals(0, multiBucketConsumer.getCount());
        // 1000 -> 1023 doesn't reach a multiple of 1024
        multiBucketConsumer.acceptZeros(23);
        Mockito.verify(breaker, Mockito.times(0)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
        // 1023 -> 1124 crosses 1024: one check
        multiBucketConsumer.acceptZeros(101);
        Mockito.verify(breaker, Mockito.times(1)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
        assertEquals(1124, callCount.sum());
    }

    public void testAcceptZerosChecksAtLeastOncePer1024Calls() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        LongAdder callCount = new LongAdder();
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            callCount,
            false,
            randomIntBetween(1, 16)
        );
        long calls = 0;
        for (int i = randomIntBetween(50, 200); i > 0; i--) {
            int batch = randomIntBetween(1, 1024);
            multiBucketConsumer.acceptZeros(batch);
            calls += batch;
        }
        assertEquals(calls, callCount.sum());
        Mockito.verify(breaker, Mockito.atLeast((int) (calls / 1024))).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
    }

    /**
     * Slices of a concurrent search share one consumer. Each crossing of a multiple of 1024 is seen by the thread whose
     * batch caused it, however the threads interleave.
     */
    public void testAcceptZerosChecksAtLeastOncePer1024CallsWithConcurrentCallers() throws Exception {
        final AtomicInteger checks = new AtomicInteger();
        final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST) {
            @Override
            public double addEstimateBytesAndMaybeBreak(long bytes, String label) {
                checks.incrementAndGet();
                return 0;
            }
        };
        final LongAdder callCount = new LongAdder();
        final MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            callCount,
            false,
            1
        );
        final int[][] batches = new int[randomIntBetween(2, 8)][];
        long calls = 0;
        for (int t = 0; t < batches.length; t++) {
            batches[t] = new int[randomIntBetween(100, 500)];
            for (int i = 0; i < batches[t].length; i++) {
                batches[t][i] = randomIntBetween(1, 1024);
                calls += batches[t][i];
            }
        }
        final CountDownLatch start = new CountDownLatch(1);
        final Thread[] threads = new Thread[batches.length];
        for (int t = 0; t < threads.length; t++) {
            final int[] mine = batches[t];
            threads[t] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                for (int batch : mine) {
                    multiBucketConsumer.acceptZeros(batch);
                }
            });
            threads[t].start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
        assertEquals(calls, callCount.sum());
        assertThat((long) checks.get(), greaterThanOrEqualTo(calls / 1024));
    }

    public void testAcceptZerosWhenCBTripped() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            new LongAdder(),
            true,
            1
        );
        expectThrows(CircuitBreakingException.class, () -> multiBucketConsumer.acceptZeros(10));
        Mockito.verify(breaker, Mockito.times(0)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
    }

    public void testAcceptZerosTripsCB() {
        CircuitBreaker breaker = Mockito.mock(CircuitBreaker.class);
        LongAdder callCount = new LongAdder();
        callCount.add(1000);
        MultiBucketConsumerService.MultiBucketConsumer multiBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            breaker,
            callCount,
            false,
            1
        );
        Mockito.when(breaker.addEstimateBytesAndMaybeBreak(0, "allocated_buckets")).thenThrow(CircuitBreakingException.class);
        expectThrows(CircuitBreakingException.class, () -> multiBucketConsumer.acceptZeros(100));
        // later calls fail fast, as with accept(0)
        expectThrows(CircuitBreakingException.class, () -> multiBucketConsumer.acceptZeros(1));
        Mockito.verify(breaker, Mockito.times(1)).addEstimateBytesAndMaybeBreak(0, "allocated_buckets");
    }
}
