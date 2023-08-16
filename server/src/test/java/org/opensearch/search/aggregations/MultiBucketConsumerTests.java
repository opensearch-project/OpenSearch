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
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.LongAdder;

import org.mockito.Mockito;

import static org.opensearch.search.aggregations.MultiBucketConsumerService.DEFAULT_MAX_BUCKETS;

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
}
