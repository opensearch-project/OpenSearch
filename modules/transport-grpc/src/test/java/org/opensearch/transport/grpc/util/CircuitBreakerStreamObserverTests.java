/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.util;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import io.grpc.stub.StreamObserver;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for CircuitBreakerStreamObserver wrapper.
 * This wrapper ensures circuit breaker bytes are released when gRPC responses complete.
 */
public class CircuitBreakerStreamObserverTests extends OpenSearchTestCase {

    @Mock
    private StreamObserver<String> delegateObserver;

    @Mock
    private CircuitBreakerService circuitBreakerService;

    @Mock
    private CircuitBreaker circuitBreaker;

    private CircuitBreakerStreamObserver<String> wrapperObserver;
    private static final int REQUEST_SIZE = 1024;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS)).thenReturn(circuitBreaker);
        wrapperObserver = new CircuitBreakerStreamObserver<>(delegateObserver, circuitBreakerService, REQUEST_SIZE);
    }

    public void testOnNextDelegatesToDelegate() {
        String value = "test-value";
        wrapperObserver.onNext(value);
        verify(delegateObserver).onNext(value);
        // Bytes should not be released on onNext
        verify(circuitBreaker, never()).addWithoutBreaking(anyLong());
    }

    public void testOnCompletedReleasesBytes() {
        wrapperObserver.onCompleted();

        // Verify delegate was called
        verify(delegateObserver).onCompleted();

        // Verify bytes were released exactly once
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq((long) -REQUEST_SIZE));
    }

    public void testOnErrorReleasesBytes() {
        Throwable error = new RuntimeException("Test error");
        wrapperObserver.onError(error);

        // Verify delegate was called
        verify(delegateObserver).onError(error);

        // Verify bytes were released exactly once
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq((long) -REQUEST_SIZE));
    }

    public void testBytesReleasedExactlyOnceOnMultipleCalls() {
        // Call onCompleted first
        wrapperObserver.onCompleted();
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq((long) -REQUEST_SIZE));

        // Call onError - should not release again
        wrapperObserver.onError(new RuntimeException("Error"));
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq((long) -REQUEST_SIZE));

        // Verify delegate was called for both
        verify(delegateObserver).onCompleted();
        verify(delegateObserver).onError(any());
    }

    public void testBytesReleasedExactlyOnceOnErrorThenCompleted() {
        // Call onError first
        wrapperObserver.onError(new RuntimeException("Error"));
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq((long) -REQUEST_SIZE));

        // Call onCompleted - should not release again
        wrapperObserver.onCompleted();
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq((long) -REQUEST_SIZE));

        // Verify delegate was called for both
        verify(delegateObserver).onError(any());
        verify(delegateObserver).onCompleted();
    }

    public void testMultipleOnNextCallsDoNotReleaseBytes() {
        wrapperObserver.onNext("value1");
        wrapperObserver.onNext("value2");
        wrapperObserver.onNext("value3");

        verify(delegateObserver, times(3)).onNext(any());
        verify(circuitBreaker, never()).addWithoutBreaking(anyLong());
    }

    public void testOnCompletedAfterMultipleOnNextReleasesBytes() {
        wrapperObserver.onNext("value1");
        wrapperObserver.onNext("value2");
        wrapperObserver.onCompleted();

        verify(delegateObserver, times(2)).onNext(any());
        verify(delegateObserver).onCompleted();
        verify(circuitBreaker, times(1)).addWithoutBreaking(eq((long) -REQUEST_SIZE));
    }
}
