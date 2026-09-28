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

import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.stub.StreamObserver;

/**
 * Wrapper for StreamObserver that automatically releases circuit breaker bytes when a gRPC response completes.
 * This ensures that in-flight request memory is properly tracked and released, preventing memory leaks.
 * Bytes are released exactly once when either onCompleted() or onError() is called.
 *
 * @param <T> The type of message observed
 */
public class CircuitBreakerStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> delegate;
    private final CircuitBreakerService circuitBreakerService;
    private final int requestSize;
    private final AtomicBoolean released = new AtomicBoolean(false);

    /**
     * Creates a new CircuitBreakerStreamObserver wrapper.
     *
     * @param delegate The underlying StreamObserver to delegate calls to
     * @param circuitBreakerService The circuit breaker service for tracking in-flight requests
     * @param requestSize The size of the request in bytes that was added to the circuit breaker
     */
    public CircuitBreakerStreamObserver(StreamObserver<T> delegate, CircuitBreakerService circuitBreakerService, int requestSize) {
        this.delegate = delegate;
        this.circuitBreakerService = circuitBreakerService;
        this.requestSize = requestSize;
    }

    /**
     * Forwards the next value to the delegate observer.
     *
     * @param value The next value in the stream
     */
    @Override
    public void onNext(T value) {
        delegate.onNext(value);
    }

    /**
     * Releases circuit breaker bytes and forwards the error to the delegate observer.
     *
     * @param t The error that occurred
     */
    @Override
    public void onError(Throwable t) {
        releaseBytes();
        delegate.onError(t);
    }

    /**
     * Releases circuit breaker bytes and forwards the completion signal to the delegate observer.
     */
    @Override
    public void onCompleted() {
        releaseBytes();
        delegate.onCompleted();
    }

    private void releaseBytes() {
        if (released.compareAndSet(false, true) == false) {
            return;
        }
        CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
        breaker.addWithoutBreaking(-requestSize);
    }
}
