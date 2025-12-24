/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.util;

import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.indices.breaker.CircuitBreakerService;

import io.grpc.stub.StreamObserver;

public class CircuitBreakerStreamObserver<T> implements StreamObserver<T> {
    private final StreamObserver<T> delegate;
    private final CircuitBreakerService circuitBreakerService;
    private final int requestSize;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public CircuitBreakerStreamObserver(
        StreamObserver<T> delegate,
        CircuitBreakerService circuitBreakerService,
        int requestSize
    ) {
        this.delegate = delegate;
        this.circuitBreakerService = circuitBreakerService;
        this.requestSize = requestSize;
    }

    @Override
    public void onNext(T value) {
        delegate.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
        releaseBytes();
        delegate.onError(t);
    }

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

