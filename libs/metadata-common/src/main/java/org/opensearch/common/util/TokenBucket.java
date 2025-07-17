/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * TokenBucket is used to limit the number of operations at a constant rate while allowing for short bursts.
 *
 * @opensearch.internal
 */
public class TokenBucket {
    /**
     * Defines a monotonically increasing counter.
     * <p>
     * Usage examples:
     * 1. clock = System::nanoTime can be used to perform rate-limiting per unit time
     * 2. clock = AtomicLong::get can be used to perform rate-limiting per unit number of operations
     */
    private final LongSupplier clock;

    /**
     * Defines the number of tokens added to the bucket per clock cycle.
     */
    private final double rate;

    /**
     * Defines the capacity and the maximum number of operations that can be performed per clock cycle before
     * the bucket runs out of tokens.
     */
    private final double burst;

    /**
     * Defines the current state of the token bucket.
     */
    private final AtomicReference<State> state;

    public TokenBucket(LongSupplier clock, double rate, double burst) {
        this(clock, rate, burst, burst);
    }

    public TokenBucket(LongSupplier clock, double rate, double burst, double initialTokens) {
        if (rate <= 0.0) {
            throw new IllegalArgumentException("rate must be greater than zero");
        }

        if (burst <= 0.0) {
            throw new IllegalArgumentException("burst must be greater than zero");
        }

        this.clock = clock;
        this.rate = rate;
        this.burst = burst;
        this.state = new AtomicReference<>(new State(Math.min(initialTokens, burst), clock.getAsLong()));
    }

    /**
     * If there are enough tokens in the bucket, it requests/deducts 'n' tokens and returns true.
     * Otherwise, returns false and leaves the bucket untouched.
     */
    public boolean request(double n) {
        if (n <= 0) {
            throw new IllegalArgumentException("requested tokens must be greater than zero");
        }

        // Refill tokens
        State currentState, updatedState;
        do {
            currentState = state.get();
            long now = clock.getAsLong();
            double incr = (now - currentState.lastRefilledAt) * rate;
            updatedState = new State(Math.min(currentState.tokens + incr, burst), now);
        } while (state.compareAndSet(currentState, updatedState) == false);

        // Deduct tokens
        do {
            currentState = state.get();
            if (currentState.tokens < n) {
                return false;
            }
            updatedState = new State(currentState.tokens - n, currentState.lastRefilledAt);
        } while (state.compareAndSet(currentState, updatedState) == false);

        return true;
    }

    public boolean request() {
        return request(1.0);
    }

    /**
     * Represents an immutable token bucket state.
     */
    private static class State {
        final double tokens;
        final long lastRefilledAt;

        public State(double tokens, long lastRefilledAt) {
            this.tokens = tokens;
            this.lastRefilledAt = lastRefilledAt;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            State state = (State) o;
            return Double.compare(state.tokens, tokens) == 0 && lastRefilledAt == state.lastRefilledAt;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tokens, lastRefilledAt);
        }
    }
}
