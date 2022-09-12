/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.function.LongSupplier;

/**
 * TokenBucket is used to limit the number of operations at a constant rate while allowing for short bursts.
 *
 * @opensearch.internal
 */
public class TokenBucket {
    /**
     * Defines a monotonically increasing counter.
     *
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

    private double tokens;

    private long lastRefilledAt;

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
        this.tokens = Math.min(initialTokens, burst);
        this.lastRefilledAt = clock.getAsLong();
    }

    /**
     * Refills the token bucket.
     */
    private void refill() {
        long now = clock.getAsLong();
        double incr = (now - lastRefilledAt) * rate;
        tokens = Math.min(tokens + incr, burst);
        lastRefilledAt = now;
    }

    /**
     * If there are enough tokens in the bucket, it requests/deducts 'n' tokens and returns true.
     * Otherwise, returns false and leaves the bucket untouched.
     */
    public boolean request(double n) {
        if (n <= 0) {
            throw new IllegalArgumentException("requested tokens must be greater than zero");
        }

        synchronized (this) {
            refill();

            if (tokens >= n) {
                tokens -= n;
                return true;
            }

            return false;
        }
    }

    public boolean request() {
        return request(1.0);
    }
}
