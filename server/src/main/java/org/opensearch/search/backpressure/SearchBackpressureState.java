/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure;

import org.opensearch.common.util.TokenBucket;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Tracks the current state of task completions and cancellations.
 *
 * @opensearch.internal
 */
public class SearchBackpressureState {
    private final AtomicReference<TokenBucket> rateLimiter, ratioLimiter;
    private final LongSupplier timeNanosSupplier;
    /**
     * The number of successful task completions.
     */
    private final AtomicLong completionCount = new AtomicLong();
    /**
     * The number of task cancellations due to limit breaches.
     */
    private final AtomicLong cancellationCount = new AtomicLong();
    /**
     * The number of times task cancellation limit was reached.
     */
    private final AtomicLong limitReachedCount = new AtomicLong();
    private double cancellationBurst, cancellationRate, cancellationRatio;

    SearchBackpressureState(
        LongSupplier timeNanosSupplier,
        double cancellationRateNanos,
        double cancellationBurst,
        double cancellationRatio) {
        rateLimiter = new AtomicReference<>(new TokenBucket(timeNanosSupplier, cancellationRateNanos, cancellationBurst));
        ratioLimiter = new AtomicReference<>(new TokenBucket(this::getCompletionCount, cancellationRatio, cancellationBurst));
        this.timeNanosSupplier = timeNanosSupplier;
        this.cancellationBurst = cancellationBurst;
    }

    public long getCompletionCount() {
        return completionCount.get();
    }

    long incrementCompletionCount() {
        return completionCount.incrementAndGet();
    }

    public long getCancellationCount() {
        return cancellationCount.get();
    }

    long incrementCancellationCount() {
        return cancellationCount.incrementAndGet();
    }

    public long getLimitReachedCount() {
        return limitReachedCount.get();
    }

    long incrementLimitReachedCount() {
        return limitReachedCount.incrementAndGet();
    }

    public AtomicReference<TokenBucket> getRateLimiter() {
        return rateLimiter;
    }

    public AtomicReference<TokenBucket> getRatioLimiter() {
        return ratioLimiter;
    }

    void onCancellationBurstChanged(double cancellationBurst) {
        this.cancellationBurst = cancellationBurst;
        onCancellationRateChanged(cancellationRate);
        onCancellationRatioChanged(cancellationRatio);
    }

    void onCancellationRateChanged(double cancellationRate) {
        this.cancellationRate = cancellationRate;
        rateLimiter.set(new TokenBucket(timeNanosSupplier, cancellationRate, cancellationBurst));
    }

    void onCancellationRatioChanged(double cancellationRatio) {
        this.cancellationRatio = cancellationRatio;
        ratioLimiter.set(new TokenBucket(this::getCompletionCount, cancellationRatio, cancellationBurst));
    }
}
