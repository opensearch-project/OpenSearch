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
public class SearchBackpressureState implements CancellationSettingsListener {
    private final AtomicReference<TokenBucket> rateLimiter;
    private final AtomicReference<TokenBucket> ratioLimiter;
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
    private volatile double cancellationBurst;
    private volatile double cancellationRate;
    private volatile double cancellationRatio;

    SearchBackpressureState(
        LongSupplier timeNanosSupplier,
        double cancellationRateNanos,
        double cancellationBurst,
        double cancellationRatio,
        double cancellationRate
    ) {
        rateLimiter = new AtomicReference<>(new TokenBucket(timeNanosSupplier, cancellationRateNanos, cancellationBurst));
        ratioLimiter = new AtomicReference<>(new TokenBucket(this::getCompletionCount, cancellationRatio, cancellationBurst));
        this.timeNanosSupplier = timeNanosSupplier;
        this.cancellationBurst = cancellationBurst;
        this.cancellationRatio = cancellationRatio;
        this.cancellationRate = cancellationRate;
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

    public TokenBucket getRateLimiter() {
        return rateLimiter.get();
    }

    public TokenBucket getRatioLimiter() {
        return ratioLimiter.get();
    }

    @Override
    public void onRatioChanged(double ratio) {
        this.cancellationRatio = ratio;
        ratioLimiter.set(new TokenBucket(this::getCompletionCount, cancellationRatio, cancellationBurst));
    }

    @Override
    public void onRateChanged(double rate) {
        this.cancellationRate = rate;
        rateLimiter.set(new TokenBucket(timeNanosSupplier, cancellationRate, cancellationBurst));
    }

    @Override
    public void onBurstChanged(double burst) {
        this.cancellationBurst = burst;
        onRateChanged(cancellationRate);
        onRatioChanged(cancellationRatio);
    }
}
