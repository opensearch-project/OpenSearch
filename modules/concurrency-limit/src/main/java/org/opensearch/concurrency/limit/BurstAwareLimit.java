/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency.limit;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.limit.AbstractLimit;

/**
 * A limit wrapper that adds a fixed burst capacity above the delegate algorithm's adaptive base limit.
 * <p>
 * The burst window starts open: effective semaphore size = {@code delegate.getLimit() + burstCapacity}.
 * It closes after {@code closeThreshold} consecutive samples where {@code inflight >= delegate.getLimit()},
 * and reopens after {@code recoveryThreshold} consecutive samples where {@code inflight < delegate.getLimit()}.
 * Any sample that breaks the streak resets the active counter.
 * <p>
 * Inflight is clamped to {@code delegate.getLimit()} before passing to the delegate so that
 * burst-range requests do not distort the algorithm's queue-size estimate.
 */
public final class BurstAwareLimit implements Limit {

    private final AbstractLimit delegate;
    private final int burstCapacity;
    private final int closeThreshold;
    private final int recoveryThreshold;

    private volatile int limit;
    private final List<Consumer<Integer>> listeners = new CopyOnWriteArrayList<>();

    private int consecutiveOverBase = 0;
    private int consecutiveBelowBase = 0;
    private boolean burstOpen = true;

    /**
     * Creates a new burst-aware limit wrapping the given delegate.
     *
     * @param delegate the adaptive limit algorithm
     * @param burstCapacity extra capacity when burst window is open
     * @param closeThreshold consecutive over-base samples to close the burst window
     * @param recoveryThreshold consecutive below-base samples to reopen the burst window
     */
    public BurstAwareLimit(AbstractLimit delegate, int burstCapacity, int closeThreshold, int recoveryThreshold) {
        this.delegate = delegate;
        this.burstCapacity = burstCapacity;
        this.closeThreshold = closeThreshold;
        this.recoveryThreshold = recoveryThreshold;
        this.limit = delegate.getLimit() + burstCapacity;
        delegate.notifyOnChange(newBase -> {
            int newEffective = newBase + (burstOpen ? burstCapacity : 0);
            updateLimit(newEffective);
        });
    }

    /** Returns the wrapped delegate limit algorithm. */
    public AbstractLimit getDelegate() {
        return delegate;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public void notifyOnChange(Consumer<Integer> consumer) {
        listeners.add(consumer);
    }

    @Override
    public synchronized void onSample(long startTime, long rtt, int inflight, boolean dropped) {
        int clamped = Math.min(inflight, delegate.getLimit());
        delegate.onSample(startTime, rtt, clamped, dropped);

        if (burstOpen) {
            if (inflight >= delegate.getLimit()) {
                if (++consecutiveOverBase >= closeThreshold) {
                    burstOpen = false;
                    consecutiveOverBase = 0;
                    updateLimit(delegate.getLimit());
                }
            } else {
                consecutiveOverBase = 0;
            }
        } else {
            if (inflight < delegate.getLimit()) {
                if (++consecutiveBelowBase >= recoveryThreshold) {
                    burstOpen = true;
                    consecutiveBelowBase = 0;
                    updateLimit(delegate.getLimit() + burstCapacity);
                }
            } else {
                consecutiveBelowBase = 0;
            }
        }
    }

    private void updateLimit(int newLimit) {
        if (newLimit != limit) {
            limit = newLimit;
            listeners.forEach(l -> l.accept(newLimit));
        }
    }
}
