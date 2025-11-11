/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ReleasableLock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A throttling class that can be activated, causing the
 * {@code acquireThrottle} method to block on a lock when throttling
 * is enabled
 *
 * @opensearch.internal
 */
public final class IndexThrottle {
    private final CounterMetric throttleTimeMillisMetric = new CounterMetric();
    private volatile long startOfThrottleNS;
    private static final ReleasableLock NOOP_LOCK = new ReleasableLock(new NoOpLock());
    private final ReleasableLock lockReference = new ReleasableLock(new ReentrantLock());
    private volatile ReleasableLock lock = NOOP_LOCK;

    public Releasable acquireThrottle() {
        return lock.acquire();
    }

    /** Activate throttling, which switches the lock to be a real lock */
    public void activate() {
        assert lock == NOOP_LOCK : "throttling activated while already active";
        startOfThrottleNS = System.nanoTime();
        lock = lockReference;
    }

    /** Deactivate throttling, which switches the lock to be an always-acquirable NoOpLock */
    public void deactivate() {
        assert lock != NOOP_LOCK : "throttling deactivated but not active";
        lock = NOOP_LOCK;

        assert startOfThrottleNS > 0 : "Bad state of startOfThrottleNS";
        long throttleTimeNS = System.nanoTime() - startOfThrottleNS;
        if (throttleTimeNS >= 0) {
            // Paranoia (System.nanoTime() is supposed to be monotonic): time slip may have occurred but never want
            // to add a negative number
            throttleTimeMillisMetric.inc(TimeValue.nsecToMSec(throttleTimeNS));
        }
    }

    public long getThrottleTimeInMillis() {
        long currentThrottleNS = 0;
        if (isThrottled() && startOfThrottleNS != 0) {
            currentThrottleNS += System.nanoTime() - startOfThrottleNS;
            if (currentThrottleNS < 0) {
                // Paranoia (System.nanoTime() is supposed to be monotonic): time slip must have happened, have to ignore this value
                currentThrottleNS = 0;
            }
        }
        return throttleTimeMillisMetric.count() + TimeValue.nsecToMSec(currentThrottleNS);
    }

    public boolean isThrottled() {
        return lock != NOOP_LOCK;
    }

    public boolean throttleLockIsHeldByCurrentThread() { // to be used in assertions and tests only
        if (isThrottled()) {
            return lock.isHeldByCurrentThread();
        }
        return false;
    }

    /**
     * A Lock implementation that always allows the lock to be acquired
     *
     * @opensearch.internal
     */
    static final class NoOpLock implements Lock {

        @Override
        public void lock() {}

        @Override
        public void lockInterruptibly() throws InterruptedException {}

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {}

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("NoOpLock can't provide a condition");
        }
    }
}
