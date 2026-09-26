/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lease.Releasable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks in-flight requests per throttle bucket on a single node and enforces a per-node cap. A bucket's counter exists
 * only while it has at least one in-flight request (created on first acquire, removed when it drains to zero), so memory
 * scales with active buckets rather than the total population of users/roles. Fully local (no cross-node coordination);
 * the per-key {@link ConcurrentHashMap#compute} region makes the limit check and increment one atomic transition, so a
 * refused acquire never publishes occupancy it is about to undo.
 */
@ExperimentalApi
public class WorkloadGroupThrottleTracker {

    private final Map<String, AtomicInteger> inFlightByBucket = new ConcurrentHashMap<>();

    /**
     * Attempts to admit one request into the bucket under the per-node limit. Returns {@code null} rather than throwing
     * when full, so the caller decides what a breach means (reject, or observe-only in MONITOR) without allocating a
     * rejection stack trace on the search hot path.
     *
     * @param bucketKey the throttle bucket identifier
     * @param nodeLimit the positive maximum number of concurrent in-flight requests this node may admit for the bucket
     * @return a {@link Releasable} that decrements the bucket's in-flight count exactly once when closed, or
     *         {@code null} if the bucket is already at the limit
     */
    public Releasable tryAcquire(String bucketKey, int nodeLimit) {
        // Check-and-increment in one per-key compute() region so the decision reads exactly the outstanding-permit count.
        // Doing it outside (increment, compare, roll back on a breach) would briefly publish a count including a pending
        // rollback, letting a concurrent acquire be refused against a slot that was actually free. admitted is set only on
        // the path that takes a slot, so it doubles as the admit/refuse signal.
        final AtomicInteger[] admitted = new AtomicInteger[1];
        inFlightByBucket.compute(bucketKey, (k, existing) -> {
            if (existing == null) {
                admitted[0] = new AtomicInteger(1);
                return admitted[0];
            }
            if (existing.get() >= nodeLimit) {
                return existing; // at the cap: the count is left untouched, so there is nothing to roll back
            }
            existing.incrementAndGet();
            admitted[0] = existing;
            return existing;
        });
        return admitted[0] == null ? null : releaseOnce(bucketKey, admitted[0]);
    }

    /**
     * Current in-flight count for a bucket, or 0 if the bucket has no active requests. Package-private for tests.
     * Note this returns 0 both for an absent bucket and for one that is present with a zero count; use
     * {@link #bucketCount()} to distinguish them.
     */
    int inFlight(String bucketKey) {
        AtomicInteger counter = inFlightByBucket.get(bucketKey);
        return counter == null ? 0 : counter.get();
    }

    /**
     * Number of buckets currently holding a counter. Package-private for tests, which use it to assert that a bucket
     * is actually evicted once it drains rather than merely reading back as zero.
     */
    int bucketCount() {
        return inFlightByBucket.size();
    }

    // One-shot guard so a double close decrements once -- defence in depth against a listener notified more than once.
    private Releasable releaseOnce(String bucketKey, AtomicInteger counter) {
        AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                release(bucketKey, counter);
            }
        };
    }

    // Decrements and evicts the entry when it drains to 0, inside the same compute() region tryAcquire decides in, so a
    // counter is never evicted from under an outstanding permit. Must stay the only decrement site and stay inside
    // compute(), or the count could be read mid-flight and free slots wrongly refused.
    private void release(String bucketKey, AtomicInteger counter) {
        inFlightByBucket.compute(bucketKey, (k, existing) -> {
            assert existing == counter : "released a counter no longer mapped for bucket [" + bucketKey + "]";
            return counter.decrementAndGet() <= 0 ? null : existing;
        });
    }
}
