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
 * Tracks the number of in-flight requests per throttle bucket on a single node and enforces a per-node cap.
 * <p>
 * A bucket is identified by an opaque key (see {@code WorkloadGroupService} for how the key is built from a
 * workload group and its throttle attribute). A counter exists only while a bucket has at least one in-flight
 * request: it is created on first acquire and removed when it drains back to zero, so memory scales with the
 * number of concurrently active buckets rather than the total population of users/roles.
 * <p>
 * This tier is fully local — no cross-node coordination — mirroring the acquire/rollback + {@link Releasable}
 * release shape of {@link org.opensearch.index.IndexingPressure}.
 */
@ExperimentalApi
public class WorkloadGroupThrottleTracker {

    private final Map<String, AtomicInteger> inFlightByBucket = new ConcurrentHashMap<>();

    /**
     * Attempts to admit one request into the given bucket under the per-node limit.
     * <p>
     * Returns {@code null} rather than throwing when the bucket is full: the caller decides what a breach means
     * (reject, or observe-only in MONITOR mode), and building a rejection exception here would mean allocating and
     * filling in a stack trace on the search hot path only to discard it in the observe-only case.
     *
     * @param bucketKey the throttle bucket identifier
     * @param nodeLimit the maximum concurrent in-flight requests this node may admit for the bucket
     * @return a {@link Releasable} that decrements the bucket's in-flight count exactly once when closed, or
     *         {@code null} if the bucket is already at the limit
     */
    public Releasable tryAcquire(String bucketKey, int nodeLimit) {
        // Create-and-increment inside compute() so the counter this acquire is about to use cannot be removed by a
        // concurrent release between lookup and increment (see release() for why removal is safe).
        // observed holds this thread's own post-increment count, captured under the map's per-key lock. Checking that
        // instead of a later counter.get() keeps the decision exact: a get() could see an unrelated concurrent
        // acquire's increment and reject a request that was actually within the limit.
        final int[] observed = new int[1];
        AtomicInteger counter = inFlightByBucket.compute(bucketKey, (k, existing) -> {
            AtomicInteger c = existing != null ? existing : new AtomicInteger(0);
            observed[0] = c.incrementAndGet();
            return c;
        });
        if (observed[0] > nodeLimit) {
            // Over the cap: roll back this increment and report the breach to the caller.
            release(bucketKey, counter);
            return null;
        }
        return releaseOnce(bucketKey, counter);
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

    // Wraps release in a one-shot guard so a double close (e.g. onRequestEnd and onRequestFailure) decrements once.
    private Releasable releaseOnce(String bucketKey, AtomicInteger counter) {
        AtomicBoolean released = new AtomicBoolean(false);
        return () -> {
            if (released.compareAndSet(false, true)) {
                release(bucketKey, counter);
            }
        };
    }

    // Decrements the bucket and removes the map entry once it drains to 0.
    //
    // The decrement deliberately sits outside the compute(), so decrement-and-remove is NOT atomic as a pair. What
    // makes removal safe is an invariant instead: decrements are 1:1 with prior increments (each permit closes at most
    // once, and the over-limit path rolls back its own increment), so the counter equals outstanding permits plus
    // pending rollbacks and is therefore >= 1 while any permit is held. Removal requires <= 0, so it can only happen
    // when no permit is outstanding and no acquire can be orphaned. A refactor that adds a second decrement site, or
    // that keys removal on identity rather than the <= 0 check, breaks this silently.
    private void release(String bucketKey, AtomicInteger counter) {
        counter.decrementAndGet();
        inFlightByBucket.compute(bucketKey, (k, existing) -> (existing != null && existing.get() <= 0) ? null : existing);
    }
}
