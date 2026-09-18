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
 * This tier is fully local — no cross-node coordination — and hands admission back as a {@link Releasable}, in the shape
 * of {@link org.opensearch.index.IndexingPressure}. Unlike that class, which guards a single global counter and so has to
 * increment first and roll back on a breach, the per-key {@link ConcurrentHashMap#compute} region here makes the limit
 * check and the increment one atomic transition, so a refused acquire never publishes occupancy it is about to undo.
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
        // Check the limit and increment in ONE per-key atomic region. compute() serialises every acquire and release for a
        // key, so the count the decision reads is exactly the number of outstanding permits.
        //
        // Deciding outside the region instead -- increment, compare, undo on a breach -- would publish a count that
        // momentarily includes an increment this thread is about to roll back. A concurrent acquire could then be refused
        // against a slot that was in truth already free: with node_limit=1, if a refused acquire is paused between its
        // increment and its rollback, the real permit holder can release (dropping the count to that pending rollback
        // alone) and the next request is rejected against occupancy nobody holds. Reading under the lock also removes the
        // need to capture a post-increment value out of the region, which a later counter.get() could not replace -- that
        // would see unrelated concurrent acquires and reject a request that was within the limit.
        //
        // admitted is set only on the path that takes a slot, so it doubles as the admit/refuse signal.
        final AtomicInteger[] admitted = new AtomicInteger[1];
        inFlightByBucket.compute(bucketKey, (k, existing) -> {
            if (existing == null) {
                if (nodeLimit < 1) {
                    // Refuse without materialising an entry, so a nonsensical limit cannot churn the map on the search path.
                    return null;
                }
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
    // The decrement sits inside the same per-key compute() region that tryAcquire decides in, which is what makes removal
    // safe: an entry is only removed when the count reaches 0 under the lock, and that requires every permit for the bucket
    // to have been released. So a counter can never be evicted from under an outstanding permit, and the counter equals the
    // number of outstanding permits exactly -- there is no "plus pending rollbacks" slack for another thread to observe.
    //
    // This is the ONLY decrement site. Adding a second one, or moving this decrement outside the compute(), reintroduces a
    // count that can be read mid-flight and silently makes the tracker refuse free slots.
    private void release(String bucketKey, AtomicInteger counter) {
        inFlightByBucket.compute(bucketKey, (k, existing) -> {
            assert existing == counter : "released a counter no longer mapped for bucket [" + bucketKey + "]";
            return counter.decrementAndGet() <= 0 ? null : existing;
        });
    }
}
