/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.lease.Releasable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Owner-side in-flight counter for {@code shared_limit} buckets, the cluster-wide sibling of
 * {@link WorkloadGroupThrottleTracker}. Acquire and release arrive as separate RPCs from a coordinator, so each grant is
 * a permit with an id and a TTL rather than a self-releasing {@link Releasable}.
 * <ul>
 *   <li><b>Atomic admission:</b> check-then-add runs inside {@link ConcurrentHashMap#compute}, so racing coordinators
 *       can never push a bucket over its limit.</li>
 *   <li><b>Count = size of the permit map</b>, so release and sweep are idempotent by permit id: a stale or repeated
 *       release can never decrement another request's permit.</li>
 *   <li><b>Departed coordinators:</b> each permit records its coordinator's ephemeral id, and {@link #releaseAllFrom}
 *       drops a departed coordinator's permits instead of leaving them until the TTL.</li>
 *   <li><b>TTL:</b> expired permits remain counted while their coordinator tasks are checked. Active tasks renew their
 *       permits; completed tasks free their slots. Legacy permits without task IDs retain age-based cleanup.</li>
 * </ul>
 * A bucket entry exists only while it holds a permit, so memory scales with concurrently active buckets.
 * <p>
 * Expired permits are found by an O(size) scan, which a saturated bucket skips while its cached earliest expiry
 * ({@code minExpiry}) is still in the future. Pending checks and short extensions after failed checks also skip scans, keeping denied acquires
 * O(1) under sustained load.
 */
final class SharedThrottleTracker {

    /**
     * Per-bucket state, only accessed under the outer map's per-key lock. {@code minExpiry} is a lower bound on the
     * earliest expiry: tightened on insert and check completion, recomputed on scan, and left stale on release, which can only cost one
     * extra scan, never skip a scan that would free a slot.
     */
    private static final class Bucket {
        final Map<String, Permit> permits = new ConcurrentHashMap<>();
        long minExpiry = Long.MAX_VALUE;
    }

    /** A retained permit; {@code expiresAt == Long.MAX_VALUE} means a task check is already pending. */
    private record Permit(long expiresAt, long ttlNanos, String coordinatorId, long coordinatorTaskId) {
    }

    /** An expired permit snapshot. Its identity fences late replies against release, purge, and subsequent checks. */
    static final class ExpiredPermit {
        final String bucketKey;
        final String permitId;
        private final Permit expected;

        private ExpiredPermit(String bucketKey, String permitId, Permit expected) {
            this.bucketKey = bucketKey;
            this.permitId = permitId;
            this.expected = expected;
        }

        String coordinatorId() {
            return expected.coordinatorId();
        }

        long coordinatorTaskId() {
            return expected.coordinatorTaskId();
        }
    }

    private static final class AcquireResult {
        boolean granted;
        List<ExpiredPermit> expired;
    }

    /** Coordinator id of a permit that must never be purged by {@link #releaseAllFrom} (the TTL still applies). */
    static final String UNKNOWN_COORDINATOR = "";
    static final long UNKNOWN_TASK_ID = -1;

    private final Map<String, Bucket> permitsByBucket = new ConcurrentHashMap<>();
    private final LongSupplier nanoTimeSupplier;
    private final Consumer<List<ExpiredPermit>> expiredPermitConsumer;

    // Expiry scans actually run; read by tests.
    private final LongAdder pruneScans = new LongAdder();

    SharedThrottleTracker() {
        this(System::nanoTime);
    }

    // Injectable clock for tests.
    SharedThrottleTracker(LongSupplier nanoTimeSupplier) {
        this(nanoTimeSupplier, expired -> {});
    }

    SharedThrottleTracker(LongSupplier nanoTimeSupplier, Consumer<List<ExpiredPermit>> expiredPermitConsumer) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.expiredPermitConsumer = expiredPermitConsumer;
    }

    /**
     * Admits one request into {@code bucketKey} if fewer than {@code sharedLimit} permits are live, recording the permit
     * against {@code coordinatorId} (an ephemeral node id, or {@link #UNKNOWN_COORDINATOR}) for {@link #releaseAllFrom}.
     *
     * @return {@code true} if granted, {@code false} if the bucket is at its limit
     */
    boolean tryAcquire(String bucketKey, int sharedLimit, String permitId, long ttlNanos, String coordinatorId) {
        return tryAcquire(bucketKey, sharedLimit, permitId, ttlNanos, coordinatorId, UNKNOWN_TASK_ID);
    }

    boolean tryAcquire(String bucketKey, int sharedLimit, String permitId, long ttlNanos, String coordinatorId, long coordinatorTaskId) {
        final long now = nanoTimeSupplier.getAsLong();
        final long expiresAt = now + ttlNanos;
        final Permit permit = new Permit(expiresAt, ttlNanos, coordinatorId, coordinatorTaskId);
        final AcquireResult result = new AcquireResult();
        permitsByBucket.compute(bucketKey, (k, bucket) -> {
            if (bucket == null) {
                bucket = new Bucket();
            }
            // Pending checks stay counted, and their Long.MAX_VALUE check time prevents repeated scans.
            if (bucket.permits.size() >= sharedLimit && bucket.minExpiry <= now) {
                result.expired = collectExpired(bucketKey, bucket, now);
            }
            if (bucket.permits.size() < sharedLimit) {
                bucket.permits.put(permitId, permit);
                if (expiresAt < bucket.minExpiry) {
                    bucket.minExpiry = expiresAt;
                }
                result.granted = true;
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
        if (result.expired != null && result.expired.isEmpty() == false) {
            expiredPermitConsumer.accept(result.expired);
        }
        return result.granted;
    }

    /** Releases a permit by id; a no-op for an unknown or already-released id. */
    void release(String bucketKey, String permitId) {
        permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
            bucket.permits.remove(permitId);
            // minExpiry intentionally left as is (see Bucket).
            return bucket.permits.isEmpty() ? null : bucket;
        });
    }

    /**
     * Releases every permit held by one of {@code coordinatorIds} (ephemeral node ids of coordinators that left the
     * cluster), across all buckets. Permits recorded with {@link #UNKNOWN_COORDINATOR} are never matched. An O(total
     * permits) scan, so only call it on node removal.
     *
     * @return the number of permits released
     */
    int releaseAllFrom(Set<String> coordinatorIds) {
        if (coordinatorIds.isEmpty()) {
            return 0;
        }
        final int[] released = new int[1];
        for (String bucketKey : permitsByBucket.keySet()) {
            permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                for (Iterator<Permit> it = bucket.permits.values().iterator(); it.hasNext();) {
                    final String coordinatorId = it.next().coordinatorId();
                    if (UNKNOWN_COORDINATOR.equals(coordinatorId) == false && coordinatorIds.contains(coordinatorId)) {
                        it.remove();
                        released[0]++;
                    }
                }
                return bucket.permits.isEmpty() ? null : bucket;
            });
        }
        return released[0];
    }

    /** Collects due task checks and reclaims expired legacy permits across all buckets. */
    void sweepExpired() {
        final long now = nanoTimeSupplier.getAsLong();
        final List<ExpiredPermit> expired = new ArrayList<>();
        for (String bucketKey : permitsByBucket.keySet()) {
            permitsByBucket.computeIfPresent(bucketKey, (k, bucket) -> {
                if (bucket.minExpiry <= now) {
                    expired.addAll(collectExpired(bucketKey, bucket, now));
                }
                return bucket.permits.isEmpty() ? null : bucket;
            });
        }
        if (expired.isEmpty() == false) {
            expiredPermitConsumer.accept(expired);
        }
    }

    /** Applies a task check only while its exact pending permit record remains present. */
    void completeCheck(ExpiredPermit expired, boolean active) {
        if (active) {
            renew(expired, expired.expected.ttlNanos());
            return;
        }
        permitsByBucket.computeIfPresent(expired.bucketKey, (k, bucket) -> {
            if (bucket.permits.get(expired.permitId) == expired.expected) {
                bucket.permits.remove(expired.permitId);
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
    }

    /** A failed check receives a short extension; the next active reply restores the original TTL. */
    void renewForRetry(ExpiredPermit expired, long retryTtlNanos) {
        renew(expired, retryTtlNanos);
    }

    private void renew(ExpiredPermit expired, long extensionNanos) {
        final long expiresAt = nanoTimeSupplier.getAsLong() + extensionNanos;
        permitsByBucket.computeIfPresent(expired.bucketKey, (k, bucket) -> {
            if (bucket.permits.get(expired.permitId) == expired.expected) {
                final Permit previous = expired.expected;
                bucket.permits.put(
                    expired.permitId,
                    new Permit(expiresAt, previous.ttlNanos(), previous.coordinatorId(), previous.coordinatorTaskId())
                );
                bucket.minExpiry = Math.min(bucket.minExpiry, expiresAt);
            }
            return bucket.permits.isEmpty() ? null : bucket;
        });
    }

    // Raw number of permit records for a bucket, not filtered by expiry.
    int inFlight(String bucketKey) {
        Bucket bucket = permitsByBucket.get(bucketKey);
        return bucket == null ? 0 : bucket.permits.size();
    }

    // Visible for tests.
    int activeBuckets() {
        return permitsByBucket.size();
    }

    // Visible for tests.
    long pruneScanCount() {
        return pruneScans.sum();
    }

    // Runs under the bucket lock; callbacks run after compute returns, so local checks cannot re-enter it.
    private List<ExpiredPermit> collectExpired(String bucketKey, Bucket bucket, long now) {
        pruneScans.increment();
        final List<ExpiredPermit> expired = new ArrayList<>();
        long min = Long.MAX_VALUE;
        for (Iterator<Entry<String, Permit>> it = bucket.permits.entrySet().iterator(); it.hasNext();) {
            final Entry<String, Permit> entry = it.next();
            Permit permit = entry.getValue();
            if (permit.expiresAt() <= now) {
                if (permit.coordinatorTaskId() == UNKNOWN_TASK_ID || UNKNOWN_COORDINATOR.equals(permit.coordinatorId())) {
                    it.remove();
                    continue;
                }
                permit = new Permit(Long.MAX_VALUE, permit.ttlNanos(), permit.coordinatorId(), permit.coordinatorTaskId());
                entry.setValue(permit);
                expired.add(new ExpiredPermit(bucketKey, entry.getKey(), permit));
            }
            min = Math.min(min, permit.expiresAt());
        }
        bucket.minExpiry = min;
        return expired;
    }
}
