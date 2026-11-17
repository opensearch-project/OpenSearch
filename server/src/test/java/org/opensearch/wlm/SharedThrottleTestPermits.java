/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

/** Builds the acquire request a test would otherwise spell out, so tests can admit a permit in one call. */
final class SharedThrottleTestPermits {

    // A coordinator task id no test registers, so a task check reports the permit's search as finished.
    static final long NO_SUCH_TASK = Long.MAX_VALUE;

    private SharedThrottleTestPermits() {}

    static boolean tryAcquire(
        SharedThrottleTracker tracker,
        String bucketKey,
        int sharedLimit,
        String permitId,
        long ttlNanos,
        String coordinatorId
    ) {
        return tryAcquire(tracker, bucketKey, sharedLimit, permitId, ttlNanos, coordinatorId, NO_SUCH_TASK);
    }

    static boolean tryAcquire(
        SharedThrottleTracker tracker,
        String bucketKey,
        int sharedLimit,
        String permitId,
        long ttlNanos,
        String coordinatorId,
        long coordinatorTaskId
    ) {
        return tracker.tryAcquire(
            new WorkloadGroupSharedThrottleService.AcquirePermitRequest(
                bucketKey,
                sharedLimit,
                permitId,
                ttlNanos,
                coordinatorId,
                coordinatorTaskId
            )
        );
    }
}
