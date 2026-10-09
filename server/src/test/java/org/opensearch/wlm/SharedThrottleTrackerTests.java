/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SharedThrottleTrackerTests extends OpenSearchTestCase {

    private static final long TTL = TimeUnit.MINUTES.toNanos(5);
    private static final String COORD = "coordinator";

    public void testGrantsUpToLimitThenDenies() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 2, "l1", TTL, COORD));
        assertTrue(tracker.tryAcquire("b", 2, "l2", TTL, COORD));
        assertFalse("third acquire must be denied at limit 2", tracker.tryAcquire("b", 2, "l3", TTL, COORD));
        assertEquals(2, tracker.inFlight("b"));
    }

    public void testReleaseFreesASlot() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 1, "l1", TTL, COORD));
        assertFalse(tracker.tryAcquire("b", 1, "l2", TTL, COORD));
        tracker.release("b", "l1");
        assertEquals(0, tracker.inFlight("b"));
        assertTrue("slot freed after release", tracker.tryAcquire("b", 1, "l3", TTL, COORD));
    }

    public void testReleaseAllFromRemovesOnlyMatchingCoordinators() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 3, "gone-1", TTL, "gone"));
        assertTrue(tracker.tryAcquire("b", 3, "live-1", TTL, "live"));
        assertTrue(tracker.tryAcquire("b", 3, "unknown-1", TTL, SharedThrottleTracker.UNKNOWN_COORDINATOR));
        assertTrue(tracker.tryAcquire("c", 1, "gone-2", TTL, "gone"));
        assertTrue(tracker.tryAcquire("d", 1, "other-gone", TTL, "also-gone"));
        assertFalse("precondition: b is full", tracker.tryAcquire("b", 3, "x", TTL, "live"));

        assertEquals(3, tracker.releaseAllFrom(Set.of("gone", "also-gone")));
        assertEquals("b keeps the live and unknown-coordinator permits", 2, tracker.inFlight("b"));
        assertEquals(0, tracker.inFlight("c"));
        assertEquals(0, tracker.inFlight("d"));
        assertEquals("emptied buckets are dropped", 1, tracker.activeBuckets());
        assertTrue("the purge freed a slot", tracker.tryAcquire("b", 3, "after", TTL, "live"));
    }

    public void testReleaseAllFromNeverMatchesUnknownCoordinator() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 2, "unknown-1", TTL, SharedThrottleTracker.UNKNOWN_COORDINATOR));
        assertEquals(0, tracker.releaseAllFrom(Set.of(SharedThrottleTracker.UNKNOWN_COORDINATOR)));
        assertEquals(0, tracker.releaseAllFrom(Set.of()));
        assertEquals("a permit without a coordinator id is left to its TTL", 1, tracker.inFlight("b"));
    }

    public void testReleaseOfUnknownOrReleasedPermitIsNoOp() {
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 5, "live", 1_000, COORD)); // held throughout
        assertTrue(tracker.tryAcquire("b", 5, "l1", 1_000, COORD));
        assertTrue(tracker.tryAcquire("b", 5, "short", 100, COORD));

        tracker.release("b", "does-not-exist");
        tracker.release("unknown-bucket", "l1");
        assertEquals(3, tracker.inFlight("b"));

        tracker.release("b", "l1");
        tracker.release("b", "l1"); // double release
        assertEquals(2, tracker.inFlight("b"));

        clock.set(100);
        tracker.sweepExpired(); // sweeps "short"
        tracker.release("b", "short"); // late release of a swept permit
        assertEquals("a stale release must never decrement another request's permit", 1, tracker.inFlight("b"));
    }

    public void testDrainedBucketIsRemoved() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("b", 5, "l1", TTL, COORD));
        assertEquals(1, tracker.activeBuckets());
        tracker.release("b", "l1");
        assertEquals("bucket entry removed when it drains to zero", 0, tracker.activeBuckets());
    }

    public void testExpiredPermitIsReclaimedBySweep() {
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 1, "l1", 100, COORD));
        assertFalse(tracker.tryAcquire("b", 1, "l2", 100, COORD)); // at limit
        clock.set(101); // permit l1 has now expired
        tracker.sweepExpired();
        assertEquals(0, tracker.inFlight("b"));
        assertTrue("slot reclaimed after TTL sweep", tracker.tryAcquire("b", 1, "l3", 100, COORD));
    }

    public void testSweepSkipsFreshPermits() {
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 1, "l1", 100, COORD));
        tracker.sweepExpired();
        assertEquals(0, tracker.pruneScanCount());
        assertEquals(1, tracker.inFlight("b"));
        clock.set(100);
        tracker.sweepExpired();
        assertEquals(1, tracker.pruneScanCount());
        assertEquals(0, tracker.activeBuckets());
    }

    public void testSaturatedBucketWithExpiredPermitIsReclaimedOnAcquire() {
        // Full bucket with one expired permit: the acquire prunes it and keeps the live one.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100, COORD)); // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 500, COORD)); // expires at 500
        assertFalse("bucket is full", tracker.tryAcquire("b", 2, "l3", 100, COORD));
        clock.set(200); // l1 expired, l2 still live
        assertTrue(tracker.tryAcquire("b", 2, "l4", 500, COORD));
        assertEquals(2, tracker.inFlight("b"));
    }

    public void testConcurrentAcquireNeverExceedsLimit() throws Exception {
        final SharedThrottleTracker tracker = new SharedThrottleTracker();
        final int limit = 10;
        final int threads = 64;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger granted = new AtomicInteger(0);
        Thread[] workers = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            final int id = i;
            workers[i] = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (tracker.tryAcquire("hot", limit, "permit-" + id, TTL, COORD)) {
                    granted.incrementAndGet();
                }
            });
            workers[i].start();
        }
        start.countDown();
        for (Thread w : workers) {
            w.join();
        }
        assertEquals("exactly limit grants under contention", limit, granted.get());
        assertEquals(limit, tracker.inFlight("hot"));
    }

    public void testBucketsAreIndependent() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tracker.tryAcquire("a", 1, "la", TTL, COORD));
        assertTrue("different bucket has its own budget", tracker.tryAcquire("b", 1, "lb", TTL, COORD));
        assertFalse(tracker.tryAcquire("a", 1, "la2", TTL, COORD));
        assertEquals(2, tracker.activeBuckets());
    }

    public void testSaturatedBucketWithFreshPermitsSkipsPruneScan() {
        // While a full bucket's permits are all live, denied acquires must not scan.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100, COORD)); // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 100, COORD)); // expires at 100
        assertEquals("no scan needed to fill an empty bucket", 0, tracker.pruneScanCount());
        for (int i = 0; i < 50; i++) {
            assertFalse("full bucket denies while fresh", tracker.tryAcquire("b", 2, "denied-" + i, 100, COORD));
        }
        assertEquals("no expiry scan runs while every permit is still live", 0, tracker.pruneScanCount());
    }

    public void testSaturatedBucketScansOncePermitsCanExpire() {
        // Once a permit could have expired, a full-bucket acquire scans once, reclaims and admits.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100, COORD)); // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 100, COORD)); // expires at 100
        assertFalse(tracker.tryAcquire("b", 2, "l3", 100, COORD)); // denied while fresh, no scan
        assertEquals(0, tracker.pruneScanCount());
        clock.set(100); // both permits now at/past expiry
        assertTrue("expired permits reclaimed and slot granted", tracker.tryAcquire("b", 2, "l4", 100, COORD));
        assertEquals("exactly one scan ran once expiry was possible", 1, tracker.pruneScanCount());
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testReleaseLeavesMinExpiryStaleButReclamationStillCorrect() {
        // release() leaves minExpiry stale-small: that may cost an extra scan but must never skip a real reclaim.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "early", 100, COORD)); // expires at 100 -> minExpiry = 100
        assertTrue(tracker.tryAcquire("b", 2, "late", 1000, COORD)); // expires at 1000
        tracker.release("b", "early"); // removes the earliest permit; minExpiry stays a stale 100
        assertTrue(tracker.tryAcquire("b", 2, "l3", 1000, COORD)); // refill to the limit; "late" (1000) + "l3" (1000) live
        assertFalse(tracker.tryAcquire("b", 2, "l4", 1000, COORD)); // full again
        clock.set(500); // past the stale minExpiry (100) but before any real expiry (1000)
        assertFalse("nothing truly expired -> still denied", tracker.tryAcquire("b", 2, "l5", 1000, COORD));
        assertEquals(2, tracker.inFlight("b"));
        clock.set(1000); // now the real permits expire
        assertTrue("real expiry is reclaimed on the next acquire", tracker.tryAcquire("b", 2, "l6", 1000, COORD));
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testSweepRecomputesMinExpiryEnablingLaterSkip() {
        // The sweep recomputes minExpiry, so a full bucket skips scans again while survivors are fresh.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tracker.tryAcquire("b", 2, "l1", 100, COORD));  // expires at 100
        assertTrue(tracker.tryAcquire("b", 2, "l2", 1_000, COORD)); // expires at 1000
        clock.set(100); // l1 expired
        tracker.sweepExpired(); // prunes l1, recomputes minExpiry to l2's expiry (1000)
        assertEquals(1, tracker.pruneScanCount());
        assertTrue(tracker.tryAcquire("b", 2, "l3", 1_000, COORD)); // back to full; survivors expire at 1000
        for (int i = 0; i < 20; i++) {
            assertFalse(tracker.tryAcquire("b", 2, "denied-" + i, 1_000, COORD));
        }
        assertEquals("no further scans while survivors are fresh", 1, tracker.pruneScanCount());
    }
}
