/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.SharedThrottleTracker.ExpiredPermit;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.wlm.SharedThrottleTestPermits.tryAcquire;

public class SharedThrottleTrackerTests extends OpenSearchTestCase {

    private static final long TTL = TimeUnit.MINUTES.toNanos(5);
    private static final String COORD = "coordinator";

    public void testGrantsUpToLimitThenDenies() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tryAcquire(tracker, "b", 2, "l1", TTL, COORD));
        assertTrue(tryAcquire(tracker, "b", 2, "l2", TTL, COORD));
        assertFalse("third acquire must be denied at limit 2", tryAcquire(tracker, "b", 2, "l3", TTL, COORD));
        assertEquals(2, tracker.inFlight("b"));
    }

    public void testReleaseFreesASlot() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tryAcquire(tracker, "b", 1, "l1", TTL, COORD));
        assertFalse(tryAcquire(tracker, "b", 1, "l2", TTL, COORD));
        tracker.release("b", "l1");
        assertEquals(0, tracker.inFlight("b"));
        assertTrue("slot freed after release", tryAcquire(tracker, "b", 1, "l3", TTL, COORD));
    }

    public void testReleaseAllFromRemovesOnlyMatchingCoordinators() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertEquals(0, tracker.releaseAllFrom(Set.of()));
        assertTrue(tryAcquire(tracker, "b", 3, "gone-1", TTL, "gone"));
        assertTrue(tryAcquire(tracker, "b", 3, "live-1", TTL, "live"));
        assertTrue(tryAcquire(tracker, "b", 3, "other-1", TTL, "other-live"));
        assertTrue(tryAcquire(tracker, "c", 1, "gone-2", TTL, "gone"));
        assertTrue(tryAcquire(tracker, "d", 1, "other-gone", TTL, "also-gone"));
        assertFalse("precondition: b is full", tryAcquire(tracker, "b", 3, "x", TTL, "live"));

        assertEquals(3, tracker.releaseAllFrom(Set.of("gone", "also-gone")));
        assertEquals("b keeps the permits of coordinators still present", 2, tracker.inFlight("b"));
        assertEquals(0, tracker.inFlight("c"));
        assertEquals(0, tracker.inFlight("d"));
        assertEquals("emptied buckets are dropped", 1, tracker.activeBuckets());
        assertTrue("the purge freed a slot", tryAcquire(tracker, "b", 3, "after", TTL, "live"));
    }

    public void testReleaseOfUnknownOrReleasedPermitIsNoOp() {
        AtomicLong clock = new AtomicLong(0);
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 5, "live", 1_000, COORD)); // held throughout
        assertTrue(tryAcquire(tracker, "b", 5, "l1", 1_000, COORD));
        assertTrue(tryAcquire(tracker, "b", 5, "short", 100, COORD));

        tracker.release("b", "does-not-exist");
        tracker.release("unknown-bucket", "l1");
        assertEquals(3, tracker.inFlight("b"));

        tracker.release("b", "l1");
        tracker.release("b", "l1"); // double release
        assertEquals(2, tracker.inFlight("b"));

        clock.set(100);
        tracker.sweepExpired(); // "short" expires; its task check reports the search finished
        checks.forEach(expired -> tracker.completeCheck(expired, false));
        tracker.release("b", "short"); // late release of a reclaimed permit
        assertEquals("a stale release must never decrement another request's permit", 1, tracker.inFlight("b"));
    }

    public void testDrainedBucketIsRemoved() {
        SharedThrottleTracker tracker = new SharedThrottleTracker();
        assertTrue(tryAcquire(tracker, "b", 5, "l1", TTL, COORD));
        assertEquals(1, tracker.activeBuckets());
        tracker.release("b", "l1");
        assertEquals("bucket entry removed when it drains to zero", 0, tracker.activeBuckets());
    }

    public void testExpiredPermitIsReclaimedBySweep() {
        AtomicLong clock = new AtomicLong(0);
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "l1", 100, COORD));
        assertFalse(tryAcquire(tracker, "b", 1, "l2", 100, COORD)); // at limit
        clock.set(101); // permit l1 has now expired
        tracker.sweepExpired();
        assertEquals("an expired permit stays counted until its task check completes", 1, tracker.inFlight("b"));
        assertEquals(1, checks.size());
        tracker.completeCheck(checks.get(0), false);
        assertEquals(0, tracker.inFlight("b"));
        assertTrue("slot reclaimed after TTL sweep", tryAcquire(tracker, "b", 1, "l3", 100, COORD));
    }

    public void testSweepSkipsFreshPermits() {
        AtomicLong clock = new AtomicLong(0);
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "l1", 100, COORD));
        tracker.sweepExpired();
        assertEquals(0, tracker.pruneScanCount());
        assertTrue(checks.isEmpty());
        assertEquals(1, tracker.inFlight("b"));
        clock.set(100);
        tracker.sweepExpired();
        assertEquals(1, tracker.pruneScanCount());
        assertEquals(1, checks.size());
        tracker.completeCheck(checks.get(0), false);
        assertEquals(0, tracker.activeBuckets());
    }

    public void testSaturatedBucketWithExpiredPermitIsReclaimedOnAcquire() {
        // Full bucket with one expired permit: the acquire checks only that one, and its finished task frees the slot.
        AtomicLong clock = new AtomicLong(0);
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 2, "l1", 100, COORD)); // expires at 100
        assertTrue(tryAcquire(tracker, "b", 2, "l2", 500, COORD)); // expires at 500
        assertFalse("bucket is full", tryAcquire(tracker, "b", 2, "l3", 100, COORD));
        clock.set(200); // l1 expired, l2 still live
        assertFalse("the expired permit stays counted while it is checked", tryAcquire(tracker, "b", 2, "l4", 500, COORD));
        assertEquals(1, checks.size());
        assertEquals("l1", checks.get(0).permitId);
        tracker.completeCheck(checks.get(0), false);
        assertTrue(tryAcquire(tracker, "b", 2, "l5", 500, COORD));
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
                if (tryAcquire(tracker, "hot", limit, "permit-" + id, TTL, COORD)) {
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
        assertTrue(tryAcquire(tracker, "a", 1, "la", TTL, COORD));
        assertTrue("different bucket has its own budget", tryAcquire(tracker, "b", 1, "lb", TTL, COORD));
        assertFalse(tryAcquire(tracker, "a", 1, "la2", TTL, COORD));
        assertEquals(2, tracker.activeBuckets());
    }

    public void testSaturatedBucketWithFreshPermitsSkipsPruneScan() {
        // While a full bucket's permits are all live, denied acquires must not scan.
        AtomicLong clock = new AtomicLong(0);
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get);
        assertTrue(tryAcquire(tracker, "b", 2, "l1", 100, COORD)); // expires at 100
        assertTrue(tryAcquire(tracker, "b", 2, "l2", 100, COORD)); // expires at 100
        assertEquals("no scan needed to fill an empty bucket", 0, tracker.pruneScanCount());
        for (int i = 0; i < 50; i++) {
            assertFalse("full bucket denies while fresh", tryAcquire(tracker, "b", 2, "denied-" + i, 100, COORD));
        }
        assertEquals("no expiry scan runs while every permit is still live", 0, tracker.pruneScanCount());
    }

    public void testSaturatedBucketScansOncePermitsCanExpire() {
        // Once a permit could have expired, a full-bucket acquire scans once and queues checks; pending checks stop rescans.
        AtomicLong clock = new AtomicLong(0);
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 2, "l1", 100, COORD)); // expires at 100
        assertTrue(tryAcquire(tracker, "b", 2, "l2", 100, COORD)); // expires at 100
        assertFalse(tryAcquire(tracker, "b", 2, "l3", 100, COORD)); // denied while fresh, no scan
        assertEquals(0, tracker.pruneScanCount());
        clock.set(100); // both permits now at/past expiry
        assertFalse("expired permits stay counted while checked", tryAcquire(tracker, "b", 2, "l4", 100, COORD));
        assertFalse(tryAcquire(tracker, "b", 2, "l5", 100, COORD));
        assertEquals("exactly one scan ran once expiry was possible", 1, tracker.pruneScanCount());
        assertEquals(2, checks.size());
        checks.forEach(expired -> tracker.completeCheck(expired, false));
        assertTrue("finished tasks free their slots", tryAcquire(tracker, "b", 2, "l6", 100, COORD));
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testReleaseLeavesMinExpiryStaleButReclamationStillCorrect() {
        // release() leaves minExpiry stale-small: that may cost an extra scan but must never skip a real reclaim.
        AtomicLong clock = new AtomicLong(0);
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 2, "early", 100, COORD)); // expires at 100 -> minExpiry = 100
        assertTrue(tryAcquire(tracker, "b", 2, "late", 1000, COORD)); // expires at 1000
        tracker.release("b", "early"); // removes the earliest permit; minExpiry stays a stale 100
        assertTrue(tryAcquire(tracker, "b", 2, "l3", 1000, COORD)); // refill to the limit; "late" (1000) + "l3" (1000) live
        assertFalse(tryAcquire(tracker, "b", 2, "l4", 1000, COORD)); // full again
        clock.set(500); // past the stale minExpiry (100) but before any real expiry (1000)
        assertFalse("nothing truly expired -> still denied", tryAcquire(tracker, "b", 2, "l5", 1000, COORD));
        assertEquals(2, tracker.inFlight("b"));
        assertEquals("the stale minimum costs one scan that finds nothing", 1, tracker.pruneScanCount());
        assertTrue(checks.isEmpty());
        clock.set(1000); // now the real permits expire
        assertFalse(tryAcquire(tracker, "b", 2, "l6", 1000, COORD));
        assertEquals("real expiry is found on the next acquire", 2, checks.size());
        checks.forEach(expired -> tracker.completeCheck(expired, false));
        assertTrue(tryAcquire(tracker, "b", 2, "l7", 1000, COORD));
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testSweepRecomputesMinExpiryEnablingLaterSkip() {
        // The sweep recomputes minExpiry, so a full bucket skips scans again while survivors are fresh.
        AtomicLong clock = new AtomicLong(0);
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 2, "l1", 100, COORD));  // expires at 100
        assertTrue(tryAcquire(tracker, "b", 2, "l2", 1_000, COORD)); // expires at 1000
        clock.set(100); // l1 expired
        tracker.sweepExpired(); // queues l1's check, recomputes minExpiry to l2's expiry (1000)
        assertEquals(1, tracker.pruneScanCount());
        tracker.completeCheck(checks.get(0), false); // l1's search finished
        assertTrue(tryAcquire(tracker, "b", 2, "l3", 1_000, COORD)); // back to full; survivors expire at 1000
        for (int i = 0; i < 20; i++) {
            assertFalse(tryAcquire(tracker, "b", 2, "denied-" + i, 1_000, COORD));
        }
        assertEquals("no further scans while survivors are fresh", 1, tracker.pruneScanCount());
    }

    public void testFreshTrackedPermitsDoNotTriggerChecks() {
        AtomicLong clock = new AtomicLong();
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 42));
        clock.set(99);
        tracker.sweepExpired();
        for (int i = 0; i < 50; i++) {
            assertFalse(tryAcquire(tracker, "b", 1, "denied-" + i, 100, COORD, 43));
        }
        assertTrue(checks.isEmpty());
        assertEquals(0, tracker.pruneScanCount());
        tracker.release("b", "held");
        clock.set(100);
        tracker.sweepExpired();
        assertTrue("released short requests never need a task check", checks.isEmpty());
    }

    public void testExpiredTrackedPermitStaysCountedDuringAdmissionCheck() {
        AtomicLong clock = new AtomicLong();
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 42));
        clock.set(100);
        assertFalse(tryAcquire(tracker, "b", 1, "overflow", 100, COORD, 43));
        assertEquals(1, tracker.inFlight("b"));
        assertEquals(1, checks.size());
        assertEquals(COORD, checks.get(0).coordinatorId());
        assertEquals(42, checks.get(0).coordinatorTaskId());
        for (int i = 0; i < 50; i++) {
            assertFalse(tryAcquire(tracker, "b", 1, "denied-" + i, 100, COORD, 43));
            tracker.sweepExpired();
        }
        assertEquals("a pending check is never duplicated", 1, checks.size());
        assertEquals("pending checks keep the saturated path constant time", 1, tracker.pruneScanCount());

        tracker.completeCheck(checks.get(0), true);
        clock.set(199);
        tracker.sweepExpired();
        assertEquals("an active task gets its full original TTL", 1, checks.size());
        clock.set(200);
        tracker.sweepExpired();
        assertEquals(2, checks.size());
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testSweepBatchesTrackedPermitsAndKeepsTheirCharges() {
        AtomicLong clock = new AtomicLong();
        List<List<ExpiredPermit>> batches = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, batches::add);
        assertTrue(tryAcquire(tracker, "a", 1, "first", 100, COORD, 42));
        assertTrue(tryAcquire(tracker, "b", 1, "second", 100, COORD, 43));
        clock.set(100);
        tracker.sweepExpired();
        assertEquals(1, batches.size());
        assertEquals(2, batches.get(0).size());
        assertEquals(1, tracker.inFlight("a"));
        assertEquals(1, tracker.inFlight("b"));
    }

    public void testCompletedTaskCheckFreesCapacity() {
        AtomicLong clock = new AtomicLong();
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 42));
        clock.set(100);
        tracker.sweepExpired();
        assertEquals(1, tracker.inFlight("b"));
        tracker.completeCheck(checks.get(0), false);
        assertEquals(0, tracker.activeBuckets());
        assertTrue(tryAcquire(tracker, "b", 1, "replacement", 100, COORD, 43));
    }

    public void testFailedCheckGetsShortExtensionThenActiveCheckRestoresFullTtl() {
        AtomicLong clock = new AtomicLong();
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 42));
        clock.set(100);
        tracker.sweepExpired();
        ExpiredPermit first = checks.get(0);
        tracker.renewForRetry(first, 10);
        clock.set(109);
        for (int i = 0; i < 50; i++) {
            assertFalse(tryAcquire(tracker, "b", 1, "denied-" + i, 100, COORD, 43));
        }
        assertEquals(1, checks.size());
        assertEquals("the short extension prevents repeated scans", 1, tracker.pruneScanCount());
        clock.set(110);
        tracker.sweepExpired();
        assertEquals("failed checks retry at the short expiry", 2, checks.size());
        tracker.completeCheck(first, false);
        assertEquals("a late reply to the failed check cannot remove the current permit", 1, tracker.inFlight("b"));
        tracker.completeCheck(checks.get(1), true);
        clock.set(209);
        tracker.sweepExpired();
        assertEquals(2, checks.size());
        clock.set(210);
        tracker.sweepExpired();
        assertEquals("an active reply restores 100, not the failed check's 10", 3, checks.size());
    }

    public void testLateActiveReplyCannotResurrectReleasedPermit() {
        AtomicLong clock = new AtomicLong();
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 42));
        clock.set(100);
        tracker.sweepExpired();
        tracker.release("b", "held");
        tracker.completeCheck(checks.get(0), true);
        tracker.renewForRetry(checks.get(0), 10);
        assertEquals(0, tracker.activeBuckets());
    }

    public void testLateReplyCannotChangeReusedPermitId() {
        AtomicLong clock = new AtomicLong();
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 42));
        clock.set(100);
        tracker.sweepExpired();
        tracker.release("b", "held");
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 43));
        tracker.completeCheck(checks.get(0), false);
        tracker.completeCheck(checks.get(0), true);
        assertEquals(1, tracker.inFlight("b"));
        clock.set(200);
        tracker.sweepExpired();
        assertEquals(2, checks.size());
        assertEquals(43, checks.get(1).coordinatorTaskId());
    }

    public void testCoordinatorPurgeFencesPendingChecks() {
        AtomicLong clock = new AtomicLong();
        List<ExpiredPermit> checks = new ArrayList<>();
        SharedThrottleTracker tracker = new SharedThrottleTracker(clock::get, checks::addAll);
        assertTrue(tryAcquire(tracker, "b", 1, "held", 100, COORD, 42));
        clock.set(100);
        tracker.sweepExpired();
        assertEquals(1, tracker.releaseAllFrom(Set.of(COORD)));
        tracker.completeCheck(checks.get(0), true);
        assertEquals(0, tracker.activeBuckets());
    }
}
