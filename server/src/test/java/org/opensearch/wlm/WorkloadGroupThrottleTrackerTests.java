/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.lease.Releasable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkloadGroupThrottleTrackerTests extends OpenSearchTestCase {

    public void testAcquireUnderLimitSucceeds() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        Releasable p1 = tracker.tryAcquire("bucket", 2);
        Releasable p2 = tracker.tryAcquire("bucket", 2);
        assertNotNull(p1);
        assertNotNull(p2);
        assertEquals(2, tracker.inFlight("bucket"));
        p1.close();
        p2.close();
    }

    public void testAcquireAtLimitIsRefused() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        assertNotNull(tracker.tryAcquire("bucket", 1));
        // Over the cap the tracker reports the breach by returning null; building the 429 is the caller's job.
        assertNull(tracker.tryAcquire("bucket", 1));
        // a refused acquire must not leave the count inflated
        assertEquals(1, tracker.inFlight("bucket"));
    }

    public void testExactlyNAdmittedForLimitAboveOne() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        int limit = randomIntBetween(2, 5);
        List<Releasable> permits = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            Releasable p = tracker.tryAcquire("bucket", limit);
            assertNotNull("acquire " + i + " of " + limit + " must be admitted", p);
            permits.add(p);
        }
        assertEquals(limit, tracker.inFlight("bucket"));
        assertNull("the limit+1'th acquire must be refused", tracker.tryAcquire("bucket", limit));
        assertEquals(limit, tracker.inFlight("bucket"));
        permits.forEach(Releasable::close);
        assertEquals(0, tracker.bucketCount());
    }

    public void testReleaseFreesAPermit() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        Releasable p = tracker.tryAcquire("bucket", 1);
        assertNull(tracker.tryAcquire("bucket", 1)); // at the limit
        p.close();
        // permit freed -> a fresh acquire now succeeds
        Releasable p2 = tracker.tryAcquire("bucket", 1);
        assertNotNull(p2);
        assertEquals(1, tracker.inFlight("bucket"));
        p2.close();
    }

    public void testDrainToZeroRemovesBucketThenReacquire() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        Releasable p = tracker.tryAcquire("bucket", 5);
        assertEquals(1, tracker.inFlight("bucket"));
        assertEquals(1, tracker.bucketCount());
        p.close();
        // bucketCount, not inFlight: inFlight returns 0 for an absent bucket AND for one still present at zero, so only
        // bucketCount actually proves the entry was evicted. Without this the memory bound is untested.
        assertEquals(0, tracker.bucketCount());
        // re-acquiring after the bucket drained (and was removed) works and starts from 1
        Releasable p2 = tracker.tryAcquire("bucket", 5);
        assertEquals(1, tracker.inFlight("bucket"));
        p2.close();
        assertEquals(0, tracker.bucketCount());
    }

    public void testRefusedAcquireDoesNotLeaveAnEmptyBucketBehind() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        assertNull(tracker.tryAcquire("bucket", 0));
        // The rollback must remove the entry it created, otherwise a stream of refused requests for distinct buckets
        // would accumulate zero-valued entries forever.
        assertEquals(0, tracker.bucketCount());
    }

    public void testReleaseIsIdempotent() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        // Two permits so the bucket survives the first close: with only one, the entry is evicted and a buggy second
        // decrement would land on an orphaned counter that inFlight() can no longer see, making the test vacuous.
        Releasable p1 = tracker.tryAcquire("bucket", 5);
        Releasable p2 = tracker.tryAcquire("bucket", 5);
        p1.close();
        p1.close(); // double close must not decrement twice
        assertEquals(1, tracker.inFlight("bucket"));
        p2.close();
        assertEquals(0, tracker.bucketCount());
    }

    public void testBucketsAreIndependent() {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        assertNotNull(tracker.tryAcquire("a", 1));
        assertNull(tracker.tryAcquire("a", 1)); // "a" is full but "b" is a separate bucket
        Releasable pb = tracker.tryAcquire("b", 1);
        assertNotNull(pb);
        assertEquals(1, tracker.inFlight("a"));
        assertEquals(1, tracker.inFlight("b"));
        pb.close();
    }

    /**
     * Hammers one bucket from many threads. Asserts the two properties the whole design rests on and that no
     * single-threaded test can show: the limit is never exceeded, and every entry is evicted once the dust settles.
     */
    public void testConcurrentAcquireNeverExceedsLimitAndFullyDrains() throws Exception {
        WorkloadGroupThrottleTracker tracker = new WorkloadGroupThrottleTracker();
        final int limit = 4;
        final int threads = 8;
        final int iterations = 200;
        final AtomicInteger concurrentlyHeld = new AtomicInteger();
        final AtomicInteger maxObserved = new AtomicInteger();
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final List<Throwable> failures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        Releasable p = tracker.tryAcquire("hot", limit);
                        if (p != null) {
                            int held = concurrentlyHeld.incrementAndGet();
                            maxObserved.accumulateAndGet(held, Math::max);
                            concurrentlyHeld.decrementAndGet();
                            p.close();
                        }
                    }
                } catch (Throwable e) {
                    synchronized (failures) {
                        failures.add(e);
                    }
                } finally {
                    done.countDown();
                }
            });
            thread.start();
        }
        start.countDown();
        assertTrue("threads did not finish in time", done.await(60, TimeUnit.SECONDS));

        synchronized (failures) {
            assertTrue("worker threads threw: " + failures, failures.isEmpty());
        }
        assertTrue("admitted " + maxObserved.get() + " concurrently for a limit of " + limit, maxObserved.get() <= limit);
        assertEquals("counter did not return to zero", 0, tracker.inFlight("hot"));
        assertEquals("bucket entry was not evicted", 0, tracker.bucketCount());
    }
}
