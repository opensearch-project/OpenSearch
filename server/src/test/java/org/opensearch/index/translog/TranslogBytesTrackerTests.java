/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TranslogBytesTrackerTests extends OpenSearchTestCase {

    public void testTracksBytesSinceLastCommit() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();

        tracker.addBytes(10);
        tracker.addBytes(20);

        assertEquals(30, tracker.getBytesSinceLastCommit());
        TranslogBytesTracker.CommitSnapshot commitSnapshot = tracker.startCommit();
        tracker.completeCommit(commitSnapshot);
        assertEquals(0, tracker.getBytesSinceLastCommit());
    }

    public void testDiscardedCommitSnapshotRetainsTrackedBytes() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();
        tracker.addBytes(10);

        // A snapshot that is never completed, which is what a failed index commit leaves behind, must not release
        // anything. The bytes have to stay eligible for the commit that follows.
        tracker.startCommit();
        tracker.addBytes(20);
        assertEquals(30, tracker.getBytesSinceLastCommit());

        tracker.completeCommit(tracker.startCommit());
        assertEquals(0, tracker.getBytesSinceLastCommit());
    }

    public void testInitializeSeedsBaselineOnce() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();
        assertFalse(tracker.isInitialized());

        assertTrue(tracker.initialize(100));
        assertTrue(tracker.isInitialized());
        assertEquals(100, tracker.getBytesSinceLastCommit());

        // A second seeding attempt is a no-op so that every entry point can call it unguarded.
        assertFalse(tracker.initialize(500));
        assertEquals(100, tracker.getBytesSinceLastCommit());
    }

    public void testInitializeRemainsMarkedAfterCommit() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();
        assertTrue(tracker.initialize(100));
        tracker.completeCommit(tracker.startCommit());

        assertEquals(0, tracker.getBytesSinceLastCommit());
        assertTrue(tracker.isInitialized());
        assertFalse(tracker.initialize(100));
        assertEquals(0, tracker.getBytesSinceLastCommit());
    }

    public void testInitializeRejectsNegativeBaseline() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> tracker.initialize(-1));
        assertEquals("translog bytes must be non-negative", exception.getMessage());
        assertFalse(tracker.isInitialized());
    }

    public void testAddBytesSaturatesInsteadOfOverflowing() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();
        tracker.addBytes(Long.MAX_VALUE - 1);

        tracker.addBytes(10);

        assertEquals(Long.MAX_VALUE, tracker.getBytesSinceLastCommit());
        tracker.completeCommit(tracker.startCommit());
        assertEquals(0, tracker.getBytesSinceLastCommit());
    }

    public void testCompleteCommitRejectsSnapshotLargerThanTrackedBytes() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();
        tracker.addBytes(100);
        TranslogBytesTracker.CommitSnapshot commitSnapshot = tracker.startCommit();
        tracker.completeCommit(commitSnapshot);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> tracker.completeCommit(commitSnapshot));
        assertEquals("commit snapshot contains [100] bytes but only [0] bytes are tracked", exception.getMessage());
    }

    public void testSuccessfulCommitRetainsConcurrentWrites() throws Exception {
        TranslogBytesTracker tracker = new TranslogBytesTracker();
        tracker.addBytes(100);
        TranslogBytesTracker.CommitSnapshot commitSnapshot = tracker.startCommit();

        int threadCount = randomIntBetween(2, 5);
        int writesPerThread = randomIntBetween(100, 500);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int write = 0; write < writesPerThread; write++) {
                        tracker.addBytes(1);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });
            threads.add(thread);
            thread.start();
        }

        assertTrue(ready.await(30, TimeUnit.SECONDS));
        start.countDown();
        tracker.completeCommit(commitSnapshot);
        assertTrue(done.await(30, TimeUnit.SECONDS));
        for (Thread thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertEquals((long) threadCount * writesPerThread, tracker.getBytesSinceLastCommit());
    }

    public void testRejectsNegativeBytes() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> tracker.addBytes(-1));
        assertEquals("translog bytes must be non-negative", exception.getMessage());
    }
}
