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

    public void testFailedCommitDoesNotResetTrackedBytes() {
        TranslogBytesTracker tracker = new TranslogBytesTracker();
        tracker.addBytes(10);

        tracker.startCommit();
        tracker.addBytes(20);

        assertEquals(30, tracker.getBytesSinceLastCommit());
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
