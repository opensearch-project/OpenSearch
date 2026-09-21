/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.concurrent;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Tests for the removable listeners of {@link CompletableContext}
 */
public class CompletableContextTests extends OpenSearchTestCase {

    public void testRemovableListenerIsNotifiedOnCompletion() {
        CompletableContext<Void> context = new CompletableContext<>();
        AtomicInteger notified = new AtomicInteger();
        context.addRemovableListener((v, e) -> notified.incrementAndGet());
        assertEquals(1, context.removableListeners());

        context.complete(null);

        assertEquals(1, notified.get());
        assertEquals(0, context.removableListeners());
    }

    public void testRemovedListenerIsNotNotified() {
        CompletableContext<Void> context = new CompletableContext<>();
        AtomicInteger notified = new AtomicInteger();
        BiConsumer<Void, Exception> listener = (v, e) -> notified.incrementAndGet();

        context.addRemovableListener(listener);
        context.removeRemovableListener(listener);
        assertEquals(0, context.removableListeners());

        context.complete(null);

        assertEquals(0, notified.get());
    }

    public void testRemovingAnUnknownListenerDoesNothing() {
        CompletableContext<Void> context = new CompletableContext<>();
        context.removeRemovableListener((v, e) -> fail("should not be notified"));

        assertEquals(0, context.removableListeners());
    }

    public void testListenerAddedAfterCompletionIsNotifiedImmediately() {
        CompletableContext<Void> context = new CompletableContext<>();
        AtomicInteger notified = new AtomicInteger();

        context.complete(null);
        context.addRemovableListener((v, e) -> notified.incrementAndGet());

        assertEquals(1, notified.get());
        assertEquals(0, context.removableListeners());
    }

    public void testRemovableListenerSeesTheResult() {
        CompletableContext<String> context = new CompletableContext<>();
        AtomicReference<String> seen = new AtomicReference<>();
        context.addRemovableListener((v, e) -> seen.set(v));

        context.complete("closed");

        assertEquals("closed", seen.get());
    }

    public void testRemovableListenerSeesTheFailure() {
        CompletableContext<Void> context = new CompletableContext<>();
        AtomicReference<Exception> seen = new AtomicReference<>();
        Exception failure = new IllegalStateException("connection reset");
        context.addRemovableListener((v, e) -> seen.set(e));

        context.completeExceptionally(failure);

        assertSame(failure, seen.get());
    }

    public void testRemovableListenersDoNotReachTheCompletableFuture() {
        CompletableContext<Void> context = new CompletableContext<>();
        int listeners = randomIntBetween(2, 100);
        List<BiConsumer<Void, ? super Exception>> added = new ArrayList<>(listeners);
        for (int i = 0; i < listeners; i++) {
            final int index = i;
            BiConsumer<Void, Exception> listener = (v, e) -> fail("listener " + index + " should have been removed");
            added.add(listener);
            context.addRemovableListener(listener);
        }
        assertEquals(listeners, context.removableListeners());

        added.forEach(context::removeRemovableListener);
        assertEquals(0, context.removableListeners());

        context.complete(null);
    }

    public void testTheSameListenerAddedTwiceIsHeldOnce() {
        CompletableContext<Void> context = new CompletableContext<>();
        AtomicInteger notified = new AtomicInteger();
        BiConsumer<Void, Exception> listener = (v, e) -> notified.incrementAndGet();

        context.addRemovableListener(listener);
        context.addRemovableListener(listener);
        assertEquals(1, context.removableListeners());

        context.complete(null);

        assertEquals(1, notified.get());
    }

    public void testListenerIsNotifiedExactlyOnceWhenAddedWhileCompleting() throws Exception {
        int listeners = 50;
        CompletableContext<Void> context = new CompletableContext<>();
        AtomicInteger notified = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(listeners + 1);
        List<Thread> threads = new ArrayList<>(listeners + 1);

        for (int i = 0; i < listeners; i++) {
            threads.add(new Thread(() -> {
                try {
                    start.await();
                    context.addRemovableListener((v, e) -> notified.incrementAndGet());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }));
        }
        threads.add(new Thread(() -> {
            try {
                start.await();
                context.complete(null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        }));

        threads.forEach(Thread::start);
        start.countDown();
        assertTrue("threads did not finish", done.await(10, TimeUnit.SECONDS));
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(listeners, notified.get());
        assertEquals(0, context.removableListeners());
    }
}
