/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class AnalyticsShardTaskCancellationListenerTests extends OpenSearchTestCase {

    private AnalyticsShardTask createTask() {
        return new AnalyticsShardTask(1L, "type", "action", "description", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    public void testListenerFiresOnCancellation() {
        AnalyticsShardTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);

        task.cancel("test reason");

        assertEquals(1, callCount.get());
    }

    public void testListenerFiresImmediatelyIfAlreadyCancelled() {
        AnalyticsShardTask task = createTask();
        task.cancel("already cancelled");

        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);

        assertEquals(1, callCount.get());
    }

    public void testClearListenerPreventsCallback() {
        AnalyticsShardTask task = createTask();
        AtomicInteger callCount = new AtomicInteger();
        task.setCancellationListener(callCount::incrementAndGet);
        task.clearCancellationListener();

        task.cancel("test reason");

        assertEquals(0, callCount.get());
    }

    public void testNullListenerDoesNotThrowOnCancellation() {
        AnalyticsShardTask task = createTask();
        task.cancel("test reason");
    }

    public void testSetListenerReplacesExisting() {
        AnalyticsShardTask task = createTask();
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();

        task.setCancellationListener(first::incrementAndGet);
        task.setCancellationListener(second::incrementAndGet);

        task.cancel("test reason");

        assertEquals(0, first.get());
        assertEquals(1, second.get());
    }

    /** addCancellationListener is ADDITIVE — multiple hooks all fire (vs setCancellationListener's
     *  single-slot replace). Models the DataFusion native-cancel + shuffle-buffer-cleanup both
     *  registering on one task; neither must overwrite the other. */
    public void testAddListenersAreAllInvoked() {
        AnalyticsShardTask task = createTask();
        AtomicInteger a = new AtomicInteger();
        AtomicInteger b = new AtomicInteger();
        task.addCancellationListener(a::incrementAndGet);
        task.addCancellationListener(b::incrementAndGet);

        task.cancel("test reason");

        assertEquals("first additive listener fired", 1, a.get());
        assertEquals("second additive listener fired (not overwritten)", 1, b.get());
    }

    /** An additive listener and the single-slot listener coexist — both fire. */
    public void testAddAndSetListenersCoexist() {
        AnalyticsShardTask task = createTask();
        AtomicInteger additive = new AtomicInteger();
        AtomicInteger single = new AtomicInteger();
        task.addCancellationListener(additive::incrementAndGet);
        task.setCancellationListener(single::incrementAndGet);

        task.cancel("test reason");

        assertEquals(1, additive.get());
        assertEquals(1, single.get());
    }

    /** addCancellationListener fires immediately if the task is already cancelled, and only once. */
    public void testAddListenerFiresImmediatelyIfAlreadyCancelled() {
        AnalyticsShardTask task = createTask();
        task.cancel("already cancelled");

        AtomicInteger count = new AtomicInteger();
        task.addCancellationListener(count::incrementAndGet);
        assertEquals(1, count.get());

        // A second cancel (no-op) must not re-run it.
        task.cancel("again");
        assertEquals(1, count.get());
    }

    /**
     * Race addCancellationListener against cancel() from another thread, many iterations: the
     * listener must fire EXACTLY ONCE regardless of interleaving (added-then-cancelled, or
     * cancelled-then-added where the inline isCancelled() branch fires it) — never zero, never twice.
     */
    public void testAddListenerExactlyOnceUnderRaceWithCancel() throws Exception {
        for (int i = 0; i < 500; i++) {
            AnalyticsShardTask task = createTask();
            AtomicInteger count = new AtomicInteger();
            java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
            Thread canceller = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                task.cancel("race");
            });
            canceller.start();
            start.countDown();
            task.addCancellationListener(count::incrementAndGet);
            canceller.join(2_000);
            // Ensure cancel happened (the listener fires on cancel, immediately-or-deferred).
            assertTrue("task should be cancelled", task.isCancelled());
            assertEquals("listener must fire exactly once under race (iteration " + i + ")", 1, count.get());
        }
    }
}
