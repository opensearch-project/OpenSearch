/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.task;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cancelling a query task must be able to reach an independent concern — concretely, the in-flight
 * analytics streams whose reads would otherwise block forever.
 *
 * <p>Regression cover for the interruption gap: a coordinator drain parks in
 * {@code FlightStream.next()} with no deadline, and the only {@code stream.cancel(...)} lives in the
 * drain loop's own {@code finally}, which that blocked thread never reaches. The cancel path
 * ({@code QueryScheduler} -> {@code QueryExecution.cancelAll} -> {@code AbstractStageExecution.cancel})
 * only flips state flags, so cancellation has to be delivered to the stream directly.
 *
 * <p>The single-slot {@link AnalyticsQueryTask#setOnCancelCallback} cannot carry that: the query driver
 * already owns it and deliberately replaces it across dispatch phases, so registering there would
 * silently steal it. These tests pin the additive listener contract that makes stream cancellation
 * possible without disturbing the driver's slot.
 */
public class AnalyticsQueryTaskCancellationTests extends OpenSearchTestCase {

    private static AnalyticsQueryTask newTask() {
        return new AnalyticsQueryTask(
            1L,
            "transport",
            "indices:data/read/analytics/query",
            "query-1",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            TimeValue.timeValueMinutes(1)
        );
    }

    /**
     * The reproduction: a drain blocked on a stream read is only released if cancellation reaches the
     * stream. Registering that unblock additively must not require the driver's single slot, and both
     * must fire.
     */
    public void testCancellationUnblocksStreamReadWithoutStealingDriverSlot() throws Exception {
        AnalyticsQueryTask task = newTask();

        // The query driver owns the single slot (QueryScheduler installs execution.cancelAll here).
        AtomicBoolean driverCallbackRan = new AtomicBoolean(false);
        task.setOnCancelCallback(() -> driverCallbackRan.set(true));

        // A stand-in for the in-flight stream: a drain thread parked with no deadline, exactly as
        // FlightTransportResponse#nextResponse parks in LinkedBlockingQueue.take().
        CountDownLatch batchAvailable = new CountDownLatch(1);
        CountDownLatch drainReleased = new CountDownLatch(1);
        AtomicBoolean streamCancelled = new AtomicBoolean(false);
        Thread drain = new Thread(() -> {
            try {
                batchAvailable.await();          // a producer that never sends
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                drainReleased.countDown();
            }
        }, "fake-stream-drain");
        drain.setDaemon(true);
        drain.start();

        // The independent concern: cancel the stream, which releases the blocked read.
        task.addCancellationListener(() -> {
            streamCancelled.set(true);
            batchAvailable.countDown();
        });

        assertFalse("nothing should have fired before cancellation", streamCancelled.get());
        assertTrue("drain must still be blocked before cancellation", drainReleased.getCount() > 0);

        task.cancel("test cancel");

        assertTrue(
            "cancelling the task must cancel the in-flight stream; without an additive hook the "
                + "blocked read is never released and the query leaks a live task and a parked thread",
            drainReleased.await(5, TimeUnit.SECONDS)
        );
        assertTrue("the stream-cancel listener must have run", streamCancelled.get());
        assertTrue("the driver's own cancel callback must still run — the slot must not be stolen", driverCallbackRan.get());
        drain.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse("drain thread must not be left alive", drain.isAlive());
    }

    /** Several streams are in flight per query, so the hook must accumulate rather than replace. */
    public void testAdditiveListenersAllRunExactlyOnce() {
        AnalyticsQueryTask task = newTask();
        AtomicInteger runs = new AtomicInteger();
        for (int i = 0; i < 4; i++) {
            task.addCancellationListener(runs::incrementAndGet);
        }

        task.cancel("test cancel");
        assertEquals("every registered stream must be cancelled", 4, runs.get());

        // onCancelled is one-shot upstream; a second cancel must not re-run listeners.
        task.cancel("test cancel again");
        assertEquals("listeners must run at most once", 4, runs.get());
    }

    /** A stream registered after cancellation has already landed must still be cancelled, not stranded. */
    public void testLateRegistrationFiresImmediately() {
        AnalyticsQueryTask task = newTask();
        task.cancel("cancelled before registration");

        AtomicBoolean ran = new AtomicBoolean(false);
        task.addCancellationListener(() -> ran.set(true));
        assertTrue("a listener registered after cancellation must fire inline", ran.get());
    }

    public void testNullListenerIsIgnored() {
        AnalyticsQueryTask task = newTask();
        task.addCancellationListener(null);
        task.cancel("test cancel");   // must not throw
    }
}
