/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeBackpressureStrategyTests extends OpenSearchTestCase {

    /**
     * register(listener) must install both onReady and onCancel handlers on the listener.
     * The channel's own setOnCancelHandler is intentionally NOT called by the channel —
     * the strategy owns it, and channel cleanup runs via the composed cancelCallback.
     */
    public void testRegisterInstallsBothHandlers() {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        AtomicReference<Runnable> capturedReadyHandler = new AtomicReference<>();
        AtomicReference<Runnable> capturedCancelHandler = new AtomicReference<>();

        doAnswer(inv -> {
            capturedReadyHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        doAnswer(inv -> {
            capturedCancelHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));

        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {}, () -> false);
        bp.register(listener);

        assertNotNull("onReadyHandler must be installed", capturedReadyHandler.get());
        assertNotNull("onCancelHandler must be installed", capturedCancelHandler.get());
    }

    /**
     * waitForListener must return READY immediately when the listener is already ready.
     */
    public void testWaitForListenerReadyFastPath() {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        when(listener.isReady()).thenReturn(true);
        when(listener.isCancelled()).thenReturn(false);

        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {}, () -> false);
        bp.register(listener);

        long start = System.nanoTime();
        BackpressureStrategy.WaitResult result = bp.waitForListener(5_000);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertEquals(BackpressureStrategy.WaitResult.READY, result);
        assertTrue("Fast path must not park (elapsed=" + elapsedMillis + "ms)", elapsedMillis < 100);
    }

    /**
     * Firing the captured onReadyHandler must wake a thread parked in waitForListener.
     * Simulates gRPC's OnReadyHandler firing while the eventloop is parked.
     */
    public void testReadyHandlerWakesWaiter() throws Exception {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean(false);
        when(listener.isReady()).thenAnswer(inv -> ready.get());
        when(listener.isCancelled()).thenReturn(false);

        AtomicReference<Runnable> readyHandler = new AtomicReference<>();
        doAnswer(inv -> {
            readyHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));

        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {}, () -> false);
        bp.register(listener);

        CountDownLatch waiterEntered = new CountDownLatch(1);
        AtomicReference<BackpressureStrategy.WaitResult> result = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterEntered.countDown();
            result.set(bp.waitForListener(30_000));
        }, "waiter");
        waiter.start();
        assertTrue(waiterEntered.await(2, TimeUnit.SECONDS));
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, waiter.getState()), 2, TimeUnit.SECONDS);

        ready.set(true);
        readyHandler.get().run();

        waiter.join(2_000);
        assertFalse("Waiter must have exited", waiter.isAlive());
        assertEquals(BackpressureStrategy.WaitResult.READY, result.get());
    }

    /**
     * The composed cancel callback must run channel cleanup BEFORE notifying waiters,
     * so any thread that wakes observes the channel in its cancelled state.
     * Also verifies cleanup runs exactly once per cancel.
     */
    public void testCancelCallbackRunsCleanupBeforeNotify() throws Exception {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        when(listener.isReady()).thenReturn(false);
        when(listener.isCancelled()).thenAnswer(inv -> cancelled.get());

        AtomicReference<Runnable> cancelHandler = new AtomicReference<>();
        doAnswer(inv -> {
            cancelHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));

        AtomicInteger cleanupCount = new AtomicInteger();
        AtomicBoolean cleanupRanWhileWaiterStillSleeping = new AtomicBoolean(false);

        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {
            cleanupCount.incrementAndGet();
            // Channel cleanup flips the cancelled flag the strategy will observe on wake.
            cancelled.set(true);
            cleanupRanWhileWaiterStillSleeping.set(true);
        }, () -> false);
        bp.register(listener);

        CountDownLatch waiterEntered = new CountDownLatch(1);
        AtomicReference<BackpressureStrategy.WaitResult> result = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterEntered.countDown();
            result.set(bp.waitForListener(30_000));
        }, "waiter");
        waiter.start();
        assertTrue(waiterEntered.await(2, TimeUnit.SECONDS));
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, waiter.getState()), 2, TimeUnit.SECONDS);

        // Fire the cancel handler — this is what gRPC would do on client cancel.
        cancelHandler.get().run();

        waiter.join(2_000);
        assertFalse("Waiter must have exited", waiter.isAlive());
        assertEquals(BackpressureStrategy.WaitResult.CANCELLED, result.get());
        assertEquals("cleanup must run exactly once", 1, cleanupCount.get());
        assertTrue("cleanup must have run before notify woke the waiter", cleanupRanWhileWaiterStillSleeping.get());
    }

    /**
     * gRPC's OnReadyHandler fires only on transitions (not-ready → ready), not on every
     * isReady()==true state. Once the listener is ready, multiple successive
     * waitForListener calls must all see READY without waiting for a fresh handler firing.
     */
    public void testRepeatedWaitsReturnReadyWithoutRehandler() {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        when(listener.isReady()).thenReturn(true);
        when(listener.isCancelled()).thenReturn(false);

        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {}, () -> false);
        bp.register(listener);

        for (int i = 0; i < 5; i++) {
            assertEquals("call #" + i, BackpressureStrategy.WaitResult.READY, bp.waitForListener(5_000));
        }
    }

    /**
     * waitForListener must return TIMEOUT when neither ready nor cancelled fires within the timeout.
     */
    public void testWaitForListenerTimeout() {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        when(listener.isReady()).thenReturn(false);
        when(listener.isCancelled()).thenReturn(false);

        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {}, () -> false);
        bp.register(listener);

        long start = System.nanoTime();
        BackpressureStrategy.WaitResult result = bp.waitForListener(100);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertEquals(BackpressureStrategy.WaitResult.TIMEOUT, result);
        assertTrue("Should wait approximately the timeout (elapsed=" + elapsedMillis + "ms)", elapsedMillis >= 90);
    }

    /**
     * An unbounded wait must not time out: it returns only when the ready or cancel callback fires.
     */
    public void testUnboundedWaitReturnsOnlyOnReadyOrCancel() throws Exception {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        AtomicBoolean ready = new AtomicBoolean(false);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        when(listener.isReady()).thenAnswer(inv -> ready.get());
        when(listener.isCancelled()).thenAnswer(inv -> cancelled.get());
        AtomicReference<Runnable> readyHandler = new AtomicReference<>();
        AtomicReference<Runnable> cancelHandler = new AtomicReference<>();
        doAnswer(inv -> {
            readyHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));
        doAnswer(inv -> {
            cancelHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));
        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {}, () -> false);
        bp.register(listener);

        for (boolean viaCancel : new boolean[] { false, true }) {
            AtomicReference<BackpressureStrategy.WaitResult> result = new AtomicReference<>();
            Thread waiter = new Thread(() -> result.set(bp.waitForListener(Long.MAX_VALUE)), "waiter");
            waiter.start();
            assertBusy(() -> assertEquals(Thread.State.WAITING, waiter.getState()), 2, TimeUnit.SECONDS);
            assertFalse("an unbounded wait must keep waiting", waitUntil(() -> waiter.isAlive() == false, 200, TimeUnit.MILLISECONDS));
            if (viaCancel) {
                cancelled.set(true);
                cancelHandler.get().run();
            } else {
                ready.set(true);
                readyHandler.get().run();
            }
            waiter.join(2_000);
            assertFalse(waiter.isAlive());
            assertEquals(viaCancel ? BackpressureStrategy.WaitResult.CANCELLED : BackpressureStrategy.WaitResult.READY, result.get());
            ready.set(false);
        }
    }

    /**
     * A call the server ended itself is never ready again and gRPC fires no callback for it, so a wait, even an unbounded
     * one, must end once the channel reports itself closed and signals the strategy.
     */
    public void testChannelCloseEndsWait() throws Exception {
        ServerStreamListener listener = mock(ServerStreamListener.class);
        when(listener.isReady()).thenReturn(false);
        when(listener.isCancelled()).thenReturn(false);
        AtomicBoolean closed = new AtomicBoolean(false);
        CompositeBackpressureStrategy bp = new CompositeBackpressureStrategy(() -> {}, closed::get);
        bp.register(listener);

        for (long timeout : new long[] { Long.MAX_VALUE, 60_000 }) {
            closed.set(false);
            AtomicReference<BackpressureStrategy.WaitResult> result = new AtomicReference<>();
            Thread waiter = new Thread(() -> result.set(bp.waitForListener(timeout)), "waiter");
            waiter.start();
            Thread.State parked = timeout == Long.MAX_VALUE ? Thread.State.WAITING : Thread.State.TIMED_WAITING;
            assertBusy(() -> assertEquals(parked, waiter.getState()), 2, TimeUnit.SECONDS);

            closed.set(true);
            bp.signal();

            waiter.join(2_000);
            assertFalse("the wait must end once the channel is closed", waiter.isAlive());
            assertEquals(BackpressureStrategy.WaitResult.OTHER, result.get());
            assertEquals("a closed channel ends the wait at once", BackpressureStrategy.WaitResult.OTHER, bp.waitForListener(timeout));
        }
    }
}
