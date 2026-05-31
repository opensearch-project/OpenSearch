/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightServerChannelTests extends OpenSearchTestCase {

    private ServerStreamListener listener;
    private BufferAllocator allocator;
    private ServerHeaderMiddleware middleware;
    private FlightCallTracker callTracker;
    private ExecutorService executor;
    private AtomicBoolean ready;
    private AtomicBoolean listenerCancelled;
    private AtomicReference<Runnable> capturedReadyHandler;
    private AtomicReference<Runnable> capturedCancelHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        listener = mock(ServerStreamListener.class);
        allocator = mock(BufferAllocator.class);
        middleware = mock(ServerHeaderMiddleware.class);
        when(middleware.getCorrelationId()).thenReturn("42");
        callTracker = mock(FlightCallTracker.class);
        executor = Executors.newSingleThreadExecutor();

        ready = new AtomicBoolean(false);
        listenerCancelled = new AtomicBoolean(false);
        when(listener.isReady()).thenAnswer(inv -> ready.get());
        when(listener.isCancelled()).thenAnswer(inv -> listenerCancelled.get());

        capturedReadyHandler = new AtomicReference<>();
        capturedCancelHandler = new AtomicReference<>();
        doAnswer(inv -> {
            capturedReadyHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnReadyHandler(any(Runnable.class));
        doAnswer(inv -> {
            capturedCancelHandler.set(inv.getArgument(0));
            return null;
        }).when(listener).setOnCancelHandler(any(Runnable.class));
    }

    @Override
    public void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        super.tearDown();
    }

    private FlightServerChannel newChannel(long readyTimeoutMillis) {
        return new FlightServerChannel(listener, allocator, middleware, callTracker, executor, readyTimeoutMillis);
    }

    public void testAwaitReadyFastPath() {
        ready.set(true);
        FlightServerChannel ch = newChannel(5_000);

        long start = System.nanoTime();
        ch.awaitReadyOrThrow();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertTrue("Fast path must not park (elapsed=" + elapsedMs + "ms)", elapsedMs < 100);
    }

    public void testAwaitReadyParksUntilOnReadyFires() throws Exception {
        ready.set(false);
        FlightServerChannel ch = newChannel(30_000);

        CountDownLatch waiterEntered = new CountDownLatch(1);
        AtomicReference<Throwable> waiterError = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterEntered.countDown();
            try {
                ch.awaitReadyOrThrow();
            } catch (Throwable t) {
                waiterError.set(t);
            }
        }, "producer-waiter");
        waiter.start();
        assertTrue(waiterEntered.await(2, TimeUnit.SECONDS));
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, waiter.getState()), 2, TimeUnit.SECONDS);

        ready.set(true);
        capturedReadyHandler.get().run();

        waiter.join(2_000);
        assertFalse("Waiter must have exited", waiter.isAlive());
        assertNull("awaitReadyOrThrow must return normally on READY", waiterError.get());
    }

    public void testAwaitReadyTimeoutThrowsTimedOut() {
        ready.set(false);
        FlightServerChannel ch = newChannel(100);

        StreamException ex = expectThrows(StreamException.class, ch::awaitReadyOrThrow);
        assertEquals(StreamErrorCode.TIMED_OUT, ex.getErrorCode());
        assertTrue("Message should reference the timeout", ex.getMessage().contains("100ms"));

        // TIMED_OUT does not run channel cleanup. The handler is expected to relay the
        // exception via channel.sendResponse(e), and FlightServerChannel.sendError records
        // the call end. Without that relay, no recordCallEnd would happen here — that is
        // the contract, and is verified end-to-end via the handler/transport-channel paths.
        verify(callTracker, org.mockito.Mockito.never()).recordCallEnd(any());
        assertTrue("channel must remain open after timeout — handler may still relay", ch.isOpen());
    }

    /**
     * Cancellation while a producer thread is parked must wake it with a CANCELLED
     * StreamException AND run the channel's onChannelCancelled cleanup (recordCallEnd, close).
     */
    public void testCancelWhileWaitingThrowsAndRunsChannelCleanup() throws Exception {
        ready.set(false);
        FlightServerChannel ch = newChannel(30_000);

        CountDownLatch waiterEntered = new CountDownLatch(1);
        AtomicReference<Throwable> waiterError = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            waiterEntered.countDown();
            try {
                ch.awaitReadyOrThrow();
            } catch (Throwable t) {
                waiterError.set(t);
            }
        }, "producer-waiter");
        waiter.start();
        assertTrue(waiterEntered.await(2, TimeUnit.SECONDS));
        assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, waiter.getState()), 2, TimeUnit.SECONDS);

        listenerCancelled.set(true);
        capturedCancelHandler.get().run();

        waiter.join(2_000);
        assertFalse("Waiter must have exited", waiter.isAlive());

        Throwable t = waiterError.get();
        assertNotNull("waiter must have thrown", t);
        assertTrue("must be StreamException, got " + t, t instanceof StreamException);
        assertEquals(StreamErrorCode.CANCELLED, ((StreamException) t).getErrorCode());

        verify(callTracker).recordCallEnd(StreamErrorCode.CANCELLED.name());
        assertFalse("channel must be closed after cancel", ch.isOpen());
    }

    public void testAwaitReadyOnAlreadyCancelledChannelThrows() {
        ready.set(false);
        FlightServerChannel ch = newChannel(30_000);

        listenerCancelled.set(true);
        capturedCancelHandler.get().run();

        long start = System.nanoTime();
        StreamException ex = expectThrows(StreamException.class, ch::awaitReadyOrThrow);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertEquals(StreamErrorCode.CANCELLED, ex.getErrorCode());
        assertTrue("Already-cancelled path must not park (elapsed=" + elapsedMs + "ms)", elapsedMs < 100);
    }

    /**
     * All concurrent producer threads parked in {@code awaitReadyOrThrow} must wake on a
     * single cancel — none should be left hanging until {@code readyTimeoutMillis}.
     * The channel cleanup ({@code recordCallEnd}, close) must run exactly once.
     */
    public void testCancelWakesAllParkedProducers() throws Exception {
        ready.set(false);
        FlightServerChannel ch = newChannel(30_000);

        int n = 4;
        CountDownLatch entered = new CountDownLatch(n);
        List<AtomicReference<Throwable>> errors = new ArrayList<>(n);
        List<Thread> waiters = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            AtomicReference<Throwable> err = new AtomicReference<>();
            errors.add(err);
            Thread w = new Thread(() -> {
                entered.countDown();
                try {
                    ch.awaitReadyOrThrow();
                } catch (Throwable t) {
                    err.set(t);
                }
            }, "producer-waiter-" + i);
            waiters.add(w);
            w.start();
        }
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        for (Thread w : waiters) {
            assertBusy(() -> assertEquals(Thread.State.TIMED_WAITING, w.getState()), 2, TimeUnit.SECONDS);
        }

        listenerCancelled.set(true);
        capturedCancelHandler.get().run();

        for (Thread w : waiters) {
            w.join(2_000);
            assertFalse("Waiter " + w.getName() + " must have exited", w.isAlive());
        }
        for (AtomicReference<Throwable> err : errors) {
            Throwable t = err.get();
            assertNotNull("each waiter must have thrown", t);
            assertTrue("must be StreamException, got " + t, t instanceof StreamException);
            assertEquals(StreamErrorCode.CANCELLED, ((StreamException) t).getErrorCode());
        }
        // Channel cleanup must be invoked exactly once even though N threads observed cancel.
        verify(callTracker, org.mockito.Mockito.times(1)).recordCallEnd(StreamErrorCode.CANCELLED.name());
    }

    /**
     * After a cancel, subsequent {@code awaitReadyOrThrow} calls must throw immediately
     * — even if the underlying listener somehow reports {@code isReady()==true}. The
     * channel's own cancelled flag short-circuits before we consult the strategy.
     */
    public void testAwaitReadyAfterCancelStaysCancelled() {
        ready.set(true); // intentionally true to ensure the channel-level guard wins
        FlightServerChannel ch = newChannel(30_000);

        listenerCancelled.set(true);
        capturedCancelHandler.get().run();

        StreamException ex = expectThrows(StreamException.class, ch::awaitReadyOrThrow);
        assertEquals(StreamErrorCode.CANCELLED, ex.getErrorCode());
    }
}
