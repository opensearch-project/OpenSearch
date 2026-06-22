/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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

    private static ByteBuffer emptyHeader() {
        return ByteBuffer.allocate(0);
    }

    /**
     * close() posts the stream-root free onto the flight executor (single-writer model). Drain the
     * executor so the free has run before asserting allocator state — submitting a no-op and waiting
     * for it guarantees the earlier-queued free task completed (FIFO, single-threaded).
     */
    private void drainExecutor() throws Exception {
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
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

    // ── Native send: metadata framing ──

    /**
     * sendBatch with non-null metadata must call {@code putNext(ArrowBuf)} (not {@code putNext()})
     * with a buffer carrying the exact bytes the producer attached.
     */
    public void testSendBatchWithMetadataCallsPutNextWithBuf() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);

            VectorSchemaRoot root = newSingleIntRoot(realAllocator, 7);
            byte[] metadata = "{\"output_rows\":1}".getBytes(StandardCharsets.UTF_8);
            AtomicReference<byte[]> capturedMetadata = new AtomicReference<>();

            // In production, Flight's putNext(ArrowBuf) takes ownership and frees the buffer.
            // The mock doesn't, so close it here to avoid an allocator leak at test teardown.
            doAnswer(inv -> {
                ArrowBuf buf = inv.getArgument(0);
                byte[] copy = new byte[(int) buf.readableBytes()];
                buf.getBytes(0, copy);
                capturedMetadata.set(copy);
                buf.close();
                return null;
            }).when(listener).putNext(any(ArrowBuf.class));

            ch.sendBatch(new TestArrowResponse(root, metadata), FlightServerChannelTests::emptyHeader);

            verify(listener, times(1)).putNext(any(ArrowBuf.class));
            verify(listener, never()).putNext();
            assertArrayEquals("metadata bytes must round-trip into the ArrowBuf", metadata, capturedMetadata.get());
            // Channel owns the (transferred) stream root; close() posts the free to the executor.
            ch.close();
            drainExecutor();
            assertEquals("all buffers must be freed", 0, realAllocator.getAllocatedMemory());
        }
    }

    /** sendBatch without metadata must call the no-arg {@code putNext()} variant. */
    public void testSendBatchWithoutMetadataCallsPutNextNoArg() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot root = newSingleIntRoot(realAllocator, 1);

            ch.sendBatch(new TestArrowResponse(root), FlightServerChannelTests::emptyHeader);

            verify(listener, times(1)).putNext();
            verify(listener, never()).putNext(any(ArrowBuf.class));
            ch.close();
            drainExecutor();
            assertEquals("all buffers must be freed", 0, realAllocator.getAllocatedMemory());
        }
    }

    // ── Source-root ownership (the channel frees it exactly once) ──

    /** A successful native send empties+frees the source root and retains the stream root until close(). */
    public void testSendBatchFreesSourceRootRetainsStreamRoot() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot source = newSingleIntRoot(realAllocator, 5);

            ch.sendBatch(new TestArrowResponse(source), FlightServerChannelTests::emptyHeader);

            assertEquals("source root must be empty after transfer", 0, source.getRowCount());
            assertNotNull("channel must retain the stream root", ch.getRoot());
            assertTrue("stream root buffers must still be alive before close", realAllocator.getAllocatedMemory() > 0);

            ch.close();
            drainExecutor();
            assertEquals("close must free the stream root", 0, realAllocator.getAllocatedMemory());
        }
    }

    /** When the channel is cancelled, sendBatch rejects and frees the source root exactly once (no leak). */
    public void testSendBatchAfterCancelRejectsAndFreesSource() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            listenerCancelled.set(true);
            capturedCancelHandler.get().run(); // cancel -> close

            VectorSchemaRoot source = newSingleIntRoot(realAllocator, 9);
            StreamException ex = expectThrows(
                StreamException.class,
                () -> ch.sendBatch(new TestArrowResponse(source), FlightServerChannelTests::emptyHeader)
            );
            assertEquals(StreamErrorCode.CANCELLED, ex.getErrorCode());
            verify(listener, never()).start(any());
            assertEquals("rejected source must be freed (no leak, no stream root created)", 0, realAllocator.getAllocatedMemory());
        }
    }

    /** When the channel is closed, sendBatch rejects and frees the source root exactly once. */
    public void testSendBatchAfterCloseRejectsAndFreesSource() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            ch.close();

            VectorSchemaRoot source = newSingleIntRoot(realAllocator, 3);
            expectThrows(
                IllegalStateException.class,
                () -> ch.sendBatch(new TestArrowResponse(source), FlightServerChannelTests::emptyHeader)
            );
            assertEquals("rejected source must be freed", 0, realAllocator.getAllocatedMemory());
        }
    }

    /** An empty-field-vector native batch throws but still frees the source (S8 early-throw path). */
    public void testSendBatchEmptyFieldVectorsFreesSource() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot empty = new VectorSchemaRoot(new ArrayList<>(), new ArrayList<>(), 0);

            expectThrows(
                IllegalStateException.class,
                () -> ch.sendBatch(new TestArrowResponse(empty), FlightServerChannelTests::emptyHeader)
            );
            assertEquals("source must be freed even on the empty-vectors throw", 0, realAllocator.getAllocatedMemory());
        }
    }

    /** If the header supplier throws (S8), the source root is still freed and no stream root leaks. */
    public void testSendBatchHeaderThrowsFreesSource() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot source = newSingleIntRoot(realAllocator, 2);

            expectThrows(IOException.class, () -> ch.sendBatch(new TestArrowResponse(source), () -> { throw new IOException("boom"); }));

            verify(listener, never()).start(any());
            assertEquals("header-throw must not leak source or created stream root", 0, realAllocator.getAllocatedMemory());
        }
    }

    /**
     * If {@code putNext} throws a non-Exception {@code Throwable} (e.g. OOM / AssertionError) AFTER the
     * channel adopted the stream root, the source root is still freed by sendBatch's finally, the stream
     * root is retained (owned by the channel), and a subsequent close() frees it — no leak. (The handler
     * is responsible for invoking close() on this path; see FlightOutboundHandlerTests.)
     */
    public void testSendBatchPutNextThrowableFreesSourceAndStreamRootClosable() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot source = newSingleIntRoot(realAllocator, 15);
            doThrow(new AssertionError("putNext boom")).when(listener).putNext();

            expectThrows(AssertionError.class, () -> ch.sendBatch(new TestArrowResponse(source), FlightServerChannelTests::emptyHeader));

            assertEquals("source root must be empty/freed after the throw", 0, source.getRowCount());
            assertNotNull("stream root was adopted before putNext threw", ch.getRoot());
            assertTrue("stream root buffers are still alive (owned by the channel)", realAllocator.getAllocatedMemory() > 0);

            ch.close();
            drainExecutor();
            assertEquals("close() must free the adopted stream root after a putNext Throwable", 0, realAllocator.getAllocatedMemory());
        }
    }

    /** releaseUnsent frees the source root of a batch that never reached sendBatch (failed hand-off). */
    public void testReleaseUnsentFreesSource() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot source = newSingleIntRoot(realAllocator, 11);

            ch.releaseUnsent(new TestArrowResponse(source));
            assertEquals("releaseUnsent must free the unsent source root", 0, realAllocator.getAllocatedMemory());
        }
    }

    // ── Byte-serialized path: stream root reused, freed once ──

    /** Multi-batch byte path reuses one VarBinary stream root and frees it exactly once at close(). */
    public void testByteMultiBatchReusesStreamRootFreedOnce() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);

            ch.sendBatch(new BytesResponse(new byte[] { 1, 2, 3 }), FlightServerChannelTests::emptyHeader);
            VectorSchemaRoot afterFirst = ch.getRoot();
            assertNotNull("byte path must create a stream root", afterFirst);

            ch.sendBatch(new BytesResponse(new byte[] { 4, 5 }), FlightServerChannelTests::emptyHeader);
            assertSame("byte path must reuse the same stream root across batches", afterFirst, ch.getRoot());

            verify(listener, times(1)).start(any()); // start only on the first batch
            verify(listener, times(2)).putNext();

            ch.close();
            drainExecutor();
            assertEquals("byte stream root must be freed exactly once at close", 0, realAllocator.getAllocatedMemory());
        }
    }

    // ── Terminal-op contracts ──

    /** completeStream is a no-op after the channel was cancelled (no terminal op after cancel). */
    public void testCompleteStreamNoOpAfterCancel() {
        FlightServerChannel ch = newChannel(5_000);
        listenerCancelled.set(true);
        capturedCancelHandler.get().run(); // cancel -> close

        ch.completeStream(emptyHeader());
        verify(listener, never()).completed();
    }

    /** sendError is a no-op after the channel was cancelled. */
    public void testSendErrorNoOpAfterCancel() {
        FlightServerChannel ch = newChannel(5_000);
        listenerCancelled.set(true);
        capturedCancelHandler.get().run();

        ch.sendError(emptyHeader(), new RuntimeException("late"));
        verify(listener, never()).error(any());
    }

    /** At most one terminal gRPC op: a second completeStream is a no-op. */
    public void testCompleteStreamOnlyOnce() {
        FlightServerChannel ch = newChannel(5_000);
        ch.completeStream(emptyHeader());
        ch.completeStream(emptyHeader());
        verify(listener, times(1)).completed();
    }

    /** Once completed(), a later sendError must not also issue error() (single terminal op). */
    public void testSendErrorNoOpAfterComplete() {
        FlightServerChannel ch = newChannel(5_000);
        ch.completeStream(emptyHeader());
        ch.sendError(emptyHeader(), new RuntimeException("after complete"));
        verify(listener, times(1)).completed();
        verify(listener, never()).error(any());
    }

    // ── close() fire-once + listener semantics ──

    /** Concurrent close() callers must each fire every close listener exactly once (no double-fire). */
    public void testCloseIsFireOnceUnderConcurrentCallers() throws Exception {
        FlightServerChannel ch = newChannel(5_000);
        AtomicInteger fires = new AtomicInteger(0);
        int listeners = 3;
        for (int i = 0; i < listeners; i++) {
            ch.addCloseListener(org.opensearch.core.action.ActionListener.wrap(r -> fires.incrementAndGet(), e -> {}));
        }

        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                ch.close();
            });
            ts.add(t);
            t.start();
        }
        start.countDown();
        for (Thread t : ts) {
            t.join(5_000);
        }
        assertEquals("each close listener must fire exactly once across all concurrent close() callers", listeners, fires.get());
    }

    /** A close listener registered after close() fires immediately. */
    public void testAddCloseListenerAfterCloseFiresImmediately() {
        FlightServerChannel ch = newChannel(5_000);
        ch.close();
        AtomicBoolean fired = new AtomicBoolean(false);
        ch.addCloseListener(org.opensearch.core.action.ActionListener.wrap(r -> fired.set(true), e -> {}));
        assertTrue("listener added after close must fire immediately", fired.get());
    }

    /** A throwing close listener must not strand the remaining listeners. */
    public void testThrowingCloseListenerDoesNotStrandOthers() {
        FlightServerChannel ch = newChannel(5_000);
        AtomicBoolean secondFired = new AtomicBoolean(false);
        ch.addCloseListener(org.opensearch.core.action.ActionListener.wrap(r -> { throw new RuntimeException("listener boom"); }, e -> {}));
        ch.addCloseListener(org.opensearch.core.action.ActionListener.wrap(r -> secondFired.set(true), e -> {}));

        ch.close();
        assertTrue("a throwing listener must not prevent later listeners from firing", secondFired.get());
    }

    // ── S4: cancel races an in-flight send; the root free must run behind the send (no use-after-free) ──

    /**
     * Single-writer model: the send runs on the flight executor and {@code close()} posts the
     * stream-root free onto that same executor. While a send is mid-{@code putNext} (occupying the
     * single executor thread), a concurrent cancel -> {@code close()} must NOT free the stream root —
     * the free is queued behind the in-flight send and runs only after it returns. The close listener
     * fires immediately on {@code close()} (it touches no Arrow buffers), but the buffers stay alive
     * until the send completes, then are freed exactly once.
     */
    public void testCancelDuringInFlightSendDefersRootFree() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot source = newSingleIntRoot(realAllocator, 13);

            CountDownLatch inPutNext = new CountDownLatch(1);
            CountDownLatch releasePutNext = new CountDownLatch(1);
            doAnswer(inv -> {
                inPutNext.countDown();
                assertTrue("test timed out waiting to release putNext", releasePutNext.await(5, TimeUnit.SECONDS));
                return null;
            }).when(listener).putNext();

            // Run the send ON the executor, exactly as production does — this is what serializes the
            // close()-posted free behind it.
            AtomicReference<Throwable> sendError = new AtomicReference<>();
            executor.execute(() -> {
                try {
                    ch.sendBatch(new TestArrowResponse(source), FlightServerChannelTests::emptyHeader);
                } catch (Throwable t) {
                    sendError.set(t);
                }
            });

            assertTrue("send must reach putNext", inPutNext.await(5, TimeUnit.SECONDS));

            // Fire the cancel -> close() from this (gRPC-like) thread. It must NOT block, and must post
            // the root free onto the busy executor rather than freeing inline.
            capturedCancelHandler.get().run();
            assertFalse("channel must be marked closed immediately", ch.isOpen());

            // The executor thread is still inside putNext, so the queued free has not run: buffers live.
            assertTrue("stream root must still be alive while the send occupies the executor", realAllocator.getAllocatedMemory() > 0);

            // Let the send finish; the executor then runs the queued free task. drainExecutor() waits
            // behind both the send and the free.
            releasePutNext.countDown();
            drainExecutor();

            assertNull("send must complete without error", sendError.get());
            assertEquals("stream root must be freed exactly once after the send drains", 0, realAllocator.getAllocatedMemory());
        }
    }

    /**
     * Cross-thread close() never frees buffers inline: when close() is called from a non-executor
     * thread, the stream root is freed on the executor (single-writer), so it stays alive until the
     * executor runs the posted free.
     */
    public void testCloseFreesStreamRootOnExecutor() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            // Send a batch on the executor so the channel adopts a stream root, then drain.
            executor.submit(() -> {
                ch.sendBatch(new TestArrowResponse(newSingleIntRoot(realAllocator, 8)), FlightServerChannelTests::emptyHeader);
                return null;
            }).get(5, TimeUnit.SECONDS);
            assertTrue("stream root should be alive after a batch", realAllocator.getAllocatedMemory() > 0);

            ch.close(); // from the test thread (not the executor)
            drainExecutor();
            assertEquals("close() must free the stream root via the executor", 0, realAllocator.getAllocatedMemory());
        }
    }

    /**
     * Batches queued behind a cancel must not leak. While the executor is busy, we enqueue several
     * native batches, then fire the cancel (which sets cancelled/closed and posts the root free behind
     * them). When the executor drains: each queued batch hits the {@code cancelled} guard in
     * {@code sendBatch}, rejects before touching the stream root, and frees its own source root in the
     * {@code finally}; the posted root-free task (last in FIFO order) frees the stream root. Nothing is
     * dropped, leaked, or double-freed.
     */
    public void testBatchesQueuedBehindCancelAreRejectedAndFreed() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);

            // Occupy the single executor thread so the batches below sit in the queue, not run.
            CountDownLatch blockExecutor = new CountDownLatch(1);
            CountDownLatch executorBusy = new CountDownLatch(1);
            executor.execute(() -> {
                executorBusy.countDown();
                try {
                    assertTrue(blockExecutor.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertTrue("executor must be busy", executorBusy.await(5, TimeUnit.SECONDS));

            // Queue several native batches behind the blocked task. Each carries its own source root.
            int queued = 4;
            List<Throwable> sendErrors = new ArrayList<>();
            for (int i = 0; i < queued; i++) {
                VectorSchemaRoot source = newSingleIntRoot(realAllocator, i);
                executor.execute(() -> {
                    try {
                        ch.sendBatch(new TestArrowResponse(source), FlightServerChannelTests::emptyHeader);
                    } catch (Throwable t) {
                        synchronized (sendErrors) {
                            sendErrors.add(t);
                        }
                    }
                });
            }
            assertTrue("all queued sources must be allocated", realAllocator.getAllocatedMemory() > 0);

            // Cancel now (from the gRPC-like thread): sets cancelled, flips open, posts the root free
            // to the BACK of the executor queue (after the queued batches).
            capturedCancelHandler.get().run();
            assertFalse("channel must be marked closed immediately", ch.isOpen());

            // Release the executor and let it drain the blocked task, the queued batches, and the free.
            blockExecutor.countDown();
            drainExecutor();

            assertEquals("every queued batch must have rejected", queued, sendErrors.size());
            for (Throwable t : sendErrors) {
                assertTrue("queued batches must reject with CANCELLED, got " + t, t instanceof StreamException);
                assertEquals(StreamErrorCode.CANCELLED, ((StreamException) t).getErrorCode());
            }
            // No stream root was ever created (every batch rejected before transfer), and each queued
            // source was freed by its own sendBatch finally.
            verify(listener, never()).start(any());
            assertEquals("no queued source root may leak after cancel", 0, realAllocator.getAllocatedMemory());
        }
    }

    // ── classifyError tests ──────────────────────────────────────────────
    // These pin the contract that Arrow allocator exhaustion (back-pressure)
    // surfaces as CallStatus.RESOURCE_EXHAUSTED, while everything else stays
    // CallStatus.INTERNAL. The mapping is what later lets the coordinator
    // translate the error into an HTTP 429 instead of 500 — preventing replica
    // retry storms via OpenSearch core's FailAwareWeightedRouting.

    public void testClassifyErrorArrowOomIsResourceExhausted() {
        OutOfMemoryException oom = new OutOfMemoryException("Unable to allocate buffer of size 128 due to memory limit");
        assertSame(CallStatus.RESOURCE_EXHAUSTED, FlightServerChannel.classifyError(oom));
    }

    public void testClassifyErrorWrappedArrowOomIsResourceExhausted() {
        // Mirrors the production cause chain: ArrayImporter wraps Arrow OOM in IllegalArgumentException.
        OutOfMemoryException oom = new OutOfMemoryException("Unable to allocate buffer of size 98304");
        IllegalArgumentException wrapped = new IllegalArgumentException("Could not load buffers for field cnt[hll_registers]", oom);
        assertSame(CallStatus.RESOURCE_EXHAUSTED, FlightServerChannel.classifyError(wrapped));
    }

    public void testClassifyErrorRuntimePanicIsInternal() {
        // Rust panic translated to RuntimeException — engine bug, NOT back-pressure.
        RuntimeException panic = new RuntimeException("Execution error: Panic: byte array offset overflow");
        assertSame(CallStatus.INTERNAL, FlightServerChannel.classifyError(panic));
    }

    public void testClassifyErrorGenericExceptionIsInternal() {
        assertSame(CallStatus.INTERNAL, FlightServerChannel.classifyError(new IllegalStateException("boom")));
    }

    public void testClassifyErrorNullCauseChainIsInternal() {
        assertSame(CallStatus.INTERNAL, FlightServerChannel.classifyError(new RuntimeException("no cause")));
    }

    private static VectorSchemaRoot newSingleIntRoot(BufferAllocator allocator, int value) {
        Schema schema = new Schema(List.of(new Field("v", FieldType.nullable(new ArrowType.Int(32, true)), null)));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        IntVector vec = (IntVector) root.getVector("v");
        vec.allocateNew();
        vec.setSafe(0, value);
        vec.setValueCount(1);
        root.setRowCount(1);
        return root;
    }

    /** Minimal send-side ArrowBatchResponse for tests. */
    static final class TestArrowResponse extends ArrowBatchResponse {
        TestArrowResponse(VectorSchemaRoot root) {
            super(root);
        }

        TestArrowResponse(VectorSchemaRoot root, byte[] metadata) {
            super(root, metadata);
        }

        TestArrowResponse(StreamInput in) throws IOException {
            super(in);
        }
    }

    /** Minimal non-Arrow response that drives the byte-serialization path. */
    static final class BytesResponse extends TransportResponse {
        private final byte[] data;

        BytesResponse(byte[] data) {
            this.data = data;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByteArray(data);
        }
    }
}
