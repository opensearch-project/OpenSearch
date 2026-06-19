/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

            try (VectorStreamOutput out = VectorStreamOutput.forNativeArrow(root)) {
                ch.sendBatch(ByteBuffer.allocate(0), out, metadata);
            }

            verify(listener, times(1)).putNext(any(ArrowBuf.class));
            verify(listener, never()).putNext();
            assertArrayEquals("metadata bytes must round-trip into the ArrowBuf", metadata, capturedMetadata.get());
            root.close();
        }
    }

    /** sendBatch without metadata must call the no-arg {@code putNext()} variant. */
    public void testSendBatchWithoutMetadataCallsPutNextNoArg() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot root = newSingleIntRoot(realAllocator, 1);

            try (VectorStreamOutput out = VectorStreamOutput.forNativeArrow(root)) {
                ch.sendBatch(ByteBuffer.allocate(0), out);
            }

            verify(listener, times(1)).putNext();
            verify(listener, never()).putNext(any(ArrowBuf.class));
            root.close();
        }
    }

    /**
     * First-frame {@code sendArrowBatch}: the channel creates the stream root from the producer's
     * allocator, transfers the producer's vectors in, adopts it, and {@code start()}s the stream.
     * The producer root is consumed (its buffers move into the channel-owned stream root).
     */
    public void testSendArrowBatchFirstFrameCreatesAndAdoptsRoot() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            VectorSchemaRoot producerRoot = newSingleIntRoot(realAllocator, 7);

            ch.sendArrowBatch(ByteBuffer.allocate(0), producerRoot, null);

            // The channel adopted a stream root (start was called) and it carries the transferred value.
            verify(listener, times(1)).start(any(VectorSchemaRoot.class));
            verify(listener, times(1)).putNext();
            VectorSchemaRoot adopted = ch.getRoot();
            assertNotNull("channel must have adopted a stream root", adopted);
            assertEquals(1, adopted.getRowCount());
            assertEquals(7, ((IntVector) adopted.getVector("v")).get(0));
            // The producer root has been drained (vectors transferred out).
            assertEquals(0, producerRoot.getRowCount());

            // The channel owns the adopted root; close() frees it and balances the allocator.
            ch.close();
            assertEquals("no buffers should leak after the channel closes its root", 0, realAllocator.getAllocatedMemory());
        }
    }

    /**
     * Second-frame {@code sendArrowBatch}: an existing stream root is reused (not recreated), and the
     * stream is not re-{@code start()}ed.
     */
    public void testSendArrowBatchReusesExistingRoot() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);

            ch.sendArrowBatch(ByteBuffer.allocate(0), newSingleIntRoot(realAllocator, 1), null);
            VectorSchemaRoot firstRoot = ch.getRoot();
            ch.sendArrowBatch(ByteBuffer.allocate(0), newSingleIntRoot(realAllocator, 2), null);

            assertSame("second frame must reuse the adopted root", firstRoot, ch.getRoot());
            verify(listener, times(1)).start(any(VectorSchemaRoot.class)); // start only on the first frame
            verify(listener, times(2)).putNext();
            assertEquals(2, ((IntVector) ch.getRoot().getVector("v")).get(0));

            ch.close();
            assertEquals(0, realAllocator.getAllocatedMemory());
        }
    }

    /**
     * Adopt-then-throw: when the channel has adopted the stream root (assigned {@code root} and
     * called {@code start()}) and then {@code putNext()} throws, {@code sendArrowBatch} must NOT free
     * the root — the channel owns it and frees it in {@code close()}. Asserts the root stays allocated
     * after the throw, then is released exactly once by {@code close()}.
     */
    public void testSendArrowBatchAdoptThenThrowLeavesRootForChannelClose() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            doThrow(new RuntimeException("putNext failed after adoption")).when(listener).putNext();

            VectorSchemaRoot producerRoot = newSingleIntRoot(realAllocator, 3);
            expectThrows(RuntimeException.class, () -> ch.sendArrowBatch(ByteBuffer.allocate(0), producerRoot, null));

            // Channel adopted the root before putNext threw, so it must still hold (and not have freed) it.
            assertNotNull("channel must have adopted the root before the throw", ch.getRoot());
            assertTrue("adopted root must stay allocated (channel owns the close)", realAllocator.getAllocatedMemory() > 0);

            ch.close();
            assertEquals("channel close must free the adopted root with no leak/double-free", 0, realAllocator.getAllocatedMemory());
        }
    }

    /**
     * Pre-adoption throw via the empty-vectors guard: if {@code sendArrowBatch} fails before it even
     * creates a stream root (a producer root with no field vectors), there is nothing to free and the
     * channel must not have adopted anything.
     */
    public void testSendArrowBatchEmptyVectorsThrowsBeforeCreate() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);

            VectorSchemaRoot emptyProducer = VectorSchemaRoot.create(new Schema(List.of()), realAllocator);
            expectThrows(IllegalStateException.class, () -> ch.sendArrowBatch(ByteBuffer.allocate(0), emptyProducer, null));

            assertNull("channel must not have adopted a root on pre-adoption failure", ch.getRoot());
            verify(listener, never()).start(any(VectorSchemaRoot.class));
            emptyProducer.close();
            assertEquals("no buffers should leak after a pre-adoption failure", 0, realAllocator.getAllocatedMemory());
            ch.close();
        }
    }

    /**
     * Pre-adoption throw AFTER the root is created: the channel creates the first-frame root and
     * transfers the producer's buffers into it, but {@code sendBatch} throws at its closed-channel
     * guard before adopting (here we close the channel first). The created root holds the transferred
     * buffers and is never adopted, so {@code sendArrowBatch} must free it ({@code NativeArrow.close()}
     * is a no-op, so otherwise it would leak). This is the orphan-root case the channel-owns design
     * must handle.
     */
    public void testSendArrowBatchCreatedButNotAdoptedFreesRoot() throws Exception {
        try (RootAllocator realAllocator = new RootAllocator()) {
            FlightServerChannel ch = new FlightServerChannel(listener, realAllocator, middleware, callTracker, executor, 5_000);
            ch.close(); // flips open=false so sendBatch throws at its guard before adopting the root

            VectorSchemaRoot producerRoot = newSingleIntRoot(realAllocator, 5);
            expectThrows(IllegalStateException.class, () -> ch.sendArrowBatch(ByteBuffer.allocate(0), producerRoot, null));

            assertNull("channel must not have adopted a root (sendBatch threw at its guard)", ch.getRoot());
            // The created root carried the transferred buffers and was freed by sendArrowBatch's cleanup.
            producerRoot.close();
            assertEquals("orphaned created root must be freed (no leak)", 0, realAllocator.getAllocatedMemory());
        }
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
}
