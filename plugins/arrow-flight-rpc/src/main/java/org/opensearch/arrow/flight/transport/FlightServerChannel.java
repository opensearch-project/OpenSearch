/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.BackpressureStrategy;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.OSFlightListeners;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.arrow.transport.VectorTransfer;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.opensearch.arrow.flight.transport.FlightErrorMapper.mapFromCallStatus;

/**
 * TcpChannel implementation for Arrow Flight. Created per call in {@link ArrowFlightProducer}.
 *
 * <p>Honours gRPC's {@code isReady()} contract via {@link CompositeBackpressureStrategy}:
 * producer threads call {@link #awaitReadyOrThrow()} before a batch is submitted and park
 * until gRPC's outbound buffer drains below {@code setOnReadyThreshold}.
 *
 * <p><b>Ownership &amp; concurrency — single-writer model.</b> This channel is the sole owner of
 * every Arrow buffer it sends and of the gRPC stream lifecycle.
 *
 * <p><b>Invariant: this channel is NOT thread-safe. All send-side mutation — the stream {@link #root}
 * (create/transfer/serialize/free), {@link #terminalSent}, and every {@code serverStreamListener} call
 * — MUST run on the channel's single {@link #getExecutor() flight-executor thread}, and never
 * concurrently from more than one thread.</b> The executor is single-threaded, so it serializes those
 * mutations and the buffer frees among themselves. Violating this (e.g. mutating the root from a
 * producer thread while {@code close()} frees it on the executor) is a data race that corrupts Arrow
 * reference counts and can orphan or double-free buffers (an off-heap leak). It keeps that ownership
 * safe by confining the stream root to that single thread:
 * <ul>
 *   <li><b>The stream {@link #root} and every {@code serverStreamListener} call
 *       ({@code start}/{@code putNext}/{@code completed}/{@code error}) happen only on the channel's
 *       single-threaded flight executor</b> ({@link #getExecutor()}). The executor serializes batch
 *       sends, {@code completeStream}/{@code sendError}, and the root free among themselves, so none
 *       can interleave.</li>
 *   <li>{@link #close()} may be called from any thread (the gRPC cancel callback, the flight executor
 *       via {@code release()}, or producer bootstrap). It is a <em>signal</em>, not a buffer
 *       operation: it flips {@link #open} exactly once via {@code compareAndSet}, fires close
 *       listeners, and <b>posts the root free onto the flight executor</b>. Because the executor is
 *       FIFO and single-threaded, that free runs only after any in-flight send completes.
 *       {@code close()} never touches Arrow buffers or gRPC from the caller's thread, so it cannot
 *       race an in-flight {@code putNext} and never blocks the cancel thread.</li>
 *   <li>A native source root is freed exactly once by {@link #sendBatch} (in a {@code finally} on
 *       every exit), or by {@link #releaseUnsent} when the batch never reached {@code sendBatch}.</li>
 * </ul>
 * The only cross-thread state is the close-listener list, guarded by a small leaf mutex
 * ({@link #closeListenerMutex}) whose critical section only mutates that list. See
 * {@code docs/channel-lifecycle-ownership.md} for the full contract.
 *
 * <p>The producer must invoke {@link #sendBatch} serially and finish with {@link #completeStream}
 * (success) or {@link #sendError} (failure).
 */
class FlightServerChannel implements TcpChannel, ArrowFlightChannel {
    private static final String PROFILE_NAME = "flight";

    private final Logger logger = LogManager.getLogger(FlightServerChannel.class);
    private final ServerStreamListener serverStreamListener;
    private final BufferAllocator allocator;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final InetSocketAddress localAddress;
    private final InetSocketAddress remoteAddress;
    private final ServerHeaderMiddleware middleware;
    private final FlightCallTracker callTracker;
    private volatile boolean cancelled = false;
    private final ExecutorService executor;
    private final long correlationId;
    private final AtomicInteger batchNumber = new AtomicInteger(0);
    private final CompositeBackpressureStrategy bp;
    private final long readyTimeoutMillis;
    /** Applies the per-stream outbound buffer threshold (bytes) to the gRPC call. */
    private final IntConsumer outboundThresholdSetter;

    /**
     * The reused stream root (native or byte-serialized). Confined to the flight-executor thread:
     * created, filled, and freed only there. {@code volatile} so the test-only {@link #getRoot()}
     * sees a consistent reference; correctness relies on executor confinement, not on volatility.
     */
    private volatile VectorSchemaRoot root = null;
    /** Whether a terminal gRPC op ({@code completed()} XOR {@code error()}) was issued. Flight-executor confined. */
    private boolean terminalSent = false;

    /** Leaf mutex guarding {@link #closeListeners} and {@link #closeListenersFired}; its critical section only mutates them. */
    private final Object closeListenerMutex = new Object();
    private final List<ActionListener<Void>> closeListeners = new ArrayList<>();
    private boolean closeListenersFired = false;

    public FlightServerChannel(
        ServerStreamListener serverStreamListener,
        BufferAllocator allocator,
        ServerHeaderMiddleware middleware,
        FlightCallTracker callTracker,
        ExecutorService executor,
        long readyTimeoutMillis
    ) {
        this(
            serverStreamListener,
            allocator,
            middleware,
            callTracker,
            executor,
            readyTimeoutMillis,
            bytes -> OSFlightListeners.setOnReadyThreshold(serverStreamListener, bytes)
        );
    }

    /** Visible for testing: injects the outbound-threshold applier in place of the gRPC-backed one. */
    FlightServerChannel(
        ServerStreamListener serverStreamListener,
        BufferAllocator allocator,
        ServerHeaderMiddleware middleware,
        FlightCallTracker callTracker,
        ExecutorService executor,
        long readyTimeoutMillis,
        IntConsumer outboundThresholdSetter
    ) {
        this.correlationId = Long.parseLong(middleware.getCorrelationId());
        logger.debug("Creating FlightServerChannel for correlation ID: {}", correlationId);
        this.serverStreamListener = serverStreamListener;
        this.serverStreamListener.setUseZeroCopy(true);
        this.outboundThresholdSetter = outboundThresholdSetter;
        this.allocator = allocator;
        this.middleware = middleware;
        this.callTracker = callTracker;
        this.executor = executor;
        this.readyTimeoutMillis = readyTimeoutMillis;
        this.localAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        this.remoteAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        // CompositeBackpressureStrategy.register installs both setOnReadyHandler and
        // setOnCancelHandler; the cancel callback runs onChannelCancelled before
        // notifying parked threads so the cancelled state is visible on wake.
        this.bp = new CompositeBackpressureStrategy(this::onChannelCancelled);
        this.bp.register(serverStreamListener);
    }

    /**
     * Sets this stream's outbound buffer watermark: the buffered-bytes count at which gRPC's {@code isReady()}
     * flips false so the producer parks. Applies to this stream only. A non-positive value is ignored, leaving
     * the transport-wide default in effect.
     *
     * @param bytes the per-stream watermark in bytes; ignored if {@code <= 0}
     */
    public void setOutboundBufferThreshold(int bytes) {
        if (bytes > 0) {
            outboundThresholdSetter.accept(bytes);
        }
    }

    /**
     * Parks the calling thread until gRPC signals it can accept another batch. Called
     * from the producer thread before the batch is submitted to the channel's executor.
     *
     * <p><b>Warning:</b> the calling thread may park for up to {@code readyTimeoutMillis}
     * under a slow consumer. If the action handler is registered on a bounded thread
     * pool (e.g. {@code SEARCH}), N concurrent slow streams will hold N threads parked
     * simultaneously and can starve the pool. Operators should size the action's thread
     * pool — or limit concurrent streams via admission control — accordingly.
     *
     * @throws StreamException with {@link StreamErrorCode#TIMED_OUT} if the consumer
     *         remains not-ready longer than {@code readyTimeoutMillis}, or
     *         {@link StreamErrorCode#CANCELLED} if the client cancelled.
     */
    public void awaitReadyOrThrow() {
        if (cancelled) {
            throw StreamException.cancelled("stream cancelled before back-pressure wait");
        }
        BackpressureStrategy.WaitResult result = bp.waitForListener(readyTimeoutMillis);
        switch (result) {
            case READY:
                return;
            case CANCELLED:
                throw StreamException.cancelled("stream cancelled while waiting for consumer");
            case TIMEOUT:
                throw new StreamException(StreamErrorCode.TIMED_OUT, "consumer not ready after " + readyTimeoutMillis + "ms");
            default:
                logger.warn("unexpected back-pressure wait result: {}", result);
                throw new StreamException(StreamErrorCode.INTERNAL, "unexpected back-pressure wait result: " + result);
        }
    }

    /** Idempotent cleanup invoked from the strategy when gRPC fires {@code OnCancelHandler}. */
    private void onChannelCancelled() {
        if (!cancelled) {
            cancelled = true;
            callTracker.recordCallEnd(StreamErrorCode.CANCELLED.name());
            close();
        }
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    /** Whether the client cancelled the gRPC stream (onChannelCancelled fired). Read by FlightTransportChannel. */
    public boolean isCancelled() {
        return cancelled;
    }

    /** Returns the current stream root. Package-private; intended for tests/assertions only. */
    VectorSchemaRoot getRoot() {
        return root;
    }

    /**
     * Gets the executor for this channel
     */
    public ExecutorService getExecutor() {
        return executor;
    }

    /**
     * Sends one streaming batch. <b>Must run on the flight executor thread</b> (it is dispatched there
     * by {@link FlightOutboundHandler}). The channel takes ownership of the response's buffers and is
     * the sole place they are transferred onto the wire and freed.
     *
     * <p>For an {@link ArrowBatchResponse} (native path) the producer's root is zero-copy transferred
     * into the reused stream root and the producer root is freed here exactly once (on every exit, via
     * the {@code finally}). For any other response (byte-serialized path) the response is written into
     * the reused {@code VarBinary} stream root. The stream root itself is freed once, at {@link #close()}
     * — never per batch — to preserve gRPC's zero-copy retention window.
     *
     * <p>This method, the terminal ops, and the {@link #close()} root-free all run on the
     * single-threaded executor and are serialized by it. The header is built before any root is
     * mutated, so a header-serialization failure fails fast (the source is freed by the {@code finally})
     * without adopting a stream root.
     *
     * @param response the batch to send; the channel takes ownership of its buffers
     * @param headerSupplier builds the response header
     * @throws IOException if header serialization or byte-path serialization fails
     */
    public void sendBatch(TransportResponse response, CheckedSupplier<ByteBuffer, IOException> headerSupplier) throws IOException {
        final boolean isNative = response instanceof ArrowBatchResponse;
        final VectorSchemaRoot sourceRoot = isNative ? ((ArrowBatchResponse) response).getRoot() : null;
        final byte[] metadata = isNative ? ((ArrowBatchResponse) response).getMetadata() : null;
        try {
            if (cancelled) {
                throw StreamException.cancelled("Cannot flush more batches. Stream cancelled by the client");
            }
            if (!open.get()) {
                throw new IllegalStateException("FlightServerChannel already closed.");
            }

            final boolean firstBatch = (root == null);
            final long batchStartTime = System.nanoTime();

            // Build the header before mutating any root so a header-serialization failure fails fast
            // (the source root is still freed by the finally) without adopting a stream root.
            final ByteBuffer header = headerSupplier.get();

            if (isNative) {
                transferIntoStreamRoot(sourceRoot, firstBatch);
            } else {
                serializeIntoStreamRoot(response, firstBatch);
            }

            batchNumber.incrementAndGet();
            if (firstBatch) {
                middleware.setHeader(header);
                serverStreamListener.start(root);
            }

            logger.debug("Sending batch #{} for correlation ID: {}", batchNumber, correlationId);
            if (metadata != null) {
                // Flight takes ownership of metadataBuf via putNext(ArrowBuf); free it ourselves
                // only if putNext never adopted it.
                ArrowBuf metadataBuf = allocator.buffer(metadata.length);
                metadataBuf.writeBytes(metadata);
                try {
                    serverStreamListener.putNext(metadataBuf);
                } catch (Throwable t) {
                    if (metadataBuf.refCnt() > 0) {
                        metadataBuf.close();
                    }
                    throw t;
                }
            } else {
                serverStreamListener.putNext();
            }

            long putNextTime = (System.nanoTime() - batchStartTime) / 1_000_000;
            if (callTracker != null) {
                long rootSize = FlightUtils.calculateVectorSchemaRootSize(root);
                callTracker.recordBatchSent(rootSize, System.nanoTime() - batchStartTime);
                logger.debug(
                    "Batch #{} sent for correlation ID: {}, size: {} bytes, putNext: {}ms",
                    batchNumber,
                    correlationId,
                    rootSize,
                    putNextTime
                );
            } else {
                logger.debug("Batch #{} sent for correlation ID: {}, putNext: {}ms", batchNumber, correlationId, putNextTime);
            }
        } finally {
            // The native source root is exclusively owned by this call. Free it exactly once on every
            // exit: after a successful transfer it is empty (close is a no-op); on any early throw it
            // still holds its buffers (close frees them).
            if (isNative && sourceRoot != null) {
                sourceRoot.close();
            }
        }
    }

    /**
     * Native path: zero-copy transfers {@code sourceRoot}'s vectors into the reused stream root,
     * creating it on the first batch. On the first batch, if transfer fails before the channel adopts
     * the freshly-created root, that root is freed here so it cannot leak (it has not yet become
     * {@link #root}, so {@link #close()} would not free it). Flight-executor confined.
     */
    private void transferIntoStreamRoot(VectorSchemaRoot sourceRoot, boolean firstBatch) {
        if (firstBatch) {
            List<FieldVector> fieldVectors = sourceRoot.getFieldVectors();
            if (fieldVectors.isEmpty()) {
                throw new IllegalStateException("Native Arrow batch has no field vectors");
            }
            // Create using the producer's allocator: cross-allocator transferOwnership of foreign-backed
            // buffers (from C data import) does not properly free the ArrowArray C struct. The producer's
            // allocator must be long-lived (not closed per-request).
            VectorSchemaRoot created = VectorSchemaRoot.create(sourceRoot.getSchema(), fieldVectors.getFirst().getAllocator());
            boolean adopted = false;
            try {
                VectorTransfer.transferRoot(sourceRoot, created);
                root = created;
                adopted = true;
            } finally {
                if (!adopted) {
                    created.close();
                }
            }
        } else {
            VectorTransfer.transferRoot(sourceRoot, root);
        }
    }

    /**
     * Byte-serialized path: writes {@code response} into the reused {@code VarBinary} stream root,
     * creating it on the first batch and clearing it for reuse on later batches. On the first batch,
     * if serialization fails before the channel adopts the freshly-created root, the output's vector is
     * freed here so it cannot leak. Flight-executor confined.
     */
    private void serializeIntoStreamRoot(TransportResponse response, boolean firstBatch) throws IOException {
        VectorStreamOutput out = VectorStreamOutput.create(allocator, root);
        boolean adopted = false;
        try {
            if (!firstBatch) {
                out.reset();
            }
            response.writeTo(out);
            root = out.getRoot();
            adopted = true;
        } finally {
            // Only the first-batch output owns a freshly-allocated vector; on reuse the output wraps the
            // channel-owned root, which close() frees.
            if (firstBatch && !adopted) {
                try {
                    out.close();
                } catch (IOException ignore) {
                    // best-effort cleanup of the un-adopted first-batch vector
                }
            }
        }
    }

    /**
     * Frees the source buffers of a batch that was built but never handed to {@link #sendBatch}
     * (the back-pressure gate threw, the executor rejected the task, or the channel was the wrong
     * type). The channel owns the source root from the moment the producer enqueues the batch, so on a
     * failed hand-off it is freed here — the mutually-exclusive counterpart to {@link #sendBatch}'s own
     * {@code finally}. No-op for the byte-serialized path (no off-heap source root).
     */
    public void releaseUnsent(TransportResponse response) {
        releaseUnsentSource(response);
    }

    /**
     * Static variant of {@link #releaseUnsent} for the defensive path where the channel is the wrong
     * type and no instance is available. Frees the native source root of an unsent batch; no-op for the
     * byte-serialized path (no off-heap source root).
     */
    static void releaseUnsentSource(TransportResponse response) {
        if (response instanceof ArrowBatchResponse arrowResponse) {
            VectorSchemaRoot sourceRoot = arrowResponse.getRoot();
            if (sourceRoot != null) {
                sourceRoot.close();
            }
        }
    }

    /**
     * Completes the streaming response. <b>Must run on the flight executor thread.</b> Terminal and
     * idempotent: a no-op if the channel is already closed, cancelled, or terminal. Does not free the
     * stream root — that is deferred to {@link #close()} to preserve gRPC's zero-copy retention window.
     */
    public void completeStream(ByteBuffer header) {
        if (!open.get() || cancelled || terminalSent) {
            logger.debug(
                "completeStream is a no-op (open={}, cancelled={}, terminalSent={}) for correlation ID: {}",
                open.get(),
                cancelled,
                terminalSent,
                correlationId
            );
            return;
        }
        if (root == null) {
            // Set header if no batches were sent
            middleware.setHeader(header);
            logger.debug("Completing empty stream for correlation ID: {}", correlationId);
        } else {
            logger.debug("Completing stream for correlation ID: {} after {} batches", correlationId, batchNumber);
        }
        serverStreamListener.completed();
        terminalSent = true;
        callTracker.recordCallEnd(StreamErrorCode.OK.name());
    }

    /**
     * Sends a terminal error to the consumer. <b>Must run on the flight executor thread.</b> Terminal
     * and idempotent: a no-op if the channel is already closed, cancelled, or terminal (the consumer is
     * then already done). Does not free the stream root — that is deferred to {@link #close()}.
     *
     * @param error the error to send
     */
    public void sendError(ByteBuffer header, Exception error) {
        if (!open.get() || cancelled || terminalSent) {
            logger.debug(
                "sendError is a no-op (open={}, cancelled={}, terminalSent={}) for correlation ID: {}",
                open.get(),
                cancelled,
                terminalSent,
                correlationId
            );
            return;
        }
        FlightRuntimeException flightExc;
        if (error instanceof FlightRuntimeException fre) {
            flightExc = fre;
        } else {
            flightExc = CallStatus.INTERNAL.withCause(error)
                .withDescription(error.getMessage() != null ? error.getMessage() : "Stream error")
                .toRuntimeException();
        }
        middleware.setHeader(header);
        if (error instanceof OpenSearchException) {
            logger.debug("Error in Flight stream: {}", error.getMessage());
        } else {
            logger.error("Unexpected error in Flight stream", error);
        }
        logger.debug("Sending error for correlation ID: {} after {} batches: {}", correlationId, batchNumber, error.getMessage());
        serverStreamListener.error(flightExc);
        terminalSent = true;
        callTracker.recordCallEnd(mapFromCallStatus(flightExc).name());
    }

    @Override
    public boolean isServerChannel() {
        return true;
    }

    @Override
    public String getProfile() {
        return PROFILE_NAME;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void sendMessage(BytesReference reference, ActionListener<Void> listener) {
        listener.onFailure(new UnsupportedOperationException("FlightServerChannel does not support BytesReference based sendMessage()"));
    }

    @Override
    public void addConnectListener(ActionListener<Void> listener) {
        // Assume Arrow Flight is connected
        listener.onResponse(null);
    }

    @Override
    public ChannelStats getChannelStats() {
        return new ChannelStats(); // TODO: Implement stats. Add custom stats as needed
    }

    @Override
    public void close() {
        // Fire-once: exactly one caller wins the CAS, regardless of thread (gRPC cancel, the flight
        // executor via release, or producer bootstrap) or interleaving.
        if (!open.compareAndSet(true, false)) {
            return;
        }
        // The stream root is owned exclusively by the flight-executor thread (created, filled, and
        // freed there). Post the free onto that executor so it is serialized behind any in-flight send
        // by the executor's own FIFO ordering; we never touch Arrow buffers from this (possibly gRPC)
        // thread.
        try {
            executor.execute(() -> {
                if (root != null) {
                    root.close();
                    root = null;
                }
            });
        } catch (RejectedExecutionException e) {
            // Executor shut down (node shutdown): the posted free will not run, so the stream root is
            // reclaimed by the OS on process exit (documented limitation, channel-lifecycle-ownership
            // §10). We deliberately do NOT free inline: a rejection from ExecutorService.shutdown()
            // means new tasks are refused while an already-running send may still be touching the root
            // on the executor thread, so freeing from this (possibly gRPC) thread would reintroduce the
            // use-after-free this single-writer model exists to prevent. Logged at WARN so the
            // shutdown-time leak is visible rather than buried.
            logger.warn(
                new ParameterizedMessage(
                    "flight executor rejected stream-root release for correlation ID: {}; root reclaimed at process exit",
                    correlationId
                ),
                e
            );
        }
        notifyCloseListeners();
    }

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        // Register atomically against notifyCloseListeners under the leaf mutex: either the listeners
        // have not fired yet and we enqueue (notifyCloseListeners will fire it), or they have already
        // fired and we fire immediately. Never both, never neither. The mutex's critical section only
        // mutates the list.
        boolean alreadyFired;
        synchronized (closeListenerMutex) {
            alreadyFired = closeListenersFired;
            if (!alreadyFired) {
                closeListeners.add(listener);
            }
        }
        if (alreadyFired) {
            listener.onResponse(null);
        }
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    private void notifyCloseListeners() {
        // Snapshot+mark-fired under the leaf mutex so a concurrent addCloseListener cannot be lost or
        // double-fired, then fire outside the mutex so foreign listener code never runs while we hold
        // it. Isolate each listener so one failure cannot strand the rest (e.g. the TaskManager untrack
        // listener).
        final List<ActionListener<Void>> toFire;
        synchronized (closeListenerMutex) {
            toFire = new ArrayList<>(closeListeners);
            closeListeners.clear();
            closeListenersFired = true;
        }
        for (ActionListener<Void> listener : toFire) {
            try {
                listener.onResponse(null);
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("close listener failed for correlation ID: {}", correlationId), e);
            }
        }
    }
}
