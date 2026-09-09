/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.Header;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.CORRELATION_ID_KEY;

/**
 * Streaming transport response implementation using Arrow Flight.
 * Manages Flight stream lifecycle with lazy initialization and prefetching support.
 */
class FlightTransportResponse<T extends TransportResponse> implements StreamTransportResponse<T> {
    private static final Logger logger = LogManager.getLogger(FlightTransportResponse.class);

    private final FlightClient flightClient;
    private final Ticket ticket;
    private final FlightCallHeaders callHeaders;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final HeaderContext headerContext;
    private final TransportResponseHandler<T> handler;
    private final boolean isNativeHandler;
    private final FlightTransportConfig config;
    private final long correlationId;

    private volatile FlightStream flightStream;
    private volatile long currentBatchSize;
    private volatile boolean firstBatchConsumed;
    private volatile boolean closed;
    private final Object streamCloseLock = new Object();
    /** Guarded by {@link #streamCloseLock}: set once a thread has entered {@code FlightStream#close}. */
    private boolean streamCloseAttempted;
    /** Guarded by {@link #streamCloseLock}: set when the stream's buffers are known to be released. */
    private boolean streamReleased;
    private volatile boolean prefetchStarted;
    private volatile Header initialHeader;

    /**
     * Notified exactly once, after the underlying flight stream (if any) has been closed and its
     * buffers returned to the allocator. Used by {@link FlightClientChannel} to track streams that
     * are still holding allocator memory, so channel close can wait for them before closing the
     * {@link FlightClient} (whose allocator close treats outstanding buffers as a leak).
     */
    private volatile Runnable onClosed;
    /** Ensures {@link #onClosed} runs exactly once. */
    private final AtomicBoolean closeNotified = new AtomicBoolean();
    /** Set when the open/prefetch task has finished (whether or not it published a stream). */
    private volatile boolean prefetchCompleted;
    /**
     * The thread currently executing the consumer's stream callback, or {@code null} when no
     * callback is in progress. A closer running on this thread cannot wait for this stream to be
     * released, because the release can only happen once the callback it is executing returns.
     */
    private volatile Thread dispatchThread;

    FlightTransportResponse(
        TransportResponseHandler<T> handler,
        long correlationId,
        FlightClient flightClient,
        HeaderContext headerContext,
        Ticket ticket,
        NamedWriteableRegistry namedWriteableRegistry,
        FlightTransportConfig config
    ) {
        this.handler = Objects.requireNonNull(handler);
        this.isNativeHandler = handler.skipsDeserialization();
        this.correlationId = correlationId;
        this.flightClient = Objects.requireNonNull(flightClient);
        this.headerContext = Objects.requireNonNull(headerContext);
        this.ticket = Objects.requireNonNull(ticket);
        this.namedWriteableRegistry = Objects.requireNonNull(namedWriteableRegistry);
        this.config = Objects.requireNonNull(config);
        this.callHeaders = new FlightCallHeaders();
        this.callHeaders.insert(CORRELATION_ID_KEY, String.valueOf(correlationId));
    }

    void openAndPrefetchAsync(CompletableFuture<Header> future) {
        if (prefetchStarted) return;

        synchronized (this) {
            if (prefetchStarted) return;
            if (closed) {
                future.completeExceptionally(new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed"));
                return;
            }

            prefetchStarted = true;

            Thread.ofVirtual().start(() -> {
                try {
                    long start = System.nanoTime();
                    flightStream = flightClient.getStream(ticket, new HeaderCallOption(callHeaders));
                    afterStreamPublished();
                    // close() may have run while we were inside getStream() and missed the stream because
                    // flightStream was still null. Now that it is published, re-check the flag: if a close()
                    // already happened, self-close the stream we just opened so the prefetched first-batch
                    // root is not stranded, then abort. This check is performed *before* future.complete(),
                    // so once the future completes, any subsequent close() always observes flightStream != null
                    // and owns the close itself.
                    //
                    // A close() can still slip in between publishing the stream and reading the flag here, in
                    // which case both paths reach releaseStream(); it closes the stream once and blocks the
                    // second caller until that close has completed.
                    if (closed) {
                        try {
                            // Notify only once the stream's buffers are actually released, so a
                            // channel waiting on stream shutdown does not proceed while they may
                            // still be allocated. releaseStream() blocks if a racing close() is
                            // the one performing the close, and reports whether it succeeded.
                            if (releaseStream()) {
                                notifyClosed();
                            }
                        } catch (StreamException e) {
                            logFailure("Error closing flight stream after close() raced the prefetch", e);
                        }
                        future.completeExceptionally(new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed"));
                        return;
                    }
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    logger.debug("FlightClient.getStream() for correlationId: {} took {}ms", correlationId, elapsedMs);
                    start = System.nanoTime();
                    flightStream.next();
                    elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    logger.debug("First FlightClient.next() for correlationId: {} took {}ms", correlationId, elapsedMs);
                    initialHeader = headerContext.getHeader(correlationId);
                    future.complete(initialHeader);
                } catch (FlightRuntimeException e) {
                    future.completeExceptionally(FlightErrorMapper.fromFlightException(e));
                } catch (Exception e) {
                    future.completeExceptionally(new StreamException(StreamErrorCode.INTERNAL, "Stream open/prefetch failed", e));
                } finally {
                    // Lets a racing close() distinguish "prefetch still running, it will handle
                    // the closed re-check" from "prefetch finished without publishing a stream
                    // (getStream failed), close() owns notification".
                    prefetchCompleted = true;
                    // A close() that raced this prefetch and found neither a published stream nor
                    // a completed prefetch deferred to us. If the stream never got published
                    // (getStream failed), there is nothing to close but the notification must
                    // still fire so a waiting channel is not left hanging. (No-op when the closed
                    // re-check or the racing close() already notified — notification is one-shot.)
                    if (closed && flightStream == null) {
                        notifyClosed();
                    }
                }
            });
        }
    }

    TransportResponseHandler<T> getHandler() {
        return handler;
    }

    /** Correlates this response with its request in logs and in {@link HeaderContext}. */
    long getCorrelationId() {
        return correlationId;
    }

    /**
     * Test seam, invoked after the stream is published and before the closed re-check below it —
     * the window in which a concurrent {@link #close()} can observe the published stream and reach
     * {@link #releaseStream()} first. No-op in production; overridden by tests that need to force
     * that interleaving, which is only a few instructions wide and cannot be hit reliably by racing.
     */
    void afterStreamPublished() {}

    @Override
    public T nextResponse() {
        if (closed) throw new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed");
        if (flightStream == null) throw new IllegalStateException("openAndPrefetch() must be called first");

        long startTime = System.currentTimeMillis();
        try {
            boolean hasNext = firstBatchConsumed ? flightStream.next() : (firstBatchConsumed = true);
            if (!hasNext) return null;

            VectorSchemaRoot streamRoot = flightStream.getRoot();
            currentBatchSize = FlightUtils.calculateVectorSchemaRootSize(streamRoot);
            // Flight owns getLatestMetadata()'s buffer until the next next() call;
            // we copy off so the response can outlive the stream cursor.
            byte[] metadata = readMetadata();
            try (VectorStreamInput input = newStreamInput(streamRoot, metadata)) {
                input.setVersion(initialHeader.getVersion());
                return handler.read(input);
            }
        } catch (FlightRuntimeException e) {
            throw FlightErrorMapper.fromFlightException(e);
        } catch (IOException e) {
            throw new StreamException(StreamErrorCode.INTERNAL, "Failed to deserialize batch", e);
        } finally {
            long took = System.currentTimeMillis() - startTime;
            if (took > config.getSlowLogThreshold().millis()) {
                logger.debug("Flight stream next() took [{}ms], exceeding threshold [{}ms]", took, config.getSlowLogThreshold().millis());
            }
            logger.debug("FlightClient.next() for correlationId: {} took {}ms", correlationId, took);
        }
    }

    long getCurrentBatchSize() {
        return currentBatchSize;
    }

    private VectorStreamInput newStreamInput(VectorSchemaRoot streamRoot, byte[] metadata) {
        return isNativeHandler
            ? VectorStreamInput.forNativeArrow(streamRoot, namedWriteableRegistry, metadata)
            : VectorStreamInput.forByteSerialized(streamRoot, namedWriteableRegistry);
    }

    private byte[] readMetadata() {
        return copyMetadata(flightStream.getLatestMetadata());
    }

    /**
     * Copies an Arrow Flight metadata buffer into a {@code byte[]} the consumer owns, or
     * returns {@code null} if the buffer is absent/empty. Package-private for testing.
     */
    static byte[] copyMetadata(ArrowBuf buf) {
        if (buf == null || buf.readableBytes() == 0) return null;
        int len = (int) buf.readableBytes();
        byte[] copy = new byte[len];
        buf.getBytes(0, copy);
        return copy;
    }

    @Override
    public void cancel(String reason, Throwable cause) {
        if (closed) return;
        try {
            if (flightStream != null) flightStream.cancel(reason, cause);
        } catch (Exception e) {
            logFailure("Error cancelling flight stream", e);
        } finally {
            close();
        }
    }

    /**
     * Sets the callback notified exactly once, after the underlying flight stream (if any) has been
     * closed and its buffers released. Must be set before the response is handed to a consumer.
     */
    void setOnClosed(Runnable onClosed) {
        assert closed == false : "onClosed callback must be set before the response is closed";
        this.onClosed = onClosed;
    }

    /**
     * Marks the calling thread as the one executing the consumer's stream callback, and returns a
     * handle that clears the mark. Used by {@link FlightClientChannel#close()} to recognise streams
     * it cannot wait for: a close running on the dispatch thread would block until the timeout,
     * because the stream can only be released once the callback it is executing returns.
     */
    Releasable markDispatchThread() {
        dispatchThread = Thread.currentThread();
        return () -> dispatchThread = null;
    }

    /**
     * The thread currently executing the consumer's stream callback, or {@code null} if none is.
     */
    Thread getDispatchThread() {
        return dispatchThread;
    }

    /**
     * Requests cancellation of the underlying flight stream without closing it. Safe to call from
     * any thread: {@link FlightStream#cancel} unblocks a consumer parked in {@link #nextResponse()}
     * (it observes a CANCELLED error), and that consumer remains responsible for {@link #close()}.
     * Closing here instead would race with a consumer concurrently reading the stream's root.
     *
     * <p>If the stream has not been published yet (open/prefetch still in flight, or never started),
     * this falls back to {@link #close()}: the prefetch's closed re-check self-closes the stream it
     * publishes, so no consumer-owned resource is torn down from under a reader.
     */
    void cancelStreamOnly(String reason) {
        FlightStream stream = flightStream;
        if (stream == null) {
            close();
            return;
        }
        try {
            stream.cancel(reason, null);
        } catch (Exception e) {
            logFailure("Error requesting flight stream cancellation", e);
        }
    }

    /**
     * Logs a one-line summary at WARN, with the full stack trace only at TRACE.
     *
     * <p>Every path in this class can run on the per-stream prefetch virtual thread started by
     * {@link #openAndPrefetchAsync}. Handing a throwable to log4j there risks pinning the carrier inside its
     * extended stack-trace renderer, which can stall the virtual-thread scheduler under a mass stream failure.
     * Both lines below pass strings only, so the renderer never runs at any level. See
     * {@code FlightClientChannel#logFailure} for the full mechanism.
     */
    private void logFailure(String message, Throwable cause) {
        logger.warn("{} for correlationId [{}]: {}", message, correlationId, FlightUtils.causeSummary(cause));
        if (logger.isTraceEnabled()) {
            logger.trace("{} for correlationId [{}]: {}", message, correlationId, ExceptionsHelper.stackTrace(cause));
        }
    }

    @Override
    public void close() {
        final boolean hadPrefetch;
        // Decide who owns stream shutdown under the same lock openAndPrefetchAsync uses, so a
        // prefetch cannot start after close() concluded none was running.
        synchronized (this) {
            if (closed) return;
            closed = true;
            hadPrefetch = prefetchStarted;
        }
        if (hadPrefetch == false) {
            // No stream was ever opened and none can start now (closed is set under the lock).
            notifyClosed();
            return;
        }
        if (flightStream != null || prefetchCompleted) {
            // Stream published (release it), or the prefetch finished without publishing one
            // (getStream failed — nothing to release). Notify only once the buffers are known to be
            // released; a failed close may still retain them and must remain tracked by the channel.
            if (releaseStream()) {
                notifyClosed();
            }
        }
        // else: the prefetch is still in flight; its closed re-check sees closed == true and
        // self-closes the stream it publishes, then notifies.
    }

    /** Runs the {@link #onClosed} callback exactly once, if set. */
    private void notifyClosed() {
        Runnable callback = onClosed;
        if (callback != null && closeNotified.compareAndSet(false, true)) {
            callback.run();
        }
    }

    /**
     * Closes the underlying flight stream at most once, and does not return until the close has
     * completed — including when another thread is the one performing it. That makes a normal
     * return a reliable statement about allocator state: {@code true} means the stream's buffers
     * have been released, so it is safe to notify a channel that is waiting to close its client.
     *
     * <p>Both {@link #close()} and the prefetch's closed re-check can reach this method for the
     * same stream (they write {@code flightStream} and {@code closed} in opposite orders, so each
     * may observe the other's write). Blocking the loser rather than letting it return early is
     * what prevents it from reporting the stream released while the winner is still inside
     * {@link FlightStream#close()}.
     *
     * @return true if the stream's buffers are known to be released, false if the close failed
     * @throws StreamException if this thread's close attempt failed unexpectedly
     */
    private boolean releaseStream() {
        FlightStream stream = flightStream;
        if (stream == null) {
            // Nothing was ever published, so nothing is holding allocator memory.
            return true;
        }
        synchronized (streamCloseLock) {
            if (streamCloseAttempted == false) {
                streamCloseAttempted = true;
                try {
                    stream.close();
                    streamReleased = true;
                } catch (IllegalStateException ignore) {
                    // Already closed underneath us; the buffers are gone either way.
                    streamReleased = true;
                } catch (Exception e) {
                    throw new StreamException(StreamErrorCode.INTERNAL, "Error closing flight stream", e);
                }
            }
            return streamReleased;
        }
    }
}
