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
    private final AtomicBoolean streamClosed = new AtomicBoolean(false);
    private volatile boolean prefetchStarted;
    private volatile Header initialHeader;

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
                    // close() may have run while we were inside getStream() and missed the stream because
                    // flightStream was still null. Now that it is published, re-check the flag: if a close()
                    // already happened, self-close the stream we just opened so the prefetched first-batch
                    // root is not stranded, then abort. This check is performed *before* future.complete(),
                    // so once the future completes, any subsequent close() always observes flightStream != null
                    // and owns the close itself. There is no post-completion window in which both this thread
                    // and a racing close() could close the same stream.
                    if (closed) {
                        try {
                            closeStreamQuietly();
                        } catch (StreamException e) {
                            logger.warn("Error closing flight stream after close() raced the prefetch", e);
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
                }
            });
        }
    }

    TransportResponseHandler<T> getHandler() {
        return handler;
    }

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
                logger.warn("Flight stream next() took [{}ms], exceeding threshold [{}ms]", took, config.getSlowLogThreshold().millis());
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
            logger.warn("Error cancelling flight stream", e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        closeStreamQuietly();
    }

    private void closeStreamQuietly() {
        FlightStream stream = flightStream;
        if (stream != null && streamClosed.compareAndSet(false, true)) {
            try {
                stream.close();
            } catch (IllegalStateException ignore) {} catch (Exception e) {
                throw new StreamException(StreamErrorCode.INTERNAL, "Error closing flight stream", e);
            }
        }
    }
}
