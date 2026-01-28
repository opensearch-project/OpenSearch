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

import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.CORRELATION_ID_KEY;

/**
 * Arrow Flight implementation of streaming transport responses.
 *
 * <p>
 * Handles streaming responses from Arrow Flight servers with lazy batch
 * processing.
 * Headers are extracted when first accessed, and responses are deserialized on
 * demand.

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
    private final FlightTransportConfig config;
    private final long correlationId;

    private volatile FlightStream flightStream;
    private volatile long currentBatchSize;
    private volatile boolean firstBatchConsumed;
    private volatile boolean closed;
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

            VectorSchemaRoot root = flightStream.getRoot();
            currentBatchSize = FlightUtils.calculateVectorSchemaRootSize(root);
            try (VectorStreamInput input = new VectorStreamInput(root, namedWriteableRegistry)) {
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

        if (flightStream != null) {
            try {
                flightStream.close();
            } catch (IllegalStateException ignore) {} catch (Exception e) {
                throw new StreamException(StreamErrorCode.INTERNAL, "Error closing flight stream", e);
            }
            flightStream.close();
        } catch (IllegalStateException ignore) {
            // this is fine if the allocator is already closed
        } catch (Exception e) {
            throw new StreamException(StreamErrorCode.INTERNAL, "Error while closing flight stream", e);
        } finally {
            isClosed = true;
        }
    }

    public TransportResponseHandler<T> getHandler() {
        return handler;
    }

    /**
     * Initializes the stream by fetching the first batch to extract headers.
     */
    private synchronized void initializeStreamIfNeeded() {
        if (streamInitialized || streamExhausted) {
            return;
        }
        long startTime = System.currentTimeMillis();
        try {
            if (flightStream.next()) {
                currentRoot = flightStream.getRoot();
                currentHeader = headerContext.getHeader(correlationId);
                // Capture the batch size before deserialization
                currentBatchSize = FlightUtils.calculateVectorSchemaRootSize(currentRoot);
                streamInitialized = true;
            } else {
                streamExhausted = true;
            }
        } catch (FlightRuntimeException e) {
            // TODO maybe add a check - handshake and validate if node is connected
            // Try to get headers even if stream failed
            currentHeader = headerContext.getHeader(correlationId);
            streamExhausted = true;
            initializationException = FlightErrorMapper.fromFlightException(e);
            logger.warn("Stream initialization failed", e);
        } catch (Exception e) {
            // Try to get headers even if stream failed
            currentHeader = headerContext.getHeader(correlationId);
            streamExhausted = true;
            initializationException = new StreamException(StreamErrorCode.INTERNAL, "Stream initialization failed", e);
            logger.warn("Stream initialization failed", e);
        } finally {
            logSlowOperation(startTime);
        }
    }

    private T deserializeResponse() {
        try (VectorStreamInput input = new VectorStreamInput(currentRoot, namedWriteableRegistry)) {
            T response = handler.read(input);
            if (response instanceof org.opensearch.search.query.QuerySearchResult) {
                logger.info("Received QuerySearchResult hasAggs: {}", ((org.opensearch.search.query.QuerySearchResult) response).hasAggs());
            }
            return response;
        } catch (IOException e) {
            throw new StreamException(StreamErrorCode.INTERNAL, "Failed to deserialize response", e);
        }
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new StreamException(StreamErrorCode.UNAVAILABLE, "Stream is closed");
        }
    }

    private void logSlowOperation(long startTime) {
        long took = System.currentTimeMillis() - startTime;
        long thresholdMs = config.getSlowLogThreshold().millis();
        if (took > thresholdMs) {
            logger.warn("Flight stream next() took [{}ms], exceeding threshold [{}ms]", took, thresholdMs);
        }
    }
}
