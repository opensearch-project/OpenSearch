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

import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.CORRELATION_ID_KEY;

/**
 * Arrow Flight implementation of streaming transport responses.
 *
 * <p>Handles streaming responses from Arrow Flight servers with lazy batch processing.
 * Headers are extracted when first accessed, and responses are deserialized on demand.
 */
class FlightTransportResponse<T extends TransportResponse> implements StreamTransportResponse<T> {
    private static final Logger logger = LogManager.getLogger(FlightTransportResponse.class);

    private final FlightStream flightStream;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final HeaderContext headerContext;
    private final long correlationId;
    private final FlightTransportConfig config;

    private final TransportResponseHandler<T> handler;
    private boolean isClosed;

    // Stream state
    private VectorSchemaRoot currentRoot;
    private Header currentHeader;
    private boolean streamInitialized = false;
    private boolean streamExhausted = false;
    private boolean firstResponseConsumed = false;
    private StreamException initializationException;
    private long currentBatchSize;

    /**
     * Creates a new Flight transport response.
     */
    public FlightTransportResponse(
        TransportResponseHandler<T> handler,
        long correlationId,
        FlightClient flightClient,
        HeaderContext headerContext,
        Ticket ticket,
        NamedWriteableRegistry namedWriteableRegistry,
        FlightTransportConfig config
    ) {
        this.handler = handler;
        this.correlationId = correlationId;
        this.headerContext = Objects.requireNonNull(headerContext, "headerContext must not be null");
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.config = config;
        // Initialize Flight stream with correlation ID header
        FlightCallHeaders callHeaders = new FlightCallHeaders();
        callHeaders.insert(CORRELATION_ID_KEY, String.valueOf(correlationId));
        HeaderCallOption callOptions = new HeaderCallOption(callHeaders);
        this.flightStream = flightClient.getStream(ticket, callOptions);

        this.isClosed = false;
    }

    /**
     * Gets the header for the current batch.
     * If no batch has been fetched yet, fetches the first batch to extract headers.
     */
    public Header getHeader() {
        ensureOpen();
        initializeStreamIfNeeded();
        return currentHeader;
    }

    /**
     * Gets the next response from the stream.
     */
    @Override
    public T nextResponse() {
        ensureOpen();
        initializeStreamIfNeeded();

        if (streamExhausted) {
            if (initializationException != null) {
                throw initializationException;
            }
            return null;
        }

        long startTime = System.currentTimeMillis();
        try {
            if (!firstResponseConsumed) {
                // First call - use the batch we already fetched during initialization
                firstResponseConsumed = true;
                return deserializeResponse();
            }

            if (flightStream.next()) {
                currentRoot = flightStream.getRoot();
                currentHeader = headerContext.getHeader(correlationId);
                // Capture the batch size before deserialization
                currentBatchSize = FlightUtils.calculateVectorSchemaRootSize(currentRoot);
                return deserializeResponse();
            } else {
                streamExhausted = true;
                return null;
            }
        } catch (FlightRuntimeException e) {
            streamExhausted = true;
            throw FlightErrorMapper.fromFlightException(e);
        } catch (Exception e) {
            streamExhausted = true;
            throw new StreamException(StreamErrorCode.INTERNAL, "Failed to fetch next batch", e);
        } finally {
            logSlowOperation(startTime);
        }
    }

    /**
     * Gets the size of the current batch in bytes.
     *
     * @return the size in bytes, or 0 if no batch is available
     */
    public long getCurrentBatchSize() {
        return currentBatchSize;
    }

    /**
     * Cancels the Flight stream.
     */
    @Override
    public void cancel(String reason, Throwable cause) {
        if (isClosed) {
            return;
        }
        try {
            flightStream.cancel(reason, cause);
            logger.debug("Cancelled flight stream: {}", reason);
        } catch (Exception e) {
            logger.warn("Error cancelling flight stream", e);
        } finally {
            close();
        }
    }

    /**
     * Closes the Flight stream and releases resources.
     */
    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        try {
            if (currentRoot != null) {
                currentRoot.close();
                currentRoot = null;
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
            return handler.read(input);
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
