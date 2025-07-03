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
import org.opensearch.arrow.flight.stats.FlightStatsCollector;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.Header;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Handles streaming transport responses using Apache Arrow Flight.
 * Lazily fetches batches from the server when requested.
 */
class FlightTransportResponse<T extends TransportResponse> implements StreamTransportResponse<T> {
    private static final Logger logger = LogManager.getLogger(FlightTransportResponse.class);
    private final FlightStream flightStream;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final HeaderContext headerContext;
    private TransportResponseHandler<T> handler;
    private boolean isClosed;
    private Throwable pendingException;
    private VectorSchemaRoot pendingRoot;  // Holds the current batch's root for reuse
    private final long reqId;
    private final FlightStatsCollector statsCollector;

    /**
     * Constructs a new streaming response. The flight stream is initialized asynchronously
     * to avoid blocking during construction.
     *
     * @param reqId                  the request ID
     * @param flightClient           the Arrow Flight client
     * @param headerContext          the context containing header information
     * @param ticket                 the ticket for fetching the stream
     * @param namedWriteableRegistry the registry for deserialization
     */
    public FlightTransportResponse(
        long reqId,
        FlightClient flightClient,
        HeaderContext headerContext,
        Ticket ticket,
        NamedWriteableRegistry namedWriteableRegistry,
        FlightStatsCollector statsCollector
    ) {
        this.reqId = reqId;
        this.statsCollector = statsCollector;
        FlightCallHeaders callHeaders = new FlightCallHeaders();
        callHeaders.insert("req-id", String.valueOf(reqId));
        HeaderCallOption callOptions = new HeaderCallOption(callHeaders);
        this.flightStream = Objects.requireNonNull(flightClient, "flightClient must not be null")
            .getStream(Objects.requireNonNull(ticket, "ticket must not be null"), callOptions);
        this.headerContext = Objects.requireNonNull(headerContext, "headerContext must not be null");
        this.namedWriteableRegistry = Objects.requireNonNull(namedWriteableRegistry, "namedWriteableRegistry must not be null");
        this.isClosed = false;
        this.pendingException = null;
        this.pendingRoot = null;
    }

    /**
     * Sets the handler for deserializing responses.
     *
     * @param handler the response handler
     * @throws IllegalStateException if the handler is already set or the stream is closed
     */
    public void setHandler(TransportResponseHandler<T> handler) {
        ensureOpen();
        if (this.handler != null) {
            throw new IllegalStateException("Handler already set");
        }
        this.handler = Objects.requireNonNull(handler, "handler must not be null");
    }

    /**
     * Retrieves the next response from the stream. This may block if the server
     * is still producing data, depending on the backpressure strategy.
     *
     * @return the next response, or null if no more responses are available
     * @throws IllegalStateException if the handler is not set or the stream is closed
     * @throws RuntimeException if an exception occurred during header retrieval or batch fetching
     */
    @Override
    public T nextResponse() {
        ensureOpen();
        ensureHandlerSet();

        if (pendingException != null) {
            Throwable e = pendingException;
            pendingException = null;
            throw new TransportException("Failed to fetch batch", e);
        }

        long batchStartTime = System.nanoTime();
        VectorSchemaRoot rootToUse;
        if (pendingRoot != null) {
            rootToUse = pendingRoot;
            pendingRoot = null;
        } else {
            try {
                if (flightStream.next()) {
                    rootToUse = flightStream.getRoot();
                } else {
                    return null;  // No more data
                }
            } catch (FlightRuntimeException e) {
                if (statsCollector != null) {
                    statsCollector.incrementClientApplicationErrors();
                }
                throw e;
            } catch (Exception e) {
                if (statsCollector != null) {
                    statsCollector.incrementClientTransportErrors();
                }
                throw new TransportException("Failed to fetch next batch", e);
            }
        }

        try {
            T response = deserializeResponse(rootToUse);
            if (statsCollector != null) {
                statsCollector.incrementClientBatchesReceived();
                // Track full client batch time (fetch + deserialization)
                long batchTime = (System.nanoTime() - batchStartTime) / 1_000_000;
                statsCollector.addClientBatchTime(batchTime);
            }
            return response;
        } catch (Exception e) {
            if (statsCollector != null) {
                statsCollector.incrementClientTransportErrors();
            }
            throw new TransportException("Failed to deserialize response", e);
        } finally {
            rootToUse.close();
        }
    }

    /**
     * Retrieves the header for the current batch. Fetches the next batch if not already fetched,
     * but keeps the root open for reuse in nextResponse().
     *
     * @return the header for the current batch, or null if no more data is available
     */
    public Header currentHeader() {
        if (pendingRoot != null) {
            return headerContext.getHeader(reqId);
        }
        try {
            ensureOpen();
            if (flightStream.next()) {
                pendingRoot = flightStream.getRoot();
                return headerContext.getHeader(reqId);
            } else {
                return null;  // No more data
            }
        } catch (Exception e) {
            pendingException = e;
            logger.warn("Error fetching next batch", e);
            return headerContext.getHeader(reqId);
        }
    }

    /**
     * Cancels the flight stream due to client-side error or timeout
     * @param reason the reason for cancellation
     * @param cause the exception that caused cancellation (can be null)
     */
    @Override
    public void cancel(String reason, Throwable cause) {
        if (isClosed) {
            return;
        }

        try {
            // Cancel the flight stream - this notifies the server to stop producing
            flightStream.cancel(reason, cause);
            logger.debug("Cancelled flight stream: {}", reason);
        } catch (Exception e) {
            if (statsCollector != null) {
                statsCollector.incrementClientTransportErrors();
            }
            logger.warn("Error cancelling flight stream", e);
        } finally {
            close();
        }
    }

    /**
     * Closes the underlying flight stream and releases resources, including any pending root.
     */
    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        if (pendingRoot != null) {
            pendingRoot.close();
            pendingRoot = null;
        }
        try {
            flightStream.close();
        } catch (Exception e) {
            if (statsCollector != null) {
                statsCollector.incrementClientTransportErrors();
            }
            throw new TransportException("Failed to close flight stream", e);
        } finally {
            isClosed = true;
        }
    }

    /**
     * Deserializes the response from the given VectorSchemaRoot.
     *
     * @param root the root containing the response data
     * @return the deserialized response
     * @throws RuntimeException if deserialization fails
     */
    private T deserializeResponse(VectorSchemaRoot root) {
        try (VectorStreamInput input = new VectorStreamInput(root, namedWriteableRegistry)) {
            return handler.read(input);
        } catch (IOException e) {
            throw new TransportException("Failed to deserialize response", e);
        }
    }

    /**
     * Ensures the stream is not closed before performing operations.
     *
     * @throws TransportException if the stream is closed
     */
    private void ensureOpen() {
        if (isClosed) {
            throw new TransportException("Stream is closed");
        }
    }

    /**
     * Ensures the handler is set before attempting to read responses.
     *
     * @throws IllegalStateException if the handler is not set
     */
    private void ensureHandlerSet() {
        if (handler == null) {
            throw new TransportException("Handler must be set before requesting responses");
        }
    }
}
