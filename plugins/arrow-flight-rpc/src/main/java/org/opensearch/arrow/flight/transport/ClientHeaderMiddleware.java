/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.opensearch.Version;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.Header;
import org.opensearch.transport.InboundDecoder;
import org.opensearch.transport.TransportStatus;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

/**
 * Client middleware for handling Arrow Flight headers. This middleware processes incoming headers
 * from Arrow Flight server responses, extracts transport headers, and stores them in the HeaderContext
 * for later retrieval.
 *
 * @opensearch.internal
 */
class ClientHeaderMiddleware implements FlightClientMiddleware {
    static final String RAW_HEADER_KEY = "raw-header";
    static final String CORRELATION_ID_KEY = "correlation-id";

    private final HeaderContext context;
    private final Version version;

    /**
     * Creates a new ClientHeaderMiddleware instance.
     *
     * @param context The header context for storing extracted headers
     * @param version The OpenSearch version for compatibility checking
     */
    ClientHeaderMiddleware(HeaderContext context, Version version) {
        this.context = Objects.requireNonNull(context, "HeaderContext must not be null");
        this.version = Objects.requireNonNull(version, "Version must not be null");
    }

    /**
     * Processes incoming headers from the Arrow Flight server response.
     * Extracts, decodes, and validates the transport header, then stores it in the context.
     *
     * @param incomingHeaders The headers received from the Arrow Flight server
     * @throws StreamException if headers are missing, invalid, or incompatible
     */
    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
        String encodedHeader = incomingHeaders.get(RAW_HEADER_KEY);
        String correlationId = incomingHeaders.get(CORRELATION_ID_KEY);

        if (encodedHeader == null) {
            throw new StreamException(StreamErrorCode.INVALID_ARGUMENT, "Missing required header: " + RAW_HEADER_KEY);
        }
        if (correlationId == null) {
            throw new StreamException(StreamErrorCode.INVALID_ARGUMENT, "Missing required header: " + CORRELATION_ID_KEY);
        }

        try {
            byte[] headerBuffer = Base64.getDecoder().decode(encodedHeader);
            BytesReference headerRef = new BytesArray(headerBuffer);

            Header header = InboundDecoder.readHeader(version, headerRef.length(), headerRef);

            if (!Version.CURRENT.isCompatible(header.getVersion())) {
                throw new StreamException(
                    StreamErrorCode.UNAVAILABLE,
                    "Incompatible version: " + header.getVersion() + ", current: " + Version.CURRENT
                );
            }

            if (TransportStatus.isError(header.getStatus())) {
                throw new StreamException(StreamErrorCode.INTERNAL, "Received error response with status: " + header.getStatus());
            }

            // Store the header in context for later retrieval
            context.setHeader(Long.parseLong(correlationId), header);
        } catch (IOException e) {
            throw new StreamException(StreamErrorCode.INTERNAL, "Failed to decode header", e);
        } catch (NumberFormatException e) {
            throw new StreamException(StreamErrorCode.INVALID_ARGUMENT, "Invalid request ID format: " + correlationId, e);
        }
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

    @Override
    public void onCallCompleted(CallStatus status) {}

    /**
     * Factory for creating ClientHeaderMiddleware instances.
     */
    public static class Factory implements FlightClientMiddleware.Factory {
        private final Version version;
        private final HeaderContext context;

        /**
         * Creates a new Factory instance.
         *
         * @param context The header context for storing extracted headers
         * @param version The OpenSearch version for compatibility checking
         */
        Factory(HeaderContext context, Version version) {
            this.context = Objects.requireNonNull(context, "HeaderContext must not be null");
            this.version = Objects.requireNonNull(version, "Version must not be null");
        }

        @Override
        public ClientHeaderMiddleware onCallStarted(CallInfo callInfo) {
            return new ClientHeaderMiddleware(context, version);
        }
    }
}
