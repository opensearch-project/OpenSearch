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
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportStatus;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

/**
 * Client middleware for handling Arrow Flight headers. This middleware processes incoming headers
 * from Arrow Flight server responses, extracts transport headers, and stores them in the HeaderContext
 * for later retrieval.
 *
 * <p>It assumes that one request is sent at a time to {@link FlightClientChannel}.</p>
 *
 * @opensearch.internal
 */
class ClientHeaderMiddleware implements FlightClientMiddleware {
    // Header field names used in Arrow Flight communication
    static final String RAW_HEADER_KEY = "raw-header";
    static final String REQUEST_ID_KEY = "req-id";

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
     * @throws TransportException if headers are missing, invalid, or incompatible
     */
    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
        // Extract header fields
        String encodedHeader = incomingHeaders.get(RAW_HEADER_KEY);
        String reqId = incomingHeaders.get(REQUEST_ID_KEY);

        // Validate required headers
        if (encodedHeader == null) {
            throw new TransportException("Missing required header: " + RAW_HEADER_KEY);
        }
        if (reqId == null) {
            throw new TransportException("Missing required header: " + REQUEST_ID_KEY);
        }

        // Decode and process the header
        try {
            // Decode base64 header
            byte[] headerBuffer = Base64.getDecoder().decode(encodedHeader);
            BytesReference headerRef = new BytesArray(headerBuffer);

            // Parse the header
            Header header = InboundDecoder.readHeader(version, headerRef.length(), headerRef);

            // Validate version compatibility
            if (!Version.CURRENT.isCompatible(header.getVersion())) {
                throw new TransportException("Incompatible version: " + header.getVersion() + ", current: " + Version.CURRENT);
            }

            // Check for transport errors
            if (TransportStatus.isError(header.getStatus())) {
                throw new TransportException("Received error response with status: " + header.getStatus());
            }

            // Store the header in context for later retrieval
            long requestId = Long.parseLong(reqId);
            context.setHeader(requestId, header);
        } catch (IOException e) {
            throw new TransportException("Failed to decode header", e);
        } catch (NumberFormatException e) {
            throw new TransportException("Invalid request ID format: " + reqId, e);
        }
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        // No headers to add when sending requests
    }

    @Override
    public void onCallCompleted(CallStatus status) {
        // No cleanup needed when call completes
    }

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
