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

/**
 * Client middleware for handling Arrow Flight headers. It assumes that one request is sent at a time to {@link FlightClientChannel}
 */
public class ClientHeaderMiddleware implements FlightClientMiddleware {
    private final HeaderContext context;
    private final Version version;

    ClientHeaderMiddleware(HeaderContext context, Version version) {
        this.context = context;
        this.version = version;
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
        String encodedHeader = incomingHeaders.get("raw-header");
        String reqId = incomingHeaders.get("req-id");
        if (encodedHeader == null || reqId == null) {
            throw new TransportException("Missing header");
        }
        byte[] headerBuffer =  Base64.getDecoder().decode(encodedHeader);
        BytesReference headerRef = new BytesArray(headerBuffer);
        Header header;
        try {
            header = InboundDecoder.readHeader(version, headerRef.length(), headerRef);
        } catch (IOException e) {
            throw new TransportException(e);
        }
        if (!Version.CURRENT.isCompatible(header.getVersion())) {
            throw new TransportException("Incompatible version: " + header.getVersion());
        }
        if (TransportStatus.isError(header.getStatus())) {
            throw new TransportException("Received error response");
        }
        context.setHeader(Long.parseLong(reqId), header);
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {}

    @Override
    public void onCallCompleted(CallStatus status) {}

    public static class Factory implements FlightClientMiddleware.Factory {
        private final Version version;
        private final HeaderContext context;

        Factory(HeaderContext context, Version version) {
            this.version = version;
            this.context = context;
        }

        @Override
        public ClientHeaderMiddleware onCallStarted(CallInfo callInfo) {
            return new ClientHeaderMiddleware(context, version);
        }
    }
}
