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
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

import java.nio.ByteBuffer;
import java.util.Base64;

import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.RAW_HEADER_KEY;
import static org.opensearch.arrow.flight.transport.ClientHeaderMiddleware.REQUEST_ID_KEY;

/**
 * ServerHeaderMiddleware is created per call to handle the response header
 * and add it to the outgoing headers. It also adds the request ID to the
 * outgoing headers, retrieved from the incoming headers.
 */
class ServerHeaderMiddleware implements FlightServerMiddleware {
    private ByteBuffer headerBuffer;
    private final String reqId;

    ServerHeaderMiddleware(String reqId) {
        this.reqId = reqId;
    }

    void setHeader(ByteBuffer headerBuffer) {
        this.headerBuffer = headerBuffer;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        if (headerBuffer != null) {
            byte[] headerBytes = new byte[headerBuffer.remaining()];
            headerBuffer.get(headerBytes);
            String encodedHeader = Base64.getEncoder().encodeToString(headerBytes);
            outgoingHeaders.insert(RAW_HEADER_KEY, encodedHeader);
            outgoingHeaders.insert(REQUEST_ID_KEY, reqId);
            headerBuffer.rewind();
        } else {
            outgoingHeaders.insert(RAW_HEADER_KEY, "");
            outgoingHeaders.insert(REQUEST_ID_KEY, reqId);
        }
    }

    @Override
    public void onCallCompleted(CallStatus status) {}

    @Override
    public void onCallErrored(Throwable err) {}

    public static class Factory implements FlightServerMiddleware.Factory<ServerHeaderMiddleware> {
        @Override
        public ServerHeaderMiddleware onCallStarted(CallInfo callInfo, CallHeaders incomingHeaders, RequestContext context) {
            String reqId = incomingHeaders.get(REQUEST_ID_KEY);
            return new ServerHeaderMiddleware(reqId);
        }
    }
}
