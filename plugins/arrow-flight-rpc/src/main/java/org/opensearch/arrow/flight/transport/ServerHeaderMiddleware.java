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

public class ServerHeaderMiddleware implements FlightServerMiddleware {
    private ByteBuffer headerBuffer;

    public void setHeader(ByteBuffer headerBuffer) {
        this.headerBuffer = headerBuffer;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
        if (headerBuffer != null) {
            byte[] headerBytes = new byte[headerBuffer.remaining()];
            headerBuffer.get(headerBytes);
            String encodedHeader = Base64.getEncoder().encodeToString(headerBytes);
            outgoingHeaders.insert("raw-header", encodedHeader);
            headerBuffer.rewind();
        }
    }

    @Override
    public void onCallCompleted(CallStatus status) {}

    @Override
    public void onCallErrored(Throwable err) {}

    public static class Factory implements FlightServerMiddleware.Factory<ServerHeaderMiddleware> {
        @Override
        public ServerHeaderMiddleware onCallStarted(CallInfo callInfo, CallHeaders incomingHeaders, RequestContext context) {
            return new ServerHeaderMiddleware();
        }
    }
}
