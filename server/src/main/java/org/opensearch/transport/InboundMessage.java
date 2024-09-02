/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Inbound data as a message
 */
@PublicApi(since = "1.0.0")
public class InboundMessage implements Releasable, ProtocolInboundMessage {

    static final InboundMessage PING = new InboundMessage(null, null, null, true, null);

    protected final Header header;
    protected final ReleasableBytesReference content;
    protected final Exception exception;
    protected final boolean isPing;
    private Releasable breakerRelease;
    private StreamInput streamInput;

    public InboundMessage(Header header, ReleasableBytesReference content, Releasable breakerRelease) {
        this(header, content, null, false, breakerRelease);
    }

    public InboundMessage(Header header, Exception exception) {
        this(header, null, exception, false, null);
    }

    public InboundMessage(Header header, boolean isPing) {
        this(header, null, null, isPing, null);
    }

    private InboundMessage(
        Header header,
        ReleasableBytesReference content,
        Exception exception,
        boolean isPing,
        Releasable breakerRelease
    ) {
        this.header = header;
        this.content = content;
        this.exception = exception;
        this.isPing = isPing;
        this.breakerRelease = breakerRelease;
    }

    TransportProtocol getTransportProtocol() {
        if (isPing) {
            return TransportProtocol.NATIVE;
        }
        return header.getTransportProtocol();
    }

    public String getProtocol() {
        return header.getTransportProtocol().toString();
    }

    public Header getHeader() {
        return header;
    }

    public int getContentLength() {
        if (content == null) {
            return 0;
        } else {
            return content.length();
        }
    }

    public Exception getException() {
        return exception;
    }

    public boolean isPing() {
        return isPing;
    }

    public boolean isShortCircuit() {
        return exception != null;
    }

    public Releasable takeBreakerReleaseControl() {
        final Releasable toReturn = breakerRelease;
        breakerRelease = null;
        if (toReturn != null) {
            return toReturn;
        } else {
            return () -> {};
        }
    }

    public StreamInput openOrGetStreamInput() throws IOException {
        assert isPing == false && content != null;
        if (streamInput == null) {
            streamInput = content.streamInput();
            streamInput.setVersion(header.getVersion());
        }
        return streamInput;
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(streamInput);
        Releasables.closeWhileHandlingException(content, breakerRelease);
    }

    @Override
    public String toString() {
        return "InboundMessage{" + header + "}";
    }
}
