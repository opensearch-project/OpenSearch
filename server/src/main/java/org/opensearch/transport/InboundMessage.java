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

import org.opensearch.common.annotation.DeprecatedApi;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.transport.nativeprotocol.NativeInboundMessage;

import java.io.IOException;

/**
 * Inbound data as a message
 * This api is deprecated, please use {@link org.opensearch.transport.nativeprotocol.NativeInboundMessage} instead.
 * @opensearch.api
 */
@DeprecatedApi(since = "2.14.0")
public class InboundMessage implements Releasable, ProtocolInboundMessage {

    private final NativeInboundMessage nativeInboundMessage;

    public InboundMessage(Header header, ReleasableBytesReference content, Releasable breakerRelease) {
        this.nativeInboundMessage = new NativeInboundMessage(header, content, breakerRelease);
    }

    public InboundMessage(Header header, Exception exception) {
        this.nativeInboundMessage = new NativeInboundMessage(header, exception);
    }

    public InboundMessage(Header header, boolean isPing) {
        this.nativeInboundMessage = new NativeInboundMessage(header, isPing);
    }

    public Header getHeader() {
        return this.nativeInboundMessage.getHeader();
    }

    public int getContentLength() {
        return this.nativeInboundMessage.getContentLength();
    }

    public Exception getException() {
        return this.nativeInboundMessage.getException();
    }

    public boolean isPing() {
        return this.nativeInboundMessage.isPing();
    }

    public boolean isShortCircuit() {
        return this.nativeInboundMessage.getException() != null;
    }

    public Releasable takeBreakerReleaseControl() {
        return this.nativeInboundMessage.takeBreakerReleaseControl();
    }

    public StreamInput openOrGetStreamInput() throws IOException {
        return this.nativeInboundMessage.openOrGetStreamInput();
    }

    @Override
    public void close() {
        this.nativeInboundMessage.close();
    }

    @Override
    public String toString() {
        return this.nativeInboundMessage.toString();
    }

    @Override
    public String getProtocol() {
        return this.nativeInboundMessage.getProtocol();
    }

}
