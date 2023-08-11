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

import org.opensearch.Version;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.transport.TransportMessage;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.hamcrest.Matchers.hasItems;

public class InboundDecoderTests extends OpenSearchTestCase {

    private ThreadContext threadContext;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testDecode() throws IOException {
        boolean isRequest = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        if (isRequest) {
            threadContext.putHeader(headerKey, headerValue);
        } else {
            threadContext.addResponseHeader(headerKey, headerValue);
        }
        OutboundMessage message;
        if (isRequest) {
            message = new OutboundMessage.Request(
                threadContext,
                new String[0],
                new TestRequest(randomAlphaOfLength(100)),
                Version.CURRENT,
                action,
                requestId,
                false,
                false
            );
        } else {
            message = new OutboundMessage.Response(
                threadContext,
                Collections.emptySet(),
                new TestResponse(randomAlphaOfLength(100)),
                Version.CURRENT,
                requestId,
                false,
                false
            );
        }

        final BytesReference totalBytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(Version.CURRENT) + totalBytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
        final BytesReference messageBytes = totalBytes.slice(totalHeaderSize, totalBytes.length() - totalHeaderSize);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(Version.CURRENT, header.getVersion());
        assertFalse(header.isCompressed());
        assertFalse(header.isHandshake());
        if (isRequest) {
            assertEquals(action, header.getActionName());
            assertTrue(header.isRequest());
            assertEquals(header.getHeaders().v1().get(headerKey), headerValue);
        } else {
            assertTrue(header.isResponse());
            assertThat(header.getHeaders().v2().get(headerKey), hasItems(headerValue));
        }
        assertFalse(header.needsToReadVariableHeader());
        fragments.clear();

        final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
        final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
        int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
        assertEquals(totalBytes.length() - totalHeaderSize, bytesConsumed2);

        final Object content = fragments.get(0);
        final Object endMarker = fragments.get(1);

        assertEquals(messageBytes, content);
        // Ref count is incremented since the bytes are forwarded as a fragment
        assertEquals(2, releasable2.refCount());
        assertEquals(InboundDecoder.END_CONTENT, endMarker);
    }

    public void testDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        Version handshakeCompatVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new String[0],
            new TestRequest(randomAlphaOfLength(100)),
            handshakeCompatVersion,
            action,
            requestId,
            true,
            false
        );

        final BytesReference bytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompatVersion) + bytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(handshakeCompatVersion, header.getVersion());
        assertFalse(header.isCompressed());
        assertTrue(header.isHandshake());
        assertTrue(header.isRequest());
        fragments.clear();
    }

    public void testCompressedDecode() throws IOException {
        boolean isRequest = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        if (isRequest) {
            threadContext.putHeader(headerKey, headerValue);
        } else {
            threadContext.addResponseHeader(headerKey, headerValue);
        }
        OutboundMessage message;
        TransportMessage transportMessage;
        if (isRequest) {
            transportMessage = new TestRequest(randomAlphaOfLength(100));
            message = new OutboundMessage.Request(
                threadContext,
                new String[0],
                transportMessage,
                Version.CURRENT,
                action,
                requestId,
                false,
                true
            );
        } else {
            transportMessage = new TestResponse(randomAlphaOfLength(100));
            message = new OutboundMessage.Response(
                threadContext,
                Collections.emptySet(),
                transportMessage,
                Version.CURRENT,
                requestId,
                false,
                true
            );
        }

        final BytesReference totalBytes = message.serialize(new BytesStreamOutput());
        final BytesStreamOutput out = new BytesStreamOutput();
        transportMessage.writeTo(out);
        final BytesReference uncompressedBytes = out.bytes();
        int totalHeaderSize = TcpHeader.headerSize(Version.CURRENT) + totalBytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(Version.CURRENT, header.getVersion());
        assertTrue(header.isCompressed());
        assertFalse(header.isHandshake());
        if (isRequest) {
            assertEquals(action, header.getActionName());
            assertTrue(header.isRequest());
            assertEquals(header.getHeaders().v1().get(headerKey), headerValue);
        } else {
            assertTrue(header.isResponse());
            assertThat(header.getHeaders().v2().get(headerKey), hasItems(headerValue));
        }
        assertFalse(header.needsToReadVariableHeader());
        fragments.clear();

        final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
        final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
        int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
        assertEquals(totalBytes.length() - totalHeaderSize, bytesConsumed2);

        final Object content = fragments.get(0);
        final Object endMarker = fragments.get(1);

        assertEquals(uncompressedBytes, content);
        // Ref count is not incremented since the bytes are immediately consumed on decompression
        assertEquals(1, releasable2.refCount());
        assertEquals(InboundDecoder.END_CONTENT, endMarker);
    }

    public void testCompressedDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        Version handshakeCompatVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new String[0],
            new TestRequest(randomAlphaOfLength(100)),
            handshakeCompatVersion,
            action,
            requestId,
            true,
            true
        );

        final BytesReference bytes = message.serialize(new BytesStreamOutput());
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompatVersion) + bytes.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(handshakeCompatVersion, header.getVersion());
        assertTrue(header.isCompressed());
        assertTrue(header.isHandshake());
        assertTrue(header.isRequest());
        fragments.clear();
    }

    public void testVersionIncompatibilityDecodeException() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        Version incompatibleVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
        OutboundMessage message = new OutboundMessage.Request(
            threadContext,
            new String[0],
            new TestRequest(randomAlphaOfLength(100)),
            incompatibleVersion,
            action,
            requestId,
            false,
            true
        );

        final BytesReference bytes = message.serialize(new BytesStreamOutput());

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        expectThrows(IllegalStateException.class, () -> decoder.decode(releasable1, fragments::add));
        // No bytes are retained
        assertEquals(1, releasable1.refCount());
    }

    public void testEnsureVersionCompatibility() throws IOException {
        IllegalStateException ise = InboundDecoder.ensureVersionCompatibility(
            VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT),
            Version.CURRENT,
            randomBoolean()
        );
        assertNull(ise);

        final Version version = Version.V_3_0_0;
        ise = InboundDecoder.ensureVersionCompatibility(Version.V_2_0_0, version, true);
        assertNull(ise);

        ise = InboundDecoder.ensureVersionCompatibility(VersionUtils.V_1_0_0, version, false);
        assertEquals(
            "Received message from unsupported version: [1.0.0] minimal compatible version is: ["
                + version.minimumCompatibilityVersion()
                + "]",
            ise.getMessage()
        );

        // For handshake we are compatible with N-2
        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("2.1.0"), version, true);
        assertNull(ise);

        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("1.3.0"), version, false);
        assertEquals(
            "Received message from unsupported version: [1.3.0] minimal compatible version is: ["
                + version.minimumCompatibilityVersion()
                + "]",
            ise.getMessage()
        );
    }
}
