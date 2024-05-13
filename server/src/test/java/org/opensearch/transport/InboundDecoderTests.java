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

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.transport.TransportMessage;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.hasItems;

public abstract class InboundDecoderTests extends OpenSearchTestCase {

    protected ThreadContext threadContext;

    protected abstract BytesReference serialize(
        boolean isRequest,
        Version version,
        boolean handshake,
        boolean compress,
        String action,
        long requestId,
        Writeable transportMessage
    ) throws IOException;

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
        TransportMessage transportMessage;
        if (isRequest) {
            threadContext.putHeader(headerKey, headerValue);
            transportMessage = new TestRequest(randomAlphaOfLength(100));
        } else {
            threadContext.addResponseHeader(headerKey, headerValue);
            transportMessage = new TestResponse(randomAlphaOfLength(100));
        }

        final BytesReference totalBytes = serialize(isRequest, Version.CURRENT, false, false, action, requestId, transportMessage);
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

    public void testDecodePreHeaderSizeVariableInt() throws IOException {
        // TODO: Can delete test on 9.0
        boolean isCompressed = randomBoolean();
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final Version preHeaderVariableInt = LegacyESVersion.V_7_5_0;
        final String contentValue = randomAlphaOfLength(100);

        final BytesReference totalBytes = serialize(
            true,
            preHeaderVariableInt,
            true,
            isCompressed,
            action,
            requestId,
            new TestRequest(contentValue)
        );
        int partialHeaderSize = TcpHeader.headerSize(preHeaderVariableInt);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(totalBytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(partialHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(preHeaderVariableInt, header.getVersion());
        assertEquals(isCompressed, header.isCompressed());
        assertTrue(header.isHandshake());
        assertTrue(header.isRequest());
        assertTrue(header.needsToReadVariableHeader());
        fragments.clear();

        final BytesReference bytes2 = totalBytes.slice(bytesConsumed, totalBytes.length() - bytesConsumed);
        final ReleasableBytesReference releasable2 = ReleasableBytesReference.wrap(bytes2);
        int bytesConsumed2 = decoder.decode(releasable2, fragments::add);
        assertEquals(2, fragments.size());
        assertEquals(InboundDecoder.END_CONTENT, fragments.get(fragments.size() - 1));
        assertEquals(totalBytes.length() - bytesConsumed, bytesConsumed2);
    }

    public void testDecodeHandshakeCompatibility() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        final String headerKey = randomAlphaOfLength(10);
        final String headerValue = randomAlphaOfLength(20);
        threadContext.putHeader(headerKey, headerValue);
        Version handshakeCompat = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();

        final BytesReference bytes = serialize(
            true,
            handshakeCompat,
            true,
            false,
            action,
            requestId,
            new TestRequest(randomAlphaOfLength(100))
        );
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompat);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(handshakeCompat, header.getVersion());
        assertFalse(header.isCompressed());
        assertTrue(header.isHandshake());
        assertTrue(header.isRequest());
        // TODO: On 9.0 this will be true because all compatible versions with contain the variable header int
        assertTrue(header.needsToReadVariableHeader());
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
        TransportMessage transportMessage;
        if (isRequest) {
            transportMessage = new TestRequest(randomAlphaOfLength(100));
        } else {
            transportMessage = new TestResponse(randomAlphaOfLength(100));
        }

        final BytesReference totalBytes = serialize(isRequest, Version.CURRENT, false, true, action, requestId, transportMessage);
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
        Version handshakeCompat = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();

        final BytesReference bytes = serialize(
            true,
            handshakeCompat,
            true,
            true,
            action,
            requestId,
            new TestRequest(randomAlphaOfLength(100))
        );
        int totalHeaderSize = TcpHeader.headerSize(handshakeCompat);

        InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final ArrayList<Object> fragments = new ArrayList<>();
        final ReleasableBytesReference releasable1 = ReleasableBytesReference.wrap(bytes);
        int bytesConsumed = decoder.decode(releasable1, fragments::add);
        assertEquals(totalHeaderSize, bytesConsumed);
        assertEquals(1, releasable1.refCount());

        final Header header = (Header) fragments.get(0);
        assertEquals(requestId, header.getRequestId());
        assertEquals(handshakeCompat, header.getVersion());
        assertTrue(header.isCompressed());
        assertTrue(header.isHandshake());
        assertTrue(header.isRequest());
        // TODO: On 9.0 this will be true because all compatible versions with contain the variable header int
        assertTrue(header.needsToReadVariableHeader());
        fragments.clear();
    }

    public void testVersionIncompatibilityDecodeException() throws IOException {
        String action = "test-request";
        long requestId = randomNonNegativeLong();
        Version incompatibleVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();

        final BytesReference bytes = serialize(
            true,
            incompatibleVersion,
            false,
            true,
            action,
            requestId,
            new TestRequest(randomAlphaOfLength(100))
        );

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

        final Version version = Version.fromString("7.0.0");
        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("6.0.0"), version, true);
        assertNull(ise);

        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("6.0.0"), version, false);
        assertEquals(
            "Received message from unsupported version: [6.0.0] minimal compatible version is: ["
                + version.minimumCompatibilityVersion()
                + "]",
            ise.getMessage()
        );

        // For handshake we are compatible with N-2
        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("6.8.0"), version, true);
        assertNull(ise);

        ise = InboundDecoder.ensureVersionCompatibility(Version.fromString("5.6.0"), version, false);
        assertEquals(
            "Received message from unsupported version: [5.6.0] minimal compatible version is: ["
                + version.minimumCompatibilityVersion()
                + "]",
            ise.getMessage()
        );
    }
}
