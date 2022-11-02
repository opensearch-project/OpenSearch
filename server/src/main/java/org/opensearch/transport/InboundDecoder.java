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
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Decodes inbound data off the wire
 *
 * @opensearch.internal
 */
public class InboundDecoder implements Releasable {

    static final Object PING = new Object();
    static final Object END_CONTENT = new Object();

    private final Version version;
    private final PageCacheRecycler recycler;
    private TransportDecompressor decompressor;
    private int totalNetworkSize = -1;
    private int bytesConsumed = 0;
    private boolean isClosed = false;

    private static Version V_4_0_0 = Version.fromId(4000099 ^ Version.MASK);

    public InboundDecoder(Version version, PageCacheRecycler recycler) {
        this.version = version;
        this.recycler = recycler;
    }

    public int decode(ReleasableBytesReference reference, Consumer<Object> fragmentConsumer) throws IOException {
        ensureOpen();
        try {
            return internalDecode(reference, fragmentConsumer);
        } catch (Exception e) {
            cleanDecodeState();
            throw e;
        }
    }

    public int internalDecode(ReleasableBytesReference reference, Consumer<Object> fragmentConsumer) throws IOException {
        if (isOnHeader()) {
            int messageLength = TcpTransport.readMessageLength(reference);
            if (messageLength == -1) {
                return 0;
            } else if (messageLength == 0) {
                fragmentConsumer.accept(PING);
                return 6;
            } else {
                int headerBytesToRead = headerBytesToRead(reference);
                if (headerBytesToRead == 0) {
                    return 0;
                } else {
                    totalNetworkSize = messageLength + TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE;

                    Header header = readHeader(version, messageLength, reference);
                    bytesConsumed += headerBytesToRead;
                    if (header.isCompressed()) {
                        decompressor = new TransportDecompressor(recycler);
                    }
                    fragmentConsumer.accept(header);

                    if (isDone()) {
                        finishMessage(fragmentConsumer);
                    }
                    return headerBytesToRead;
                }
            }
        } else {
            // There are a minimum number of bytes required to start decompression
            if (decompressor != null && decompressor.canDecompress(reference.length()) == false) {
                return 0;
            }
            int bytesToConsume = Math.min(reference.length(), totalNetworkSize - bytesConsumed);
            bytesConsumed += bytesToConsume;
            ReleasableBytesReference retainedContent;
            if (isDone()) {
                retainedContent = reference.retainedSlice(0, bytesToConsume);
            } else {
                retainedContent = reference.retain();
            }
            if (decompressor != null) {
                decompress(retainedContent);
                ReleasableBytesReference decompressed;
                while ((decompressed = decompressor.pollDecompressedPage()) != null) {
                    fragmentConsumer.accept(decompressed);
                }
            } else {
                fragmentConsumer.accept(retainedContent);
            }
            if (isDone()) {
                finishMessage(fragmentConsumer);
            }

            return bytesToConsume;
        }
    }

    @Override
    public void close() {
        isClosed = true;
        cleanDecodeState();
    }

    private void finishMessage(Consumer<Object> fragmentConsumer) {
        cleanDecodeState();
        fragmentConsumer.accept(END_CONTENT);
    }

    private void cleanDecodeState() {
        IOUtils.closeWhileHandlingException(decompressor);
        decompressor = null;
        totalNetworkSize = -1;
        bytesConsumed = 0;
    }

    private void decompress(ReleasableBytesReference content) throws IOException {
        try (ReleasableBytesReference toRelease = content) {
            int consumed = decompressor.decompress(content);
            assert consumed == content.length();
        }
    }

    private boolean isDone() {
        return bytesConsumed == totalNetworkSize;
    }

    private int headerBytesToRead(BytesReference reference) {
        if (reference.length() < TcpHeader.BYTES_REQUIRED_FOR_VERSION) {
            return 0;
        }

        Version remoteVersion = Version.fromId(reference.getInt(TcpHeader.VERSION_POSITION));
        int fixedHeaderSize = TcpHeader.headerSize(remoteVersion);
        if (fixedHeaderSize > reference.length()) {
            return 0;
        } else {
            int variableHeaderSize = reference.getInt(TcpHeader.VARIABLE_HEADER_SIZE_POSITION);
            int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            if (totalHeaderSize > reference.length()) {
                return 0;
            } else {
                return totalHeaderSize;
            }
        }
    }

    // exposed for use in tests
    static Header readHeader(Version version, int networkMessageSize, BytesReference bytesReference) throws IOException {
        try (StreamInput streamInput = bytesReference.streamInput()) {
            streamInput.skip(TcpHeader.BYTES_REQUIRED_FOR_MESSAGE_SIZE);
            long requestId = streamInput.readLong();
            byte status = streamInput.readByte();
            Version remoteVersion = Version.fromId(streamInput.readInt());
            Header header = new Header(networkMessageSize, requestId, status, remoteVersion);
            final IllegalStateException invalidVersion = ensureVersionCompatibility(remoteVersion, version, header.isHandshake());
            if (invalidVersion != null) {
                throw invalidVersion;
            } else {
                // Skip since we already have ensured enough data available
                streamInput.readInt();
                header.finishParsingHeader(streamInput);
            }
            return header;
        }
    }

    private boolean isOnHeader() {
        return totalNetworkSize == -1;
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Decoder is already closed");
        }
    }

    static IllegalStateException ensureVersionCompatibility(Version remoteVersion, Version currentVersion, boolean isHandshake) {
        // for handshakes we are compatible with N-2 since otherwise we can't figure out our initial version
        // since we are compatible with N-1 and N+1 so we always send our minCompatVersion as the initial version in the
        // handshake. This looks odd but it's required to establish the connection correctly we check for real compatibility
        // once the connection is established
        final Version compatibilityVersion = isHandshake ? currentVersion.minimumCompatibilityVersion() : currentVersion;
        boolean v3x = currentVersion.onOrAfter(Version.V_3_0_0) && currentVersion.before(V_4_0_0);
        if ((v3x && remoteVersion.equals(Version.fromId(7099999)) == false) && remoteVersion.isCompatible(compatibilityVersion) == false) {
            final Version minCompatibilityVersion = isHandshake ? compatibilityVersion : compatibilityVersion.minimumCompatibilityVersion();
            String msg = "Received " + (isHandshake ? "handshake " : "") + "message from unsupported version: [";
            return new IllegalStateException(msg + remoteVersion + "] minimal compatible version is: [" + minCompatibilityVersion + "]");
        }
        return null;
    }
}
