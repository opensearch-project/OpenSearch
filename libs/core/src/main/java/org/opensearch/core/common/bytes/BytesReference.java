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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.core.common.bytes;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.BytesStream;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.util.ByteArray;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * A reference to bytes.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface BytesReference extends Comparable<BytesReference>, ToXContentFragment {

    /**
     * Convert an {@link XContentBuilder} into a BytesReference. This method closes the builder,
     * so no further fields may be added.
     */
    static BytesReference bytes(XContentBuilder xContentBuilder) {
        xContentBuilder.close();
        OutputStream stream = xContentBuilder.getOutputStream();
        if (stream instanceof ByteArrayOutputStream) {
            return new BytesArray(((ByteArrayOutputStream) stream).toByteArray());
        } else {
            return ((BytesStream) stream).bytes();
        }
    }

    /**
     * Returns a compact array from the given BytesReference. The returned array won't be copied unless necessary. If you need
     * to modify the returned array use {@code BytesRef.deepCopyOf(reference.toBytesRef()} instead
     */
    static byte[] toBytes(BytesReference reference) {
        final BytesRef bytesRef = reference.toBytesRef();
        if (bytesRef.offset == 0 && bytesRef.length == bytesRef.bytes.length) {
            return bytesRef.bytes;
        }
        return ArrayUtil.copyOfSubArray(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
    }

    static byte[] toBytesWithoutCompact(BytesReference reference) {
        final BytesRef bytesRef = reference.toBytesRef();
        return bytesRef.bytes;
    }

    /**
     * Returns an array of byte buffers from the given BytesReference.
     */
    static ByteBuffer[] toByteBuffers(BytesReference reference) {
        BytesRefIterator byteRefIterator = reference.iterator();
        BytesRef r;
        try {
            ArrayList<ByteBuffer> buffers = new ArrayList<>();
            while ((r = byteRefIterator.next()) != null) {
                buffers.add(ByteBuffer.wrap(r.bytes, r.offset, r.length));
            }
            return buffers.toArray(new ByteBuffer[0]);

        } catch (IOException e) {
            // this is really an error since we don't do IO in our bytesreferences
            throw new AssertionError("won't happen", e);
        }
    }

    /**
     * Returns BytesReference composed of the provided ByteBuffers.
     */
    static BytesReference fromByteBuffers(ByteBuffer[] buffers) {
        int bufferCount = buffers.length;
        if (bufferCount == 0) {
            return BytesArray.EMPTY;
        } else if (bufferCount == 1) {
            return fromByteBuffer(buffers[0]);
        } else {
            BytesReference[] references = new BytesReference[bufferCount];
            for (int i = 0; i < bufferCount; ++i) {
                references[i] = fromByteBuffer(buffers[i]);
            }

            return CompositeBytesReference.of(references);
        }
    }

    /**
     * Returns BytesReference composed of the provided ByteBuffer.
     */
    static BytesReference fromByteBuffer(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            final byte[] array = new byte[buffer.remaining()];
            buffer.asReadOnlyBuffer().get(array, 0, buffer.remaining());
            return new BytesArray(array);
        }
    }

    /**
     * Returns BytesReference either wrapping the provided {@link ByteArray} or in case the has a backing raw byte array one that wraps
     * that backing array directly.
     */
    static BytesReference fromByteArray(ByteArray byteArray, int length) {
        if (length == 0) {
            return BytesArray.EMPTY;
        }
        if (byteArray.hasArray()) {
            return new BytesArray(byteArray.array(), 0, length);
        }
        return new PagedBytesReference(byteArray, 0, length);
    }

    /**
     * Returns the byte at the specified index. Need to be between 0 and length.
     */
    byte get(int index);

    /**
     * Returns the integer read from the 4 bytes (big endian) starting at the given index.
     */
    default int getInt(int index) {
        return ((get(index) & 0xFF) << 24) | ((get(index + 1) & 0xFF) << 16) | ((get(index + 2) & 0xFF) << 8) | (get(index + 3) & 0xFF);
    }

    /**
     * Finds the index of the first occurrence of the given marker between within the given bounds.
     * @param marker marker byte to search
     * @param from lower bound for the index to check (inclusive)
     * @return first index of the marker or {@code -1} if not found
     */
    int indexOf(byte marker, int from);

    /**
     * The length.
     */
    int length();

    /**
     * Slice the bytes from the {@code from} index up to {@code length}.
     */
    BytesReference slice(int from, int length);

    /**
     * The amount of memory used by this BytesReference
     */
    long ramBytesUsed();

    /**
     * A stream input of the bytes.
     */
    StreamInput streamInput() throws IOException;

    /**
     * Writes the bytes directly to the output stream.
     */
    void writeTo(OutputStream os) throws IOException;

    /**
     * Interprets the referenced bytes as UTF8 bytes, returning the resulting string
     */
    String utf8ToString();

    /**
     * Converts to Lucene BytesRef.
     */
    BytesRef toBytesRef();

    /**
     * Returns a BytesRefIterator for this BytesReference. This method allows
     * access to the internal pages of this reference without copying them. Use with care!
     * @see BytesRefIterator
     */
    BytesRefIterator iterator();
}
