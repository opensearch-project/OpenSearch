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

package org.opensearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStream;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.util.ByteArray;

import java.io.IOException;

/**
 * A @link {@link StreamOutput} that uses {@link BigArrays} to acquire pages of
 * bytes, which avoids frequent reallocation &amp; copying of the internal data.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class BytesStreamOutput extends BytesStream {

    protected final BigArrays bigArrays;

    @Nullable
    protected ByteArray bytes;
    protected int count;

    /**
     * Create a non recycling {@link BytesStreamOutput} with an initial capacity of 0.
     */
    public BytesStreamOutput() {
        // since this impl is not recycling anyway, don't bother aligning to
        // the page size, this will even save memory
        this(0);
    }

    /**
     * Create a non recycling {@link BytesStreamOutput} with enough initial pages acquired
     * to satisfy the capacity given by expected size.
     *
     * @param expectedSize the expected maximum size of the stream in bytes.
     */
    public BytesStreamOutput(int expectedSize) {
        this(expectedSize, BigArrays.NON_RECYCLING_INSTANCE);
    }

    protected BytesStreamOutput(int expectedSize, BigArrays bigArrays) {
        this.bigArrays = bigArrays;
        if (expectedSize != 0) {
            this.bytes = bigArrays.newByteArray(expectedSize, false);
        }
    }

    @Override
    public long position() {
        return count;
    }

    @Override
    public void writeByte(byte b) {
        ensureCapacity(count + 1L);
        bytes.set(count, b);
        count++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        // nothing to copy
        if (length == 0) {
            return;
        }

        // illegal args: offset and/or length exceed array size
        if (b.length < (offset + length)) {
            throw new IllegalArgumentException("Illegal offset " + offset + "/length " + length + " for byte[] of length " + b.length);
        }

        // get enough pages for new size
        ensureCapacity(((long) count) + length);

        // bulk copy
        bytes.set(count, b, offset, length);

        // advance
        count += length;
    }

    @Override
    public void reset() {
        // shrink list of pages
        if (bytes != null && bytes.size() > PageCacheRecycler.PAGE_SIZE_IN_BYTES) {
            bytes = bigArrays.resize(bytes, PageCacheRecycler.PAGE_SIZE_IN_BYTES);
        }

        // go back to start
        count = 0;
    }

    @Override
    public void flush() {
        // nothing to do
    }

    @Override
    public void seek(long position) {
        ensureCapacity(position);
        count = (int) position;
    }

    public void skip(int length) {
        seek(((long) count) + length);
    }

    @Override
    public void close() {
        // empty for now.
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number of valid
     *         bytes in this output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public int size() {
        return count;
    }

    @Override
    public BytesReference bytes() {
        if (bytes == null) {
            return BytesArray.EMPTY;
        }
        return BytesReference.fromByteArray(bytes, count);
    }

    /**
     * Like {@link #bytes()} but copies the bytes to a freshly allocated buffer.
     *
     * @return copy of the bytes in this instances
     */
    public BytesReference copyBytes() {
        final byte[] keyBytes = new byte[count];
        int offset = 0;
        final BytesRefIterator iterator = bytes().iterator();
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                System.arraycopy(slice.bytes, slice.offset, keyBytes, offset, slice.length);
                offset += slice.length;
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return new BytesArray(keyBytes);
    }

    /**
     * Returns the number of bytes used by the underlying {@link ByteArray}
     * @see ByteArray#ramBytesUsed()
     */
    public long ramBytesUsed() {
        return bytes.ramBytesUsed();
    }

    void ensureCapacity(long offset) {
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(getClass().getSimpleName() + " cannot hold more than 2GB of data");
        }
        if (bytes == null) {
            this.bytes = bigArrays.newByteArray(BigArrays.overSize(offset, PageCacheRecycler.PAGE_SIZE_IN_BYTES, 1), false);
        } else {
            bytes = bigArrays.grow(bytes, offset);
        }
    }

}
