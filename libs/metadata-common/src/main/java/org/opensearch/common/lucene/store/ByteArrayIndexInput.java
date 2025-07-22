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

package org.opensearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;

import java.io.EOFException;
import java.io.IOException;

/**
 * Wraps array of bytes into IndexInput
 *
 * @opensearch.internal
 */
public class ByteArrayIndexInput extends IndexInput implements RandomAccessInput {
    private final byte[] bytes;

    private final int offset;

    private final int length;

    private int pos;

    public ByteArrayIndexInput(String resourceDesc, byte[] bytes) {
        this(resourceDesc, bytes, 0, bytes.length);
    }

    public ByteArrayIndexInput(String resourceDesc, byte[] bytes, int offset, int length) {
        super(resourceDesc);
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public void seek(long l) throws IOException {
        if (l < 0) {
            throw new IllegalArgumentException("Seeking to negative position: " + pos);
        } else if (l > length) {
            throw new EOFException("seek past EOF");
        }
        pos = (int) l;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset >= 0L && length >= 0L && offset + length <= this.length) {
            return new ByteArrayIndexInput(sliceDescription, bytes, this.offset + (int) offset, (int) length);
        } else {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length
                    + ": "
                    + this
            );
        }
    }

    @Override
    public byte readByte() throws IOException {
        validatePos(pos, Byte.BYTES);
        return bytes[offset + pos++];
    }

    @Override
    public void readBytes(final byte[] b, final int offset, int len) throws IOException {
        validatePos(pos, len);
        System.arraycopy(bytes, this.offset + pos, b, offset, len);
        pos += len;
    }

    @Override
    public byte readByte(long pos) throws IOException {
        validatePos(pos, Byte.BYTES);
        return bytes[offset + (int) pos];
    }

    @Override
    public short readShort(long pos) throws IOException {
        validatePos(pos, Short.BYTES);
        return (short) BitUtil.VH_LE_SHORT.get(bytes, offset + (int) pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        validatePos(pos, Integer.BYTES);
        return (int) BitUtil.VH_LE_INT.get(bytes, offset + (int) pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        validatePos(pos, Long.BYTES);
        return (long) BitUtil.VH_LE_LONG.get(bytes, offset + (int) pos);
    }

    private void validatePos(long pos, int len) throws EOFException {
        if (pos < 0 || pos + len > length) {
            throw new EOFException("seek past EOF");
        }
    }
}
