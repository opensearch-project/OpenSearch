/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import org.apache.lucene.util.BytesRef;

import java.io.EOFException;
import java.io.IOException;

/**
 * {@link StreamInput} version of Lucene's {@link org.apache.lucene.store.ByteArrayDataInput}
 * This is used as a replacement of Lucene ByteArrayDataInput for abstracting byte order changes
 * in Lucene's API
 *
 * Attribution given to apache lucene project under ALv2:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @opensearch.internal
 */
public class BytesStreamInput extends StreamInput {
    private byte[] bytes;
    private int pos;
    private int limit;

    public BytesStreamInput(byte[] bytes) {
        reset(bytes);
    }

    public BytesStreamInput(byte[] bytes, int offset, int len) {
        reset(bytes, offset, len);
    }

    public BytesStreamInput() {
        reset(BytesRef.EMPTY_BYTES);
    }

    public void reset(byte[] bytes) {
        reset(bytes, 0, bytes.length);
    }

    public int getPosition() {
        return pos;
    }

    public void setPosition(int pos) {
        this.pos = pos;
    }

    public void reset(byte[] bytes, int offset, int len) {
        this.bytes = bytes;
        pos = offset;
        limit = offset + len;
    }

    public boolean eof() {
        return pos == limit;
    }

    public void skipBytes(long count) {
        pos += count;
    }

    @Override
    public byte readByte() throws EOFException {
        if (eof()) {
            throw new EOFException();
        }
        return bytes[pos++];
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws EOFException {
        if (available() < len) {
            throw new EOFException();
        }
        System.arraycopy(bytes, pos, b, offset, len);
        pos += len;
    }

    @Override
    public void close() {}

    @Override
    public int available() {
        return limit - pos;
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        int available = available();
        if (length > available) {
            throw new EOFException("attempting to read " + length + " bytes but only " + available + " bytes are available");
        }
    }

    @Override
    public int read() throws IOException {
        if (eof()) {
            throw new EOFException();
        }
        return bytes[pos++] & 0xFF;
    }

}
