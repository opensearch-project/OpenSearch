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

package org.opensearch.core.common.io.stream;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.util.BitUtil;

import java.io.EOFException;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Similar to Lucene's BufferedChecksumIndexInput, however this wraps a
 * {@link StreamInput} so anything read will update the checksum
 *
 * @opensearch.internal
 */
public final class BufferedChecksumStreamInput extends FilterStreamInput {
    private static final int SKIP_BUFFER_SIZE = 1024;
    private byte[] skipBuffer;
    private final Checksum digest;
    private final String source;

    public BufferedChecksumStreamInput(StreamInput in, String source, BufferedChecksumStreamInput reuse) {
        super(in);
        this.source = source;
        if (reuse == null) {
            this.digest = new BufferedChecksum(new CRC32());
        } else {
            this.digest = reuse.digest;
            digest.reset();
            this.skipBuffer = reuse.skipBuffer;
        }
    }

    public BufferedChecksumStreamInput(StreamInput in, String source) {
        this(in, source, null);
    }

    public long getChecksum() {
        return this.digest.getValue();
    }

    @Override
    public byte readByte() throws IOException {
        final byte b = delegate.readByte();
        digest.update(b);
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        delegate.readBytes(b, offset, len);
        digest.update(b, offset, len);
    }

    private static final ThreadLocal<byte[]> buffer = ThreadLocal.withInitial(() -> new byte[8]);

    @Override
    public short readShort() throws IOException {
        final byte[] buf = buffer.get();
        readBytes(buf, 0, 2);
        return (short) BitUtil.VH_BE_SHORT.get(buf, 0);
    }

    @Override
    public int readInt() throws IOException {
        final byte[] buf = buffer.get();
        readBytes(buf, 0, 4);
        return (int) BitUtil.VH_BE_INT.get(buf, 0);
    }

    @Override
    public long readLong() throws IOException {
        final byte[] buf = buffer.get();
        readBytes(buf, 0, 8);
        return (long) BitUtil.VH_BE_LONG.get(buf, 0);
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
        digest.reset();
    }

    @Override
    public int read() throws IOException {
        try {
            return readByte() & 0xFF;
        } catch (EOFException e) {
            return -1;
        }
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }

    @Override
    public long skip(long numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
        }
        if (skipBuffer == null) {
            skipBuffer = new byte[SKIP_BUFFER_SIZE];
        }
        assert skipBuffer.length == SKIP_BUFFER_SIZE;
        long skipped = 0;
        for (; skipped < numBytes;) {
            final int step = (int) Math.min(SKIP_BUFFER_SIZE, numBytes - skipped);
            readBytes(skipBuffer, 0, step);
            skipped += step;
        }
        return skipped;
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    public void resetDigest() {
        digest.reset();
    }

    public String getSource() {
        return source;
    }
}
