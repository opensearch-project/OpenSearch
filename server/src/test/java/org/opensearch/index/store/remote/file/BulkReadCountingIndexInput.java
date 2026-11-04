/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.file;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;

import java.io.IOException;

/**
 * An {@link IndexInput} over a byte array that counts how often each read method is invoked. Tests use it
 * as the input wrapped by {@link AbstractBlockIndexInput} or
 * {@link org.opensearch.index.store.remote.filecache.FileCachedIndexInput} to assert that bulk reads are
 * forwarded as bulk reads instead of being degraded into one element-wise read per value.
 */
public class BulkReadCountingIndexInput extends FilterIndexInput implements RandomAccessInput {

    private final ByteArrayIndexInput delegate;

    public int readByteCalls;
    public int readBytesCalls;
    public int readShortCalls;
    public int readIntCalls;
    public int readLongCalls;
    public int readFloatsCalls;
    public int readIntsCalls;
    public int readLongsCalls;

    public BulkReadCountingIndexInput(String resourceDescription, byte[] data, int offset, int length) {
        this(resourceDescription, new ByteArrayIndexInput(resourceDescription, data, offset, length));
    }

    private BulkReadCountingIndexInput(String resourceDescription, ByteArrayIndexInput delegate) {
        super(resourceDescription, delegate);
        this.delegate = delegate;
    }

    @Override
    public byte readByte() throws IOException {
        readByteCalls++;
        return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        readBytesCalls++;
        in.readBytes(b, offset, len);
    }

    @Override
    public short readShort() throws IOException {
        readShortCalls++;
        return in.readShort();
    }

    @Override
    public int readInt() throws IOException {
        readIntCalls++;
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        readLongCalls++;
        return in.readLong();
    }

    @Override
    public void readFloats(float[] floats, int offset, int len) throws IOException {
        readFloatsCalls++;
        in.readFloats(floats, offset, len);
    }

    @Override
    public void readInts(int[] dst, int offset, int len) throws IOException {
        readIntsCalls++;
        in.readInts(dst, offset, len);
    }

    @Override
    public void readLongs(long[] dst, int offset, int len) throws IOException {
        readLongsCalls++;
        in.readLongs(dst, offset, len);
    }

    @Override
    public byte readByte(long pos) throws IOException {
        return delegate.readByte(pos);
    }

    @Override
    public short readShort(long pos) throws IOException {
        return delegate.readShort(pos);
    }

    @Override
    public int readInt(long pos) throws IOException {
        return delegate.readInt(pos);
    }

    @Override
    public long readLong(long pos) throws IOException {
        return delegate.readLong(pos);
    }
}
