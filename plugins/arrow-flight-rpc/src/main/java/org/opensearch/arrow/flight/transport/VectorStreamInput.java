/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

class VectorStreamInput extends StreamInput {

    private final VarBinaryVector vector;
    private final NamedWriteableRegistry registry;
    private int row = 0;
    private ByteBuffer buffer = null;

    public VectorStreamInput(VectorSchemaRoot root, NamedWriteableRegistry registry) {
        vector = (VarBinaryVector) root.getVector("0");
        this.registry = registry;
    }

    @Override
    public byte readByte() throws IOException {
        // Check if buffer has remaining bytes
        if (buffer != null && buffer.hasRemaining()) {
            return buffer.get();
        }
        // No buffer or buffer exhausted, read from vector
        if (row >= vector.getValueCount()) {
            throw new EOFException("No more rows available in vector");
        }
        byte[] v = vector.get(row++);
        if (v.length == 0) {
            throw new IOException("Empty byte array in vector at row " + (row - 1));
        }
        // Wrap the byte array in buffer for future reads
        buffer = ByteBuffer.wrap(v);
        return buffer.get(); // Read the first byte
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (offset < 0 || len < 0 || offset + len > b.length) {
            throw new IllegalArgumentException("Invalid offset or length");
        }
        int remaining = len;

        // First, exhaust any remaining bytes in the buffer
        if (buffer != null && buffer.hasRemaining()) {
            int bufferBytes = Math.min(buffer.remaining(), remaining);
            buffer.get(b, offset, bufferBytes);
            offset += bufferBytes;
            remaining -= bufferBytes;
            if (!buffer.hasRemaining()) {
                buffer = null; // Clear buffer if exhausted
            }
        }

        // Read from vector if more bytes are needed
        while (remaining > 0) {
            if (row >= vector.getValueCount()) {
                throw new EOFException("No more rows available in vector");
            }
            byte[] v = vector.get(row++);
            if (v.length == 0) {
                throw new IOException("Empty byte array in vector at row " + (row - 1));
            }
            if (v.length <= remaining) {
                // The entire vector row can be consumed
                System.arraycopy(v, 0, b, offset, v.length);
                offset += v.length;
                remaining -= v.length;
            } else {
                // Partial read from vector row
                System.arraycopy(v, 0, b, offset, remaining);
                // Store remaining bytes in buffer without copying
                buffer = ByteBuffer.wrap(v, remaining, v.length - remaining);
                remaining = 0;
            }
        }
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(Class<C> categoryClass) throws IOException {
        String name = readString();
        Writeable.Reader<? extends C> reader = namedWriteableRegistry().getReader(categoryClass, name);
        return reader.read(this);
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(Class<C> categoryClass, String name) throws IOException {
        Writeable.Reader<? extends C> reader = namedWriteableRegistry().getReader(categoryClass, name);
        return reader.read(this);
    }

    @Override
    public NamedWriteableRegistry namedWriteableRegistry() {
        return registry;
    }

    @Override
    public void close() throws IOException {
        vector.close();
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int available() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {

    }
}
