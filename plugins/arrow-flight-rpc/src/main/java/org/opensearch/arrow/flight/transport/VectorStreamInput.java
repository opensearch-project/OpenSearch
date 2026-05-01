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

/**
 * A {@link StreamInput} backed by a {@link VectorSchemaRoot} from the Flight transport.
 *
 * <p>Two factories, mirroring {@link VectorStreamOutput}:
 * <ul>
 *   <li>{@link #forByteSerialized} — reads bytes directly from the shared root. {@code handler.read()}
 *       copies bytes into the response's Java fields, so no ownership transfer is needed.</li>
 *   <li>{@link #forNativeArrow} — zero-copy transfers the shared root's vectors into a
 *       response-owned root before reading, so the returned {@link ArrowBatchResponse} is
 *       independent of the FlightStream lifecycle.</li>
 * </ul>
 *
 * <p>The caller ({@link FlightTransportResponse#nextResponse}) picks the factory based on the
 * {@code opensearch-encoding} header stashed on the client-side {@link HeaderContext} by
 * {@link ClientHeaderMiddleware}.
 *
 * @opensearch.internal
 */
abstract class VectorStreamInput extends StreamInput {

    protected final VectorSchemaRoot root;
    protected final NamedWriteableRegistry registry;

    protected VectorStreamInput(VectorSchemaRoot root, NamedWriteableRegistry registry) {
        this.root = root;
        this.registry = registry;
    }

    /**
     * Byte-serialized path: the shared root carries a single {@code VarBinary} column of chunked
     * bytes written by {@link VectorStreamOutput.ByteSerialized}. Reads are over the shared root;
     * FlightStream retains ownership.
     */
    static VectorStreamInput forByteSerialized(VectorSchemaRoot sharedRoot, NamedWriteableRegistry registry) {
        return new ByteSerialized(sharedRoot, registry);
    }

    /**
     * Native-Arrow path: zero-copy transfers the shared root's vectors into a response-owned root
     * so the returned {@link ArrowBatchResponse} outlives the next FlightStream batch and close.
     */
    static VectorStreamInput forNativeArrow(VectorSchemaRoot sharedRoot, NamedWriteableRegistry registry) {
        VectorSchemaRoot ownedRoot = VectorSchemaRoot.create(
            sharedRoot.getSchema(),
            sharedRoot.getFieldVectors().getFirst().getAllocator()
        );
        FlightUtils.transferRoot(sharedRoot, ownedRoot);
        return new NativeArrow(ownedRoot, registry);
    }

    /**
     * Returns the underlying {@link VectorSchemaRoot}. For {@link NativeArrow} this is the
     * response-owned root; {@link ArrowBatchResponse} grabs it via the receive-side constructor.
     */
    public VectorSchemaRoot getRoot() {
        return root;
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
    public int read() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int available() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void ensureCanReadBytes(int length) {}

    // ── Byte serialization ──

    static final class ByteSerialized extends VectorStreamInput {
        private final VarBinaryVector vector;
        private int row = 0;
        private ByteBuffer buffer = null;

        ByteSerialized(VectorSchemaRoot root, NamedWriteableRegistry registry) {
            super(root, registry);
            this.vector = (VarBinaryVector) root.getVector("0");
        }

        @Override
        public byte readByte() throws IOException {
            if (buffer != null && buffer.hasRemaining()) {
                return buffer.get();
            }
            if (row >= vector.getValueCount()) {
                throw new EOFException("No more rows available in vector");
            }
            byte[] v = vector.get(row++);
            if (v.length == 0) {
                throw new IOException("Empty byte array in vector at row " + (row - 1));
            }
            buffer = ByteBuffer.wrap(v);
            return buffer.get();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            if (offset < 0 || len < 0 || offset + len > b.length) {
                throw new IllegalArgumentException("Invalid offset or length");
            }
            int remaining = len;

            if (buffer != null && buffer.hasRemaining()) {
                int bufferBytes = Math.min(buffer.remaining(), remaining);
                buffer.get(b, offset, bufferBytes);
                offset += bufferBytes;
                remaining -= bufferBytes;
                if (!buffer.hasRemaining()) {
                    buffer = null;
                }
            }

            while (remaining > 0) {
                if (row >= vector.getValueCount()) {
                    throw new EOFException("No more rows available in vector");
                }
                byte[] v = vector.get(row++);
                if (v.length == 0) {
                    throw new IOException("Empty byte array in vector at row " + (row - 1));
                }
                if (v.length <= remaining) {
                    System.arraycopy(v, 0, b, offset, v.length);
                    offset += v.length;
                    remaining -= v.length;
                } else {
                    System.arraycopy(v, 0, b, offset, remaining);
                    buffer = ByteBuffer.wrap(v, remaining, v.length - remaining);
                    remaining = 0;
                }
            }
        }

        /**
         * No-op: the shared root belongs to {@link org.apache.arrow.flight.FlightStream}, which
         * clears the vectors on the next {@code next()} and closes them on stream close.
         */
        @Override
        public void close() {}
    }

    // ── Native Arrow ──

    static final class NativeArrow extends VectorStreamInput {

        NativeArrow(VectorSchemaRoot root, NamedWriteableRegistry registry) {
            super(root, registry);
        }

        @Override
        public byte readByte() {
            throw new UnsupportedOperationException("Native Arrow responses read vectors directly from getRoot()");
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) {
            throw new UnsupportedOperationException("Native Arrow responses read vectors directly from getRoot()");
        }

        /**
         * No-op: the owned root is handed off to {@link ArrowBatchResponse}; its lifecycle is
         * owned by the response, not the stream input.
         */
        @Override
        public void close() {}
    }
}
