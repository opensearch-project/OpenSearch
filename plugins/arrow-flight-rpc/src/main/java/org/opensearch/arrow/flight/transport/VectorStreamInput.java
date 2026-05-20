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
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.arrow.transport.ArrowBatchResponseHandler;
import org.opensearch.arrow.transport.ArrowStreamInput;
import org.opensearch.arrow.transport.VectorTransfer;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Flight's {@link StreamInput} backed by a {@link VectorSchemaRoot}.
 *
 * <p>Two factories:
 * <ul>
 *   <li>{@link #forByteSerialized} — reads bytes directly from the stream root. Used when the
 *       response is not an {@link ArrowBatchResponse}: {@code handler.read()} copies bytes into
 *       the response's Java fields, so no ownership transfer is needed.</li>
 *   <li>{@link #forNativeArrow} — zero-copy transfers the stream root's vectors into a
 *       consumer root before reading, so the returned {@link ArrowBatchResponse} is
 *       independent of the Flight stream's lifecycle.</li>
 * </ul>
 *
 * <p>Flight picks the factory based on whether the registered handler is an
 * {@link ArrowBatchResponseHandler}. Native inputs implement {@link ArrowStreamInput} so
 * {@code ArrowBatchResponse} can claim the batch without knowing about Flight.
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
     * Byte-serialized path: the stream root carries a single {@code VarBinary} column of chunked
     * bytes written by the transport. Reads are over the stream root; FlightStream retains ownership.
     */
    static VectorStreamInput forByteSerialized(VectorSchemaRoot streamRoot, NamedWriteableRegistry registry) {
        return new ByteSerialized(streamRoot, registry);
    }

    /**
     * Transfers the stream root's vectors into a consumer root so the returned response
     * outlives the next Flight batch. Released by {@link NativeArrow#close()} unless the
     * response takes ownership via {@link NativeArrow#claimOwnership()}.
     */
    static VectorStreamInput forNativeArrow(VectorSchemaRoot streamRoot, NamedWriteableRegistry registry) {
        if (streamRoot.getFieldVectors().isEmpty()) {
            throw new IllegalStateException("Native Arrow batch has no field vectors");
        }
        VectorSchemaRoot consumerRoot = VectorSchemaRoot.create(
            streamRoot.getSchema(),
            streamRoot.getFieldVectors().getFirst().getAllocator()
        );
        try {
            VectorTransfer.transferRoot(streamRoot, consumerRoot);
        } catch (Throwable t) {
            consumerRoot.close();
            throw t;
        }
        return new NativeArrow(consumerRoot, registry);
    }

    /**
     * Returns the underlying {@link VectorSchemaRoot}. For {@link NativeArrow} this is the
     * consumer root; {@link ArrowBatchResponse} grabs it via the receive-side constructor.
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

    // No-op: bounds checks happen at read time in ByteSerialized, which throws
    // EOFException when the column is exhausted.
    @Override
    protected void ensureCanReadBytes(int length) {}

    // ── Byte serialization ──

    /** Byte-serialized input: reads bytes from the stream root's single {@code VarBinary} column. */
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
         * No-op: the stream root is owned by the transport, which resets its vectors between
         * batches and closes them when the stream ends.
         */
        @Override
        public void close() {}
    }

    // ── Native Arrow ──

    /** Native Arrow input: consumer root carrying vectors transferred from the Flight stream. */
    static final class NativeArrow extends VectorStreamInput implements ArrowStreamInput {
        private boolean transferred = false;

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

        @Override
        public void claimOwnership() {
            transferred = true;
        }

        /** Releases the consumer root unless {@link #claimOwnership()} was called. */
        @Override
        public void close() {
            if (!transferred && root != null) {
                root.close();
            }
        }
    }
}
