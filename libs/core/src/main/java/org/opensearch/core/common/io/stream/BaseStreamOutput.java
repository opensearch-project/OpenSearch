/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.core.common.io.stream;

import org.opensearch.Version;
import org.opensearch.common.Nullable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Foundation class for writing core types over the transport stream
 *
 * todo: refactor {@code StreamOutput} primitive writers to this class
 *
 * @opensearch.internal
 */
public abstract class BaseStreamOutput extends OutputStream {
    protected static final ThreadLocal<byte[]> scratch = ThreadLocal.withInitial(() -> new byte[1024]);
    private static byte ZERO = 0;
    private static byte ONE = 1;
    private static byte TWO = 2;
    protected Version version = Version.CURRENT;

    /**
     * The version of the node on the other side of this stream.
     */
    public Version getVersion() {
        return this.version;
    }

    /**
     * Set the version of the node on the other side of this stream.
     */
    public void setVersion(Version version) {
        this.version = version;
    }

    /**
     * Closes this stream to further operations.
     */
    @Override
    public abstract void close() throws IOException;

    /**
     * Forces any buffered output to be written.
     */
    @Override
    public abstract void flush() throws IOException;

    public abstract void reset() throws IOException;

    /**
     * Writes a single byte.
     */
    public abstract void writeByte(byte b) throws IOException;

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param offset the offset in the byte array
     * @param length the number of bytes to write
     */
    public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

    /**
     * Writes a boolean.
     */
    public void writeBoolean(boolean b) throws IOException {
        writeByte(b ? ONE : ZERO);
    }

    public void writeOptionalBoolean(@Nullable Boolean b) throws IOException {
        if (b == null) {
            writeByte(TWO);
        } else {
            writeBoolean(b);
        }
    }

    /**
     * Writes an int as four bytes.
     */
    public void writeInt(int i) throws IOException {
        final byte[] buffer = scratch.get();
        buffer[0] = (byte) (i >> 24);
        buffer[1] = (byte) (i >> 16);
        buffer[2] = (byte) (i >> 8);
        buffer[3] = (byte) i;
        writeBytes(buffer, 0, 4);
    }

    /**
     * Writes an optional {@link Integer}.
     */
    public void writeOptionalInt(@Nullable Integer integer) throws IOException {
        if (integer == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeInt(integer);
        }
    }

    /**
     * Writes an int in a variable-length format.  Writes between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers
     * will always use all 5 bytes and are therefore better serialized
     * using {@link #writeInt}
     */
    public void writeVInt(int i) throws IOException {
        /*
         * Shortcut writing single byte because it is very, very common and
         * can skip grabbing the scratch buffer. This is marginally slower
         * than hand unrolling the entire encoding loop but hand unrolling
         * the encoding loop blows out the method size so it can't be inlined.
         * In that case benchmarks of the method itself are faster but
         * benchmarks of methods that use this method are slower.
         * This is philosophically in line with vint in general - it biases
         * twoards being simple and fast for smaller numbers.
         */
        if (Integer.numberOfLeadingZeros(i) >= 25) {
            writeByte((byte) i);
            return;
        }
        byte[] buffer = scratch.get();
        int index = 0;
        do {
            buffer[index++] = ((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        } while ((i & ~0x7F) != 0);
        buffer[index++] = ((byte) i);
        writeBytes(buffer, 0, index);
    }

    public void writeOptionalVInt(@Nullable Integer integer) throws IOException {
        if (integer == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeVInt(integer);
        }
    }
}
