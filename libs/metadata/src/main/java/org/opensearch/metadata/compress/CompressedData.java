/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.compress;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Generic data holder class for compressed data.
 * This class stores compressed bytes and their checksum.
 * <p>
 * Can be used for any type of compressed content (XContent, raw bytes, etc.)
 */
@ExperimentalApi
public final class CompressedData implements Writeable {

    private final byte[] compressedBytes;
    private final int checksum;

    /**
     * Creates a new CompressedData object.
     *
     * @param compressedBytes the compressed bytes (must not be null)
     * @param checksum        the checksum of the uncompressed content
     * @throws NullPointerException if compressedBytes is null
     */
    public CompressedData(byte[] compressedBytes, int checksum) {
        Objects.requireNonNull(compressedBytes, "compressedBytes must not be null");
        this.compressedBytes = Arrays.copyOf(compressedBytes, compressedBytes.length);
        this.checksum = checksum;
    }

    /**
     * Creates a new CompressedData object from a stream.
     *
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public CompressedData(StreamInput in) throws IOException {
        this.checksum = in.readInt();
        byte[] bytes = in.readByteArray();
        this.compressedBytes = Arrays.copyOf(bytes, bytes.length);
    }

    /**
     * Returns a copy of the compressed bytes.
     * A defensive copy is returned to prevent external mutation of internal state.
     *
     * @return a copy of the compressed bytes
     */
    public byte[] compressedBytes() {
        return Arrays.copyOf(compressedBytes, compressedBytes.length);
    }

    /**
     * Returns the checksum of the uncompressed content.
     *
     * @return the checksum value
     */
    public int checksum() {
        return checksum;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(checksum);
        out.writeByteArray(compressedBytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedData that = (CompressedData) o;
        return checksum == that.checksum && Arrays.equals(compressedBytes, that.compressedBytes);
    }

    @Override
    public int hashCode() {
        return checksum;
    }
}
