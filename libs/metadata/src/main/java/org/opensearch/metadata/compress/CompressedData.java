/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.compress;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.zip.CRC32;

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
     * Creates a new CompressedData from bytes that may or may not be already compressed.
     * Auto-detects compression. If already compressed, stores as-is and computes checksum
     * from the decompressed content. If not compressed, compresses using the default compressor.
     *
     * @param data the bytes (compressed or uncompressed)
     * @throws IOException if compression or decompression fails
     */
    public CompressedData(byte[] data) throws IOException {
        Objects.requireNonNull(data, "data must not be null");
        BytesReference ref = new BytesArray(data);
        Compressor compressor = CompressorRegistry.defaultCompressor();
        if (compressor.isCompressed(ref)) {
            this.compressedBytes = Arrays.copyOf(data, data.length);
            this.checksum = computeCRC32Checksum(BytesReference.toBytes(compressor.uncompress(ref)));
        } else {
            this.compressedBytes = BytesReference.toBytes(compressor.compress(ref));
            this.checksum = computeCRC32Checksum(data);
        }
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

    /**
     * Decompresses the data and returns as bytes.
     *
     * @return uncompressed bytes
     */
    public byte[] uncompressed() {
        try {
            return BytesReference.toBytes(CompressorRegistry.defaultCompressor().uncompress(new BytesArray(compressedBytes)));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot decompress compressed data", e);
        }
    }

    /**
     * Returns the compressed bytes as a {@link BytesReference}.
     */
    public BytesReference compressedReference() {
        return new BytesArray(compressedBytes);
    }

    /**
     * Computes CRC32 checksum for the given data.
     *
     * @param data the data to compute checksum for
     */
    public static int computeCRC32Checksum(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return (int) crc32.getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedData that = (CompressedData) o;

        if (Arrays.equals(compressedBytes, that.compressedBytes)) {
            return true;
        }

        if (checksum != that.checksum) {
            return false;
        }

        return Arrays.equals(uncompressed(), that.uncompressed());
    }

    @Override
    public int hashCode() {
        return checksum;
    }

    @Override
    public String toString() {
        return new String(uncompressed(), StandardCharsets.UTF_8);
    }

}
