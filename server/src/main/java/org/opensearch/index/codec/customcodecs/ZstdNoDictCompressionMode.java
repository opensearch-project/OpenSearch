/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import com.github.luben.zstd.Zstd;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/** ZSTD Compression Mode (without a dictionary support). */
public class ZstdNoDictCompressionMode extends CompressionMode {

    private static final int NUM_SUB_BLOCKS = 10;
    private static final int DEFAULT_COMPRESSION_LEVEL = 6;

    private final int compressionLevel;

    /** default constructor */
    protected ZstdNoDictCompressionMode() {
        this.compressionLevel = DEFAULT_COMPRESSION_LEVEL;
    }

    /**
     * Creates a new instance with the given compression level.
     *
     * @param compressionLevel The compression level.
     */
    protected ZstdNoDictCompressionMode(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    /** Creates a new compressor instance.*/
    @Override
    public Compressor newCompressor() {
        return new ZstdCompressor(compressionLevel);
    }

    /** Creates a new decompressor instance. */
    @Override
    public Decompressor newDecompressor() {
        return new ZstdDecompressor();
    }

    /** zstandard compressor */
    private static final class ZstdCompressor extends Compressor {

        private final int compressionLevel;
        private byte[] compressedBuffer;

        /** compressor with a given compresion level */
        public ZstdCompressor(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            compressedBuffer = BytesRef.EMPTY_BYTES;
        }

        private void compress(byte[] bytes, int offset, int length, DataOutput out) throws IOException {
            assert offset >= 0 : "offset value must be greater than 0";

            int blockLength = (length + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(blockLength);

            final int end = offset + length;
            assert end >= 0 : "buffer read size must be greater than 0";

            for (int start = offset; start < end; start += blockLength) {
                int l = Math.min(blockLength, end - start);

                if (l == 0) {
                    out.writeVInt(0);
                    return;
                }

                final int maxCompressedLength = (int) Zstd.compressBound(l);
                compressedBuffer = ArrayUtil.growNoCopy(compressedBuffer, maxCompressedLength);

                int compressedSize = (int) Zstd.compressByteArray(
                    compressedBuffer,
                    0,
                    compressedBuffer.length,
                    bytes,
                    start,
                    l,
                    compressionLevel
                );

                out.writeVInt(compressedSize);
                out.writeBytes(compressedBuffer, compressedSize);
            }
        }

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final int length = (int) buffersInput.size();
            byte[] bytes = new byte[length];
            buffersInput.readBytes(bytes, 0, length);
            compress(bytes, 0, length, out);
        }

        @Override
        public void close() throws IOException {}
    }

    /** zstandard decompressor */
    private static final class ZstdDecompressor extends Decompressor {

        private byte[] compressed;

        /** default decompressor */
        public ZstdDecompressor() {
            compressed = BytesRef.EMPTY_BYTES;
        }

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength : "buffer read size must be within limit";

            if (length == 0) {
                bytes.length = 0;
                return;
            }

            final int blockLength = in.readVInt();
            bytes.offset = bytes.length = 0;
            int offsetInBlock = 0;
            int offsetInBytesRef = offset;

            // Skip unneeded blocks
            while (offsetInBlock + blockLength < offset) {
                final int compressedLength = in.readVInt();
                in.skipBytes(compressedLength);
                offsetInBlock += blockLength;
                offsetInBytesRef -= blockLength;
            }

            // Read blocks that intersect with the interval we need
            while (offsetInBlock < offset + length) {
                bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + blockLength);
                final int compressedLength = in.readVInt();
                if (compressedLength == 0) {
                    return;
                }
                compressed = ArrayUtil.growNoCopy(compressed, compressedLength);
                in.readBytes(compressed, 0, compressedLength);

                int l = Math.min(blockLength, originalLength - offsetInBlock);
                bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + l);

                byte[] output = new byte[l];

                final int uncompressed = (int) Zstd.decompressByteArray(output, 0, l, compressed, 0, compressedLength);
                System.arraycopy(output, 0, bytes.bytes, bytes.length, uncompressed);

                bytes.length += uncompressed;
                offsetInBlock += blockLength;
            }

            bytes.offset = offsetInBytesRef;
            bytes.length = length;

            assert bytes.isValid() : "decompression output is corrupted.";
        }

        @Override
        public Decompressor clone() {
            return new ZstdDecompressor();
        }
    }
}
