/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

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

    @Override
    public Compressor newCompressor() {
        return new ZSTDCompressor(compressionLevel);
    }

    @Override
    public Decompressor newDecompressor() {
        return new ZSTDDecompressor();
    }

    /** zstandard compressor */
    private static final class ZSTDCompressor extends Compressor {

        private final int compressionLevel;
        private byte[] compressedBuffer;

        /** compressor with a given compresion level */
        public ZSTDCompressor(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            compressedBuffer = BytesRef.EMPTY_BYTES;
        }

        @Override
        public void close() throws IOException {}

        private void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {

            int blockLength = (len + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(blockLength);

            final int end = off + len;

            for (int start = off; start < end; start += blockLength) {
                int l = Math.min(blockLength, off + len - start);

                if (l == 0) {
                    out.writeVInt(0);
                    return;
                }

                final int maxCompressedLength = (int) Zstd.compressBound(l);
                compressedBuffer = ArrayUtil.grow(compressedBuffer, maxCompressedLength);

                int compressedSize = (int) Zstd.compressByteArray(
                    compressedBuffer,
                    0,
                    compressedBuffer.length,
                    bytes,
                    start,
                    l,
                    this.compressionLevel
                );

                out.writeVInt(compressedSize);
                out.writeBytes(compressedBuffer, compressedSize);
            }
        }

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final int len = (int) buffersInput.size();
            byte[] bytes = new byte[len];
            buffersInput.readBytes(bytes, 0, len);
            compress(bytes, 0, len, out);
        }
    }

    /** zstandard decompressor */
    private static final class ZSTDDecompressor extends Decompressor {

        private byte[] compressed;

        /** default decompressor */
        public ZSTDDecompressor() {
            compressed = BytesRef.EMPTY_BYTES;
        }

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength;

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
                compressed = ArrayUtil.grow(compressed, compressedLength);
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
            assert bytes.isValid();
        }

        @Override
        public Decompressor clone() {
            return new ZSTDDecompressor();
        }
    }
}
