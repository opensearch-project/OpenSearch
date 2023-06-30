/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/** Zstandard Compression Mode */
public class ZstdCompressionMode extends CompressionMode {

    private static final int NUM_SUB_BLOCKS = 10;
    private static final int DICT_SIZE_FACTOR = 6;
    private static final int DEFAULT_COMPRESSION_LEVEL = 6;

    private final int compressionLevel;

    /** default constructor */
    protected ZstdCompressionMode() {
        this.compressionLevel = DEFAULT_COMPRESSION_LEVEL;
    }

    /**
     * Creates a new instance.
     *
     * @param compressionLevel The compression level to use.
     */
    protected ZstdCompressionMode(int compressionLevel) {
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

        /*resuable compress function*/
        private void doCompress(byte[] bytes, int offset, int length, ZstdCompressCtx cctx, DataOutput out) throws IOException {
            if (length == 0) {
                out.writeVInt(0);
                return;
            }
            final int maxCompressedLength = (int) Zstd.compressBound(length);
            compressedBuffer = ArrayUtil.growNoCopy(compressedBuffer, maxCompressedLength);

            int compressedSize = cctx.compressByteArray(compressedBuffer, 0, compressedBuffer.length, bytes, offset, length);

            out.writeVInt(compressedSize);
            out.writeBytes(compressedBuffer, compressedSize);
        }

        private void compress(byte[] bytes, int offset, int length, DataOutput out) throws IOException {
            assert offset >= 0 : "offset value must be greater than 0";

            final int dictLength = length / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR);
            final int blockLength = (length - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(dictLength);
            out.writeVInt(blockLength);

            final int end = offset + length;
            assert end >= 0 : "buffer read size must be greater than 0";

            try (ZstdCompressCtx cctx = new ZstdCompressCtx()) {
                cctx.setLevel(compressionLevel);

                // dictionary compression first
                doCompress(bytes, offset, dictLength, cctx, out);
                cctx.loadDict(new ZstdDictCompress(bytes, offset, dictLength, compressionLevel));

                for (int start = offset + dictLength; start < end; start += blockLength) {
                    int l = Math.min(blockLength, end - start);
                    doCompress(bytes, start, l, cctx, out);
                }
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

        private byte[] compressedBuffer;

        /** default decompressor */
        public ZstdDecompressor() {
            compressedBuffer = BytesRef.EMPTY_BYTES;
        }

        /*resuable decompress function*/
        private void doDecompress(DataInput in, ZstdDecompressCtx dctx, BytesRef bytes, int decompressedLen) throws IOException {
            final int compressedLength = in.readVInt();
            if (compressedLength == 0) {
                return;
            }

            compressedBuffer = ArrayUtil.growNoCopy(compressedBuffer, compressedLength);
            in.readBytes(compressedBuffer, 0, compressedLength);

            bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + decompressedLen);
            int uncompressed = dctx.decompressByteArray(bytes.bytes, bytes.length, decompressedLen, compressedBuffer, 0, compressedLength);

            if (decompressedLen != uncompressed) {
                throw new IllegalStateException(decompressedLen + " " + uncompressed);
            }
            bytes.length += uncompressed;
        }

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength : "buffer read size must be within limit";

            if (length == 0) {
                bytes.length = 0;
                return;
            }
            final int dictLength = in.readVInt();
            final int blockLength = in.readVInt();
            bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, dictLength);
            bytes.offset = bytes.length = 0;

            try (ZstdDecompressCtx dctx = new ZstdDecompressCtx()) {

                // decompress dictionary first
                doDecompress(in, dctx, bytes, dictLength);

                dctx.loadDict(new ZstdDictDecompress(bytes.bytes, 0, dictLength));

                int offsetInBlock = dictLength;
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
                    int l = Math.min(blockLength, originalLength - offsetInBlock);
                    doDecompress(in, dctx, bytes, l);
                    offsetInBlock += blockLength;
                }

                bytes.offset = offsetInBytesRef;
                bytes.length = length;

                assert bytes.isValid() : "decompression output is corrupted";
            }
        }

        @Override
        public Decompressor clone() {
            return new ZstdDecompressor();
        }
    }
}
