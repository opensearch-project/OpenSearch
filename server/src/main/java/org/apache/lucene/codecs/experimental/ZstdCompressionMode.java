/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.apache.lucene.codecs.experimental;

import com.github.luben.zstd.*;
import java.io.IOException;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/** Zstandard Compression Mode */
public class ZstdCompressionMode extends CompressionMode {
    public static final int defaultLevel = 6;
    private static final int NUM_SUB_BLOCKS = 10;
    private static final int DICT_SIZE_FACTOR = 6;
    private final int level;

    /** default constructor */
    ZstdCompressionMode() {
        this.level = defaultLevel;
    }

    /** compression mode for a given compression level */
    ZstdCompressionMode(int level) {
        this.level = level;
    }

    @Override
    public Compressor newCompressor() {
        return new ZSTDCompressor(level);
    }

    @Override
    public Decompressor newDecompressor() {
        return new ZSTDDecompressor();
    }

    /** zstandard compressor */
    private static final class ZSTDCompressor extends Compressor {

        int compressionLevel;
        byte[] compressedBuffer;

        /** compressor with a given compresion level */
        public ZSTDCompressor(int compressionLevel) {
            this.compressionLevel = compressionLevel;
            compressedBuffer = BytesRef.EMPTY_BYTES;
        }

        @Override
        public void close() throws IOException {}

        /*resuable compress function*/
        private void doCompress(byte[] bytes, int off, int len, ZstdCompressCtx cctx, DataOutput out) throws IOException {
            if (len == 0) {
                out.writeVInt(0);
                return;
            }
            final int maxCompressedLength = (int) Zstd.compressBound(len);
            compressedBuffer = ArrayUtil.grow(compressedBuffer, maxCompressedLength);

            int compressedSize = cctx.compressByteArray(compressedBuffer, 0, compressedBuffer.length, bytes, off, len);

            out.writeVInt(compressedSize);
            out.writeBytes(compressedBuffer, compressedSize);
        }

        @Override
        public void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {
            final int dictLength = len / (NUM_SUB_BLOCKS * DICT_SIZE_FACTOR);
            final int blockLength = (len - dictLength + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(dictLength);
            out.writeVInt(blockLength);

            final int end = off + len;

            try (ZstdCompressCtx cctx = new ZstdCompressCtx()) {
                cctx.setLevel(this.compressionLevel);

                // dictionary compression first
                doCompress(bytes, off, dictLength, cctx, out);
                cctx.loadDict(new ZstdDictCompress(bytes, off, dictLength, this.compressionLevel));

                for (int start = off + dictLength; start < end; start += blockLength) {
                    int l = Math.min(blockLength, off + len - start);
                    doCompress(bytes, start, l, cctx, out);
                }
            }
        }
    }

    /** zstandard decompressor */
    private static final class ZSTDDecompressor extends Decompressor {

        byte[] compressed;

        /** default decompressor */
        public ZSTDDecompressor() {
            compressed = BytesRef.EMPTY_BYTES;
        }

        /*resuable decompress function*/
        private void doDecompress(DataInput in, ZstdDecompressCtx dctx, BytesRef bytes, int decompressedLen) throws IOException {
            final int compressedLength = in.readVInt();
            if (compressedLength == 0) {
                return;
            }

            compressed = ArrayUtil.grow(compressed, compressedLength);
            in.readBytes(compressed, 0, compressedLength);

            bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + decompressedLen);
            int uncompressed = dctx.decompressByteArray(bytes.bytes, bytes.length, decompressedLen, compressed, 0, compressedLength);

            if (decompressedLen != uncompressed) {
                throw new IllegalStateException(decompressedLen + " " + uncompressed);
            }
            bytes.length += uncompressed;
        }

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength;

            if (length == 0) {
                bytes.length = 0;
                return;
            }
            final int dictLength = in.readVInt();
            final int blockLength = in.readVInt();
            bytes.bytes = ArrayUtil.grow(bytes.bytes, dictLength);
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
                assert bytes.isValid();
            }
        }

        @Override
        public Decompressor clone() {
            return new ZSTDDecompressor();
        }
    }
}
