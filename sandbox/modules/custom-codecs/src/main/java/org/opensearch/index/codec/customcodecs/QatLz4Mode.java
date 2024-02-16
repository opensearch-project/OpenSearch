/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

import com.intel.qat.QatZipper;

/** QLZ4 Compression Mode */
public class QatLz4Mode extends CompressionMode {

    private static final int NUM_SUB_BLOCKS = 10;
    private static final int DEFAULT_COMPRESSION_LEVEL = 6;

    private final int compressionLevel;
    private final QatZipper.Mode qatMode;

    /** default constructor
     * @param accelerationMode The acceleration mode.
     */
    protected QatLz4Mode(String accelerationMode) {
        this.compressionLevel = DEFAULT_COMPRESSION_LEVEL;
        this.qatMode = QatDeflateMode.getMode(accelerationMode);
    }

    /**
     * Creates a new instance.
     *
     * @param compressionLevel The compression level to use.
     * @param accelerationMode The acceleration mode.
     */
    protected QatLz4Mode(int compressionLevel, String accelerationMode) {
        this.compressionLevel = compressionLevel;
        this.qatMode = QatDeflateMode.getMode(accelerationMode);
    }

    @Override
    public Compressor newCompressor() {
        return new QatCompressor(compressionLevel, qatMode);
    }

    @Override
    public Decompressor newDecompressor() {
        return new QatDecompressor(qatMode);
    }

    /** zstandard compressor */
    private static final class QatCompressor extends Compressor {

        private byte[] compressedBuffer;

        private QatZipper qatZipper;

        /** compressor with a given compresion level */
        public QatCompressor(int compressionLevel, QatZipper.Mode mode) {
            compressedBuffer = BytesRef.EMPTY_BYTES;
            qatZipper = Lucene99QatCodec.getCompressor(QatZipper.Algorithm.LZ4, compressionLevel, mode, QatZipper.PollingMode.PERIODICAL);
        }

        private void compress(byte[] bytes, int offset, int length, DataOutput out) throws IOException {
            assert offset >= 0 : "Offset value must be greater than 0.";

            int blockLength = (length + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(blockLength);

            final int end = offset + length;
            assert end >= 0 : "Buffer read size must be greater than 0.";

            for (int start = offset; start < end; start += blockLength) {
                int l = Math.min(blockLength, end - start);

                if (l == 0) {
                    out.writeVInt(0);
                    return;
                }

                final int maxCompressedLength = qatZipper.maxCompressedLength(l);
                compressedBuffer = ArrayUtil.grow(compressedBuffer, maxCompressedLength);

                int compressedSize = qatZipper.compress(bytes, start, l, compressedBuffer, 0, compressedBuffer.length);
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
    private static final class QatDecompressor extends Decompressor {

        private byte[] compressed;
        private QatZipper qatZipper;
        private final QatZipper.Mode qatMode;

        /** default decompressor */
        public QatDecompressor(QatZipper.Mode mode) {
            compressed = BytesRef.EMPTY_BYTES;
            qatZipper = Lucene99QatCodec.getCompressor(QatZipper.Algorithm.LZ4, mode, QatZipper.PollingMode.PERIODICAL);
            qatMode = mode;
        }

        /*resuable decompress function*/
        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength : "Buffer read size must be within limit.";

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

                final int uncompressed = qatZipper.decompress(compressed, 0, compressedLength, output, 0, l);
                System.arraycopy(output, 0, bytes.bytes, bytes.length, uncompressed);

                bytes.length += uncompressed;
                offsetInBlock += blockLength;
            }

            bytes.offset = offsetInBytesRef;
            bytes.length = length;

            assert bytes.isValid() : "Decompression output is corrupted.";
        }

        @Override
        public Decompressor clone() {
            return new QatDecompressor(qatMode);
        }
    }
}
