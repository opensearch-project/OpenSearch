/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;

/**
 * A fast {@code LZ4_FRAME} {@link org.apache.arrow.vector.compression.CompressionCodec} backed by
 * {@code lz4-java} ({@link LZ4FrameOutputStream}/{@link LZ4FrameInputStream}, JNI-native with a
 * pure-Java fallback) instead of Arrow's bundled {@code Lz4CompressionCodec}.
 *
 * <p><b>Why this exists.</b> Arrow's own {@code org.apache.arrow.compression.Lz4CompressionCodec}
 * hard-wires commons-compress's {@code FramedLZ4CompressorOutputStream} — a PURE-JAVA block matcher
 * whose back-reference bookkeeping ({@code LZ77Compressor} + a {@code LinkedList} of match pairs in
 * {@code BlockLZ4CompressorOutputStream.clearUnusedPairs}) degrades pathologically on the large
 * buffers a fact-table shuffle produces (TPC-H lineitem): observed as every worker search thread
 * pinned in {@code LZ77Compressor.compress} and the query never finishing (150s+ client timeout, low
 * heap — pure CPU starvation). zstd-jni avoids this because it is native; commons-compress LZ4 does
 * not. {@code lz4-java} restores the conventional "LZ4 is the cheap shuffle codec" property.
 *
 * <p>The on-wire bytes are the STANDARD LZ4 frame format (RFC-style frame with magic
 * {@code 0x184D2204}), so the codec id Arrow stamps in the IPC metadata stays {@code LZ4_FRAME} and
 * any standards-compliant reader — including Arrow's own commons-compress decoder — can still read
 * it. We only swap the IMPLEMENTATION, not the format.
 *
 * <p>Extends {@link AbstractCompressionCodec} so the shared length-prefix framing and the
 * "incompressible buffer is stored uncompressed" fallback are inherited; this class implements only
 * the raw byte transform, mirroring Arrow's {@code Lz4CompressionCodec.doCompress/doDecompress}
 * shape ({@code getBytes → stream copy → setBytes}).
 *
 * @opensearch.internal
 */
final class FastLz4CompressionCodec extends AbstractCompressionCodec {

    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
        int inLen = (int) uncompressedBuffer.writerIndex();
        byte[] inBytes = new byte[inLen];
        uncompressedBuffer.getBytes(0, inBytes);

        // CRITICAL: Arrow compresses EACH Arrow buffer separately (VectorUnloader.appendNodes calls
        // this once per validity / offset / data buffer — thousands per batch on a wide fact table).
        // LZ4FrameOutputStream's default block size is SIZE_4MB, and its constructor allocates AND
        // Arrays.fill-zeroes a ~2×blockSize working buffer EVERY call — so the default makes q7 spend
        // all its time in LZ4FrameOutputStream.<init>/writeBlock zeroing 4 MB per tiny buffer (observed
        // as a multi-minute hang at low heap). SIZE_64KB caps that per-call allocation; Arrow buffers
        // are mostly far smaller than 64 KB so the ratio is unaffected.
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.max(64, inLen / 2));
        try (LZ4FrameOutputStream out = new LZ4FrameOutputStream(baos, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB)) {
            out.write(inBytes);
        } catch (java.io.IOException e) {
            throw new RuntimeException("FastLz4CompressionCodec: LZ4 compress failed", e);
        }
        byte[] outBytes = baos.toByteArray();

        // Contract (mirrors Arrow's Lz4CompressionCodec.doCompress): RESERVE the leading
        // SIZE_OF_UNCOMPRESSED_LENGTH (8) bytes — the parent compress() writes the uncompressed length
        // there afterward via writeUncompressedLength. Writing the payload at offset 0 instead would let
        // the parent overwrite the frame's first 8 bytes → a corrupt frame the reader can't decode.
        ArrowBuf compressed = allocator.buffer(outBytes.length + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH);
        compressed.setBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, outBytes);
        compressed.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + outBytes.length);
        return compressed;
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
        // Contract (mirrors Arrow's Lz4CompressionCodec.doDecompress): the parent decompress() has
        // already consumed the 8-byte uncompressed-length prefix; the actual LZ4-frame payload starts
        // at offset SIZE_OF_UNCOMPRESSED_LENGTH and runs to writerIndex(). Reading from offset 0 would
        // feed the length prefix bytes to the LZ4 decoder → "LZ4 decompress failed".
        long payloadLen = compressedBuffer.writerIndex() - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;
        byte[] inBytes = new byte[(int) payloadLen];
        compressedBuffer.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, inBytes);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(inBytes.length * 2);
        byte[] chunk = new byte[8192];
        try (LZ4FrameInputStream in = new LZ4FrameInputStream(new ByteArrayInputStream(inBytes))) {
            int n;
            while ((n = in.read(chunk)) != -1) {
                baos.write(chunk, 0, n);
            }
        } catch (java.io.IOException e) {
            throw new RuntimeException("FastLz4CompressionCodec: LZ4 decompress failed", e);
        }
        byte[] outBytes = baos.toByteArray();

        ArrowBuf decompressed = allocator.buffer(outBytes.length);
        decompressed.setBytes(0, outBytes);
        decompressed.writerIndex(outBytes.length);
        return decompressed;
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
        return CompressionUtil.CodecType.LZ4_FRAME;
    }
}
