/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * {@link Compressor} implementation based on the ZSTD compression algorithm.
 *
 * @opensearch.internal
 */
public class ZstdCompressor implements Compressor {
    // An arbitrary header that we use to identify compressed streams
    // It needs to be different from other compressors and to not be specific
    // enough so that no stream starting with these bytes could be detected as
    // a XContent
    private static final byte[] HEADER = new byte[] { 'Z', 'S', 'T', 'D', '\0' };

    private static final int LEVEL = 3;

    private static final int BUFFER_SIZE = 4096;

    @Override
    public boolean isCompressed(BytesReference bytes) {
        if (bytes.length() < HEADER.length) {
            return false;
        }
        for (int i = 0; i < HEADER.length; ++i) {
            if (bytes.get(i) != HEADER[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int headerLength() {
        return HEADER.length;
    }

    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        final byte[] header = in.readNBytes(HEADER.length);
        if (Arrays.equals(header, HEADER) == false) {
            throw new IllegalArgumentException("Input stream is not compressed with ZSTD!");
        }
        return new ZstdInputStreamNoFinalizer(new BufferedInputStream(in, BUFFER_SIZE), RecyclingBufferPool.INSTANCE);
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        out.write(HEADER);
        return new ZstdOutputStreamNoFinalizer(new BufferedOutputStream(out, BUFFER_SIZE), RecyclingBufferPool.INSTANCE, LEVEL);
    }

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        throw new UnsupportedOperationException("ZSTD compression is supported only for snapshotting");
    }

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        throw new UnsupportedOperationException("ZSTD compression is supported only for snapshotting");
    }
}
