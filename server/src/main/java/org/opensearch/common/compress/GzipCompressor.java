/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress;

import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * {@link Compressor} implementation based on the GZIP compression algorithm.
 *
 * @opensearch.internal
 */
public class GzipCompressor implements Compressor {
    private static final byte[] HEADER = new byte[] { (byte) 0x1f, (byte) 0x8b };

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
        return 0;
    }

    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        return new GZIPInputStream(in);
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        return new GZIPOutputStream(out);
    }

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        return null;
    }

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        return null;
    }

}
