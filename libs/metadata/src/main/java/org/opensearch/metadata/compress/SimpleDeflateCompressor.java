/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.compress;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;

/**
 * Simple DEFLATE compressor compatible with OpenSearch's wire format.
 * <p>
 * This is a standalone-friendly implementation that uses only {@code java.util.zip}
 * and {@code libs/core} types. It matches the server's {@code DeflateCompressor} format
 * (header, compression level, raw deflate) but without server-specific dependencies
 * like {@code BytesStreamOutput} or ThreadLocal pooling.
 * <p>
 * When the server module is on the classpath, its optimized {@code DeflateCompressor}
 * is registered under the same {@code "DEFLATE"} name and takes precedence.
 */
public class SimpleDeflateCompressor implements Compressor {

    /** Creates a new SimpleDeflateCompressor. */
    public SimpleDeflateCompressor() {}

    /** Compressor name for SPI registration. Distinct from server's {@code "DEFLATE"} to avoid conflicts. */
    public static final String NAME = "SIMPLE_DEFLATE";

    private static final byte[] HEADER = new byte[] { 'D', 'F', 'L', '\0' };
    private static final int LEVEL = 3;
    private static final int BUFFER_SIZE = 4096;

    @Override
    public boolean isCompressed(BytesReference bytes) {
        if (bytes.length() < HEADER.length) {
            return false;
        }
        for (int i = 0; i < HEADER.length; i++) {
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
            throw new IllegalArgumentException("Input stream is not compressed with DEFLATE!");
        }
        return new BufferedInputStream(new InflaterInputStream(in, new Inflater(true), BUFFER_SIZE), BUFFER_SIZE);
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        out.write(HEADER);
        return new BufferedOutputStream(new DeflaterOutputStream(out, new Deflater(LEVEL, true), BUFFER_SIZE, true), BUFFER_SIZE);
    }

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        Inflater inflater = new Inflater(true);
        try (InflaterOutputStream ios = new InflaterOutputStream(buffer, inflater)) {
            byte[] compressed = BytesReference.toBytes(bytesReference);
            ios.write(compressed, HEADER.length, compressed.length - HEADER.length);
        } finally {
            inflater.end();
        }
        return new BytesArray(buffer.toByteArray());
    }

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(HEADER);
        Deflater deflater = new Deflater(LEVEL, true);
        try (DeflaterOutputStream dos = new DeflaterOutputStream(buffer, deflater, true)) {
            byte[] uncompressed = BytesReference.toBytes(bytesReference);
            dos.write(uncompressed);
        } finally {
            deflater.end();
        }
        return new BytesArray(buffer.toByteArray());
    }
}
