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

/**
 * {@link Compressor} no compressor implementation.
 *
 * @opensearch.internal
 */
public class NoneCompressor implements Compressor {
    @Override
    public boolean isCompressed(BytesReference bytes) {
        return false;
    }

    @Override
    public int headerLength() {
        return 0;
    }

    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        return in;
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        return out;
    }

    @Override
    public BytesReference uncompress(BytesReference bytesReference) throws IOException {
        return bytesReference;
    }

    @Override
    public BytesReference compress(BytesReference bytesReference) throws IOException {
        return bytesReference;
    }

}
