/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.io.InputStream;
import java.util.function.Supplier;


/**
 * ABCDE
 *
 * @opensearch.internal
 */
public class Stream {

    private final InputStream inputStream;
    private final long contentLength;
    private final long offset;
    private final Supplier<Long> checksumProvider;

    /**
     * Construct a new stream
     *
     * @param inputStream
     * @param contentLength
     * @param offset
     */
    public Stream(InputStream inputStream, long contentLength, long offset, Supplier<Long> checksumProvider) {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
        this.offset = offset;
        this.checksumProvider = checksumProvider;
    }

    /**
     * Return the input stream
     *
     * @return
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public long getContentLength() {
        return contentLength;
    }

    public long getOffset() {
        return offset;
    }

    public Supplier<Long> getChecksumProvider() {
        return checksumProvider;
    }
}
