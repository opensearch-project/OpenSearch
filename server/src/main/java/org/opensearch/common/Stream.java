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
 * Model composed of an input stream, the total content length and offset
 */
public class Stream {

    private final InputStream inputStream;
    private final long contentLength;
    private final long offset;
    private final Supplier<Long> checksumProvider;

    /**
     * Construct a new stream object
     *
     * @param inputStream The input stream that is to be encapsulated
     * @param contentLength The total content length that is to be read from the stream
     * @param offset The offset pointer that this stream reads from in the file
     */
    public Stream(InputStream inputStream, long contentLength, long offset, Supplier<Long> checksumProvider) {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
        this.offset = offset;
        this.checksumProvider = checksumProvider;
    }

    /**
     * @return The input stream this object is reading from
     */
    public InputStream getInputStream() {
        return inputStream;
    }

    /**
     * @return The total length of the content that has to be read from this stream
     */
    public long getContentLength() {
        return contentLength;
    }

    /**
     * @return The offset pointer in the file that this stream is reading from
     */
    public long getOffset() {
        return offset;
    }

    public Supplier<Long> getChecksumProvider() {
        return checksumProvider;
    }
}
