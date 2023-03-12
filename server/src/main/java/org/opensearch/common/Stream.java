/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.io.InputStream;


/**
 * ABCDE
 *
 * @opensearch.internal
 */
public class Stream {

    private final InputStream inputStream;
    private final long contentLength;
    private final long offset;

    /**
     * ABCDE
     *
     * @param inputStream
     * @param contentLength
     * @param offset
     */
    public Stream(InputStream inputStream, long contentLength, long offset) {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
        this.offset = offset;
    }

    /**
     * ABCDE
     *
     * @param inputStream
     * @param contentLength
     */
    public Stream(InputStream inputStream, long contentLength) {
        this.inputStream = inputStream;
        this.contentLength = contentLength;
        this.offset = 0;
    }

    /**
     * ABCDE
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
}
