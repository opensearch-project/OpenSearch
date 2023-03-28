/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream;

import org.opensearch.common.StreamIterable;

/**
 * StreamContext encapsulates all the data required for uploading multiple streams
 */
public class StreamContext {

    private final StreamIterable streamIterable;
    private final long totalContentLength;
    private final int numberOfParts;

    /**
     * Construct a new StreamContext object
     *
     * @param streamIterable A stream iterable to iterate over streams for upload
     * @param totalContentLength The total content length for the upload
     */
    public StreamContext(StreamIterable streamIterable, long totalContentLength, int numberOfParts) {
        this.streamIterable = streamIterable;
        this.totalContentLength = totalContentLength;
        this.numberOfParts = numberOfParts;
    }

    /**
     * @return The stream iterable for the current upload
     */
    public StreamIterable getStreamIterable() {
        return streamIterable;
    }

    /**
     * @return The total content length for the upload
     */
    public long getTotalContentLength() {
        return totalContentLength;
    }

    /**
     * @return The number of parts in current upload
     */
    public int getNumberOfParts() {
        return numberOfParts;
    }
}
