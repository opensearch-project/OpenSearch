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
 * ABCDE
 *
 * @opensearch.internal
 */
public class StreamContext {

    private final StreamIterable streamIterable;
    private final long totalContentLength;
    private final int numberOfParts;

    /**
     * ABCDE
     *
     * @param streamIterable
     * @param totalContentLength
     */
    public StreamContext(StreamIterable streamIterable, long totalContentLength, int numberOfParts) {
        this.streamIterable = streamIterable;
        this.totalContentLength = totalContentLength;
        this.numberOfParts = numberOfParts;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public StreamIterable getStreamIterable() {
        return streamIterable;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public long getTotalContentLength() {
        return totalContentLength;
    }

    public int getNumberOfParts() {
        return numberOfParts;
    }
}
