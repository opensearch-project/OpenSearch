/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream;

import org.opensearch.common.StreamProvder;

/**
 * StreamContext encapsulates all the data required for uploading multiple streams
 */
public class StreamContext {

    private final StreamProvder streamProvider;
    private final int numberOfParts;

    /**
     * Construct a new StreamContext object
     *
     * @param streamProvider A stream provider to provide a stream for a given part number.
     * @param numberOfParts Number of parts of the content referenced by equivalent number of streams.
     */
    public StreamContext(StreamProvder streamProvider, int numberOfParts) {
        this.streamProvider = streamProvider;
        this.numberOfParts = numberOfParts;
    }

    /**
     * @return The stream iterable for the current upload
     */
    public StreamProvder getStreamProvider() {
        return streamProvider;
    }

    /**
     * @return The number of parts in current upload
     */
    public int getNumberOfParts() {
        return numberOfParts;
    }
}
