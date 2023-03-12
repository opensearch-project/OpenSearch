/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream;

import org.opensearch.common.Stream;

import java.util.List;
import java.util.function.Supplier;

/**
 * ABCDE
 *
 * @opensearch.internal
 */
public class StreamContext {

    private final List<Supplier<Stream>> streamSuppliers;
    private final long totalContentLength;

    /**
     * ABCDE
     *
     * @param streamSuppliers
     * @param totalContentLength
     */
    public StreamContext(List<Supplier<Stream>> streamSuppliers, long totalContentLength) {
        this.streamSuppliers = streamSuppliers;
        this.totalContentLength = totalContentLength;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public List<Supplier<Stream>> getStreamSuppliers() {
        return streamSuppliers;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public long getTotalContentLength() {
        return totalContentLength;
    }
}
