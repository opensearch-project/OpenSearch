/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.io.IOException;

/**
 * StreamContext is used to supply streams to vendor plugins using {@link StreamContext#provideStream}
 */
public class StreamContext {

    private final ThrowingTriFunction<Integer, Long, Long, OffsetStreamContainer, IOException> streamSupplier;
    private final long partSize;
    private final long lastPartSize;
    private final int numberOfParts;

    /**
     * Construct a new StreamProvider object
     *
     * @param streamSupplier An implementation of TransferPartStreamSupplier which will be called when provideStreams is called
     * @param partSize Size of all parts apart from the last one
     * @param lastPartSize Size of the last part
     * @param numberOfParts Total number of parts
     */
    public StreamContext(
        ThrowingTriFunction<Integer, Long, Long, OffsetStreamContainer, IOException> streamSupplier,
        long partSize,
        long lastPartSize,
        int numberOfParts
    ) {
        this.streamSupplier = streamSupplier;
        this.partSize = partSize;
        this.lastPartSize = lastPartSize;
        this.numberOfParts = numberOfParts;
    }

    /**
     * Vendor plugins can use this method to create new streams only when they are required for processing
     * New streams won't be created till this method is called with the specific <code>partNumber</code>
     *
     * @param partNumber The index of the part
     * @return A stream reference to the part requested
     */
    public OffsetStreamContainer provideStream(int partNumber) throws IOException {
        long position = partSize * partNumber;
        long size = (partNumber == numberOfParts - 1) ? lastPartSize : partSize;
        return streamSupplier.apply(partNumber, size, position);
    }

    public int getNumberOfParts() {
        return numberOfParts;
    }
}
