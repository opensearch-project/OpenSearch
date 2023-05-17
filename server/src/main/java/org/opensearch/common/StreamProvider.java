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
 * StreamProvider is used to supply streams to vendor plugins using <code>StreamProvider#provideStream</code>
 */
public class StreamProvider {

    private final TransferPartStreamSupplier streamSupplier;
    private final long partSize;
    private final long lastPartSize;
    private final int numOfParts;

    /**
     * Construct a new StreamProvider object
     *
     * @param streamSupplier An implementation of TransferPartStreamSupplier which will be called when provideStreams is called
     * @param partSize Size of all parts apart from the last one
     * @param lastPartSize Size of the last part
     * @param numOfParts Total number of parts
     */
    public StreamProvider(TransferPartStreamSupplier streamSupplier, long partSize, long lastPartSize, int numOfParts) {
        this.streamSupplier = streamSupplier;
        this.partSize = partSize;
        this.lastPartSize = lastPartSize;
        this.numOfParts = numOfParts;
    }

    /**
     * @param partNumber The index of the part
     * @return A stream reference to the part requested
     */
    public Stream provideStream(int partNumber) throws IOException {
        long position = partSize * partNumber;
        long size = (partNumber == numOfParts - 1) ? lastPartSize : partSize;
        return streamSupplier.supply(partNumber, size, position);
    }
}
