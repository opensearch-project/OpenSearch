/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

public class StreamProvider {

    private final TransferPartStreamSupplier streamSupplier;
    private final long partSize;
    private final long lastPartSize;
    private final int numOfParts;

    public StreamProvider(TransferPartStreamSupplier streamSupplier, long partSize, long lastPartSize,
                          int numOfParts) {
        this.streamSupplier = streamSupplier;
        this.partSize = partSize;
        this.lastPartSize = lastPartSize;
        this.numOfParts = numOfParts;
    }

    public Stream provideStream(int partNumber) {
        long position = partSize * partNumber;
        long size = (partNumber == numOfParts - 1) ? lastPartSize : partSize;
        return streamSupplier.supply(partNumber, size, position);
    }
}
