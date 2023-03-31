/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class StreamIterable implements Iterable<Stream> {

    private final TransferPartStreamSupplier streamSupplier;
    private final long partSize;
    private final long lastPartSize;
    private final int numOfParts;
    private int partNumber = 0;

    public StreamIterable(TransferPartStreamSupplier streamSupplier, long partSize, long lastPartSize,
                          int numOfParts) {
        this.streamSupplier = streamSupplier;
        this.partSize = partSize;
        this.lastPartSize = lastPartSize;
        this.numOfParts = numOfParts;
    }

    @Override
    public Iterator<Stream> iterator() {
        return new StreamIterator();
    }

    private class StreamIterator implements Iterator<Stream> {

        @Override
        public boolean hasNext() {
            return partNumber < numOfParts;
        }

        @Override
        public Stream next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No Stream available");
            }

            long position = partSize * partNumber;
            long size = partNumber == numOfParts - 1 ? lastPartSize : partSize;
            if (numOfParts > 1) {
                System.out.println();
            }
            return streamSupplier.supply(partNumber++, size, position);
        }
    }
}
