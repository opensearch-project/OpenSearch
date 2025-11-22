/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.datafusion.RecordBatchStream;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator over Arrow record batches from a RecordBatchStream.
 */
public class RecordBatchIterator implements Iterator<VectorSchemaRoot> {

    private final RecordBatchStream stream;
    private Boolean hasNext;

    public RecordBatchIterator(RecordBatchStream stream) {
        this.stream = stream;
    }

    @Override
    public boolean hasNext() {
        if (hasNext == null) {
            hasNext = stream.loadNextBatch().join();
        }
        return hasNext;
    }

    @Override
    public VectorSchemaRoot next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNext = null;
        return stream.getVectorSchemaRoot();
    }
}
