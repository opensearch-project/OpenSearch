/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultBatchIterator;
import org.opensearch.datafusion.search.RecordBatchIterator;

/**
 * Adapts a {@link RecordBatchIterator} (which yields Arrow
 * {@link VectorSchemaRoot} instances) to the engine-agnostic
 * {@link EngineResultBatchIterator} interface, wrapping each batch
 * in a {@link DataFusionResultBatch}.
 *
 * @opensearch.internal
 */
public class DataFusionResultBatchIterator implements EngineResultBatchIterator {

    private final RecordBatchIterator delegate;

    public DataFusionResultBatchIterator(RecordBatchIterator delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public EngineResultBatch next() {
        VectorSchemaRoot root = delegate.next();
        return new DataFusionResultBatch(root);
    }
}
