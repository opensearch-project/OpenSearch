/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.SearchExecutionContext;

import java.io.IOException;

/**
 * Searcher and reader acquisition operations for a search execution engine.
 *
 * @param <C> the context type
 * @param <S> the searcher type
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearcherOperations<C extends SearchExecutionContext, S extends EngineSearcher<C>> {

    /**
     * Acquire a point-in-time searcher supplier. Holds a reader reference
     * and can create searchers on demand. Caller must close the supplier
     * when done to release the reader reference.
     */
    EngineSearcherSupplier<S> acquireSearcherSupplier() throws IOException;

    /**
     * Acquire a point-in-time searcher directly.
     */
    default S acquireSearcher(String source) throws IOException {
        return acquireSearcherSupplier().acquireSearcher(source);
    }

    /**
     * Get the reader/reference manager for this engine.
     */
    EngineReaderManager<?> getReferenceManager();
}
