/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;

import java.util.function.Function;

public interface SearcherOperations<S extends EngineSearcher, R> {
    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    EngineSearcherSupplier<S> acquireSearcherSupplier(Function<S, S> wrapper) throws EngineException;
    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    EngineSearcherSupplier<S> acquireSearcherSupplier(Function<S, S> wrapper, Engine.SearcherScope scope) throws EngineException;

    S acquireSearcher(String source) throws EngineException;

    S acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException;

    S acquireSearcher(String source, Engine.SearcherScope scope, Function<S, S> wrapper) throws EngineException;

    R getReferenceManager(Engine.SearcherScope scope);

    boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope);

    default CatalogSnapshotAwareRefreshListener getRefreshListener(Engine.SearcherScope searcherScope) {
        // default is no-op, TODO : revisit this
        return null;
    }

    default FileDeletionListener getFileDeletionListener(Engine.SearcherScope searcherScope) {
        return null;
    }
}
