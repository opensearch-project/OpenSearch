/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.bridge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.exec.read.CatalogSnapshotAwareRefreshListener;
import org.opensearch.index.engine.exec.read.EngineSearcher;
import org.opensearch.index.engine.exec.read.EngineSearcherSupplier;

import java.util.function.Function;

/**
 * Basic interface for reader and searcher to acquire point in time view to search over same data throughout
 * the query lifecycle
 * @param <S> Searcher
 * @param <R> Reader
 */
@ExperimentalApi
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
}
