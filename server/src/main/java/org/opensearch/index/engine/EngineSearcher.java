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

public interface EngineSearcher {
    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    Engine.SearcherSupplier acquireSearcherSupplier(Function<Engine.Searcher, Engine.Searcher> wrapper) throws EngineException;
    /**
     * Acquires a point-in-time reader that can be used to create {@link Engine.Searcher}s on demand.
     */
    Engine.SearcherSupplier acquireSearcherSupplier(Function<Engine.Searcher, Engine.Searcher> wrapper, Engine.SearcherScope scope) throws EngineException;

    Engine.Searcher acquireSearcher(String source) throws EngineException;

    Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException;

    public Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope, Function<Engine.Searcher, Engine.Searcher> wrapper) throws EngineException;

    ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(Engine.SearcherScope scope);

    boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope);
}
