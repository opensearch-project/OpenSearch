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

public class EngineLucene implements SearcherOperations<Engine.Searcher, ReferenceManager<OpenSearchDirectoryReader>>{
    @Override
    public EngineSearcherSupplier<Engine.Searcher> acquireSearcherSupplier(Function<Engine.Searcher, Engine.Searcher> wrapper) throws EngineException {
        return null;
    }

    @Override
    public EngineSearcherSupplier<Engine.Searcher> acquireSearcherSupplier(Function<Engine.Searcher, Engine.Searcher> wrapper, Engine.SearcherScope scope) throws EngineException {
        return null;
    }

    @Override
    public Engine.Searcher acquireSearcher(String source) throws EngineException {
        return null;
    }

    @Override
    public Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException {
        return null;
    }

    @Override
    public Engine.Searcher acquireSearcher(String source, Engine.SearcherScope scope, Function<Engine.Searcher, Engine.Searcher> wrapper) throws EngineException {
        return null;
    }

    @Override
    public ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(Engine.SearcherScope scope) {
        return null;
    }

    @Override
    public boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope) {
        return false;
    }
}
