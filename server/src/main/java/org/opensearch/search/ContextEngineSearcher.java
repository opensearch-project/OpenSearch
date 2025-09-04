package org.opensearch.search;

import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;

/**
 * Engine-agnostic equivalent of ContextIndexSearcher that wraps EngineSearcher
 * and provides search context awareness
 */
public record ContextEngineSearcher<Q>(EngineSearcher<Q> engineSearcher,
                                       SearchContext searchContext) implements EngineSearcher<Q> {

    @Override
    public String source() {
        return engineSearcher.source();
    }

    @Override
    public void search(Q query, List<SearchResultsCollector<?>> collectors) throws IOException {
        engineSearcher.search(query, collectors);
    }

    @Override
    public void close() {
        engineSearcher.close();
    }
}
