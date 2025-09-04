package org.opensearch.search.query;

import org.opensearch.search.internal.SearchContext;

/**
 * Lucene-specific query phase executor
 */
public class LuceneQueryPhaseExecutor implements QueryPhaseExecutor<SearchContext> {
    
    @Override
    public boolean execute(SearchContext context) throws QueryPhaseExecutionException {
        return QueryPhase.executeInternal(context);
    }
    
    @Override
    public boolean canHandle(SearchContext context) {
        return context != null;
    }
}
