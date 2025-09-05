package org.opensearch.search.query;

import java.util.LinkedList;

/**
 * Generic query phase that can work with different context and searcher types
 * @param <C> Context type
 * @param <S> Searcher type
 * @param <Q> Query type
 */
public class GenericQueryPhase<C, S, Q> {
    private final GenericQueryPhaseSearcher<C, S, Q> queryPhaseSearcher;

    public GenericQueryPhase(GenericQueryPhaseSearcher<C, S, Q> queryPhaseSearcher) {
        this.queryPhaseSearcher = queryPhaseSearcher;
    }

    public boolean executeInternal(C context, S searcher, Q query) throws QueryPhaseExecutionException {
        try {
            return queryPhaseSearcher.searchWith(context, searcher, query, new LinkedList<>() /* Figure out how to pass collectors */, false, false);
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(null, "Failed to execute query", e);
        }
    }
}
