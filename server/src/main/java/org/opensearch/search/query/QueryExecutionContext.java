package org.opensearch.search.query;

/**
 * Common interface for query execution contexts
 */
public interface QueryExecutionContext {
    
    /**
     * Execute query phase for this context
     * @return whether rescoring phase should be executed
     */
    boolean executeQueryPhase() throws QueryPhaseExecutionException;
}
