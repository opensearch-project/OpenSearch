package org.opensearch.search.query;

import org.opensearch.search.internal.SearchContext;

/**
 * Strategy interface for executing query phases across different engines
 */
public interface QueryPhaseExecutor<C extends SearchContext> {

    boolean execute(C context) throws QueryPhaseExecutionException;

    boolean canHandle(C context);
}
