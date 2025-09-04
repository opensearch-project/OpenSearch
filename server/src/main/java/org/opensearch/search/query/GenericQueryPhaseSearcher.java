package org.opensearch.search.query;

import org.opensearch.search.aggregations.AggregationProcessor;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Generic query phase searcher that can work with different context and searcher types
 * @param <C> Context type (SearchContext for Lucene, EngineReaderContext for DataFusion)
 * @param <S> Searcher type (ContextIndexSearcher for Lucene, ContextEngineSearcher for DataFusion)
 * @param <Q> Query type (Query for Lucene, byte[] for DataFusion Substrait)
 */
// TODO make this part of QueryPhaseSearcher
public interface GenericQueryPhaseSearcher<C, S, Q> {

    boolean searchWith(
        C context,
        S searcher,
        Q query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException;

    default AggregationProcessor aggregationProcessor(C context) {
        return new org.opensearch.search.aggregations.DefaultAggregationProcessor();
    }
}
