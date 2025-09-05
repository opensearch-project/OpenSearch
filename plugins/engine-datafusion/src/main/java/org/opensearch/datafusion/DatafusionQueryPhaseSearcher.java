package org.opensearch.datafusion;

import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.index.engine.EngineSearcher;
import org.opensearch.search.query.GenericQueryPhaseSearcher;
import org.opensearch.search.query.QueryCollectorContext;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.vectorized.execution.search.spi.RecordBatchStream;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;

/**
 * DataFusion-specific query phase searcher using Substrait queries
 *
 */
public class DatafusionQueryPhaseSearcher implements GenericQueryPhaseSearcher<DatafusionContext,DatafusionSearcher, DatafusionQuery> {

    // How to pass table providers that search other engines such as Lucene ?
    @Override
    public boolean searchWith(
        DatafusionContext context,
        DatafusionSearcher searcher,
        DatafusionQuery datafusionQuery,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {

        List<SearchResultsCollector<RecordBatchStream>> searchCollectors = new ArrayList<>(); // TODO : derive from collectors ?

        // Execute DataFusion query with Substrait plan
        searcher.search(datafusionQuery, searchCollectors);

        // Process results into QuerySearchResult
        context.queryResult().searchTimedOut(false);

        return false; // No rescoring for DataFusion
    }
}
