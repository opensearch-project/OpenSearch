/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Wrapper class for QueryPhaseSearcher that handles path selection for concurrent vs
 * non-concurrent search query phase and aggregation processor.
 *
 * @opensearch.internal
 */
public class QueryPhaseSearcherWrapper implements QueryPhaseSearcher {
    private static final Logger LOGGER = LogManager.getLogger(QueryPhaseSearcherWrapper.class);
    private final QueryPhaseSearcher defaultQueryPhaseSearcher;
    private final QueryPhaseSearcher concurrentQueryPhaseSearcher;

    public QueryPhaseSearcherWrapper() {
        this.defaultQueryPhaseSearcher = new QueryPhase.DefaultQueryPhaseSearcher();
        this.concurrentQueryPhaseSearcher = new ConcurrentQueryPhaseSearcher();
    }

    /**
     * Perform search using {@link CollectorManager}
     *
     * @param searchContext      search context
     * @param searcher           context index searcher
     * @param query              query
     * @param hasTimeout         "true" if timeout was set, "false" otherwise
     * @return is rescoring required or not
     * @throws java.io.IOException IOException
     */
    @Override
    public boolean searchWith(
        SearchContext searchContext,
        ContextIndexSearcher searcher,
        Query query,
        LinkedList<QueryCollectorContext> collectors,
        boolean hasFilterCollector,
        boolean hasTimeout
    ) throws IOException {
        if (isHybridQuery(query, searchContext)) {
            query = extractHybridQuery(searchContext, query);
        }
        if (searchContext.shouldUseConcurrentSearch()) {
            return concurrentQueryPhaseSearcher.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        } else {
            return defaultQueryPhaseSearcher.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        }
    }

    /**
     * {@link AggregationProcessor} to use to setup and post process aggregation related collectors during search request
     * @param searchContext search context
     * @return {@link AggregationProcessor} to use
     */
    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        if (searchContext.shouldUseConcurrentSearch()) {
            return concurrentQueryPhaseSearcher.aggregationProcessor(searchContext);
        } else {
            return defaultQueryPhaseSearcher.aggregationProcessor(searchContext);
        }
    }

    protected Query extractHybridQuery(final SearchContext searchContext, final Query query) {
        if (isHybridQueryWrappedInBooleanQuery(searchContext, query)) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            if (!(booleanClauses.get(0).query() instanceof HybridQuery)) {
                throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
            }
            HybridQuery hybridQuery = (HybridQuery) booleanClauses.get(0).query();
            List<BooleanClause> filterQueries = booleanClauses.stream().skip(1).collect(Collectors.toList());
            HybridQuery hybridQueryWithFilter = new HybridQuery(hybridQuery.getSubQueries(), hybridQuery.getQueryContext(), filterQueries);
            return hybridQueryWithFilter;
        }
        return query;
    }

    public static boolean isHybridQueryWrappedInBooleanQuery(final SearchContext searchContext, final Query query) {
        return ((hasAliasFilter(query, searchContext) || hasNestedFieldOrNestedDocs(query, searchContext))
            && isWrappedHybridQuery(query)
            && !((BooleanQuery) query).clauses().isEmpty());
    }

    private static boolean hasNestedFieldOrNestedDocs(final Query query, final SearchContext searchContext) {
        return searchContext.mapperService().hasNested() && new NestedHelper(searchContext.mapperService()).mightMatchNestedDocs(query);
    }

    private static boolean isWrappedHybridQuery(final Query query) {
        return query instanceof BooleanQuery
            && ((BooleanQuery) query).clauses().stream().anyMatch(clauseQuery -> clauseQuery.query() instanceof HybridQuery);
    }

    private static boolean hasAliasFilter(final Query query, final SearchContext searchContext) {
        return Objects.nonNull(searchContext.aliasFilter());
    }

    public static boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery
            || (Objects.nonNull(searchContext.parsedQuery()) && searchContext.parsedQuery().query() instanceof HybridQuery)) {
            return true;
        }
        return false;
    }
}
