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
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.search.NestedHelper;
import org.opensearch.search.aggregations.AggregationProcessor;
import org.opensearch.search.aggregations.HybridAggregationProcessor;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Custom search implementation to be used at {@link QueryPhase} for Hybrid Query search. For queries other than Hybrid the
 * upstream standard implementation of searcher is called.
 */
public class HybridQueryPhaseSearcher extends QueryPhaseSearcherWrapper {
    private static final Logger logger = LogManager.getLogger(HybridQueryPhaseSearcher.class);

    private final QueryPhaseSearcher defaultQueryPhaseSearcherWithEmptyCollectorContext;
    private final QueryPhaseSearcher concurrentQueryPhaseSearcherWithEmptyCollectorContext;

    public HybridQueryPhaseSearcher() {
        this.defaultQueryPhaseSearcherWithEmptyCollectorContext = new DefaultQueryPhaseSearcherWithEmptyQueryCollectorContext();
        this.concurrentQueryPhaseSearcherWithEmptyCollectorContext = new ConcurrentQueryPhaseSearcherWithEmptyQueryCollectorContext();
    }

    public boolean searchWith(
        final SearchContext searchContext,
        final ContextIndexSearcher searcher,
        final Query query,
        final LinkedList<QueryCollectorContext> collectors,
        final boolean hasFilterCollector,
        final boolean hasTimeout
    ) throws IOException {
        if (!isHybridQuery(query, searchContext)) {
            validateQuery(searchContext, query);
            return super.searchWith(searchContext, searcher, query, collectors, hasFilterCollector, hasTimeout);
        } else {
            Query hybridQuery = extractHybridQuery(searchContext, query);
            QueryPhaseSearcher queryPhaseSearcher = getQueryPhaseSearcher(searchContext);
            queryPhaseSearcher.searchWith(searchContext, searcher, hybridQuery, collectors, hasFilterCollector, hasTimeout);
            // we decide on rescore later in collector manager
            return false;
        }
    }

    private QueryPhaseSearcher getQueryPhaseSearcher(final SearchContext searchContext) {
        return searchContext.shouldUseConcurrentSearch()
            ? concurrentQueryPhaseSearcherWithEmptyCollectorContext
            : defaultQueryPhaseSearcherWithEmptyCollectorContext;
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

    /**
     * Validate the query from neural-search plugin point of view. Current main goal for validation is to block cases
     * when hybrid query is wrapped into other compound queries.
     * For example, if we have Bool query like below we need to throw an error
     * bool: {
     *   should: [
     *      match: {},
     *      hybrid: {
     *        sub_query1 {}
     *        sub_query2 {}
     *      }
     *   ]
     * }
     * TODO add similar validation for other compound type queries like constant_score, function_score etc.
     *
     * @param query query to validate
     */
    private void validateQuery(final SearchContext searchContext, final Query query) {
        if (query instanceof BooleanQuery) {
            List<BooleanClause> booleanClauses = ((BooleanQuery) query).clauses();
            for (BooleanClause booleanClause : booleanClauses) {
                validateNestedBooleanQuery(booleanClause.query(), getMaxDepthLimit(searchContext));
            }
        } else if (query instanceof DisjunctionMaxQuery) {
            for (Query disjunct : (DisjunctionMaxQuery) query) {
                validateNestedDisJunctionQuery(disjunct, getMaxDepthLimit(searchContext));
            }
        }
    }

    private void validateNestedBooleanQuery(final Query query, final int level) {
        if (query instanceof HybridQuery) {
            throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
        }
        if (level <= 0) {
            // ideally we should throw an error here but this code is on the main search workflow path and that might block
            // execution of some queries. Instead, we're silently exit and allow such query to execute and potentially produce incorrect
            // results in case hybrid query is wrapped into such bool query
            logger.error("reached max nested query limit, cannot process bool query with that many nested clauses");
            return;
        }
        if (query instanceof BooleanQuery) {
            for (BooleanClause booleanClause : ((BooleanQuery) query).clauses()) {
                validateNestedBooleanQuery(booleanClause.query(), level - 1);
            }
        }
    }

    private void validateNestedDisJunctionQuery(final Query query, final int level) {
        if (query instanceof HybridQuery) {
            throw new IllegalArgumentException("hybrid query must be a top level query and cannot be wrapped into other queries");
        }
        if (level <= 0) {
            // ideally we should throw an error here but this code is on the main search workflow path and that might block
            // execution of some queries. Instead, we're silently exit and allow such query to execute and potentially produce incorrect
            // results in case hybrid query is wrapped into such dis_max query
            logger.error("reached max nested query limit, cannot process dis_max query with that many nested clauses");
            return;
        }
        if (query instanceof DisjunctionMaxQuery) {
            for (Query disjunct : (DisjunctionMaxQuery) query) {
                validateNestedDisJunctionQuery(disjunct, level - 1);
            }
        }
    }

    private int getMaxDepthLimit(final SearchContext searchContext) {
        Settings indexSettings = searchContext.getQueryShardContext().getIndexSettings().getSettings();
        return MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.get(indexSettings).intValue();
    }

    @Override
    public AggregationProcessor aggregationProcessor(SearchContext searchContext) {
        AggregationProcessor coreAggProcessor = super.aggregationProcessor(searchContext);
        return new HybridAggregationProcessor(coreAggProcessor);
    }

    /**
     * Class that inherits ConcurrentQueryPhaseSearcher implementation but calls its search with only
     * empty query collector context
     */
    final class ConcurrentQueryPhaseSearcherWithEmptyQueryCollectorContext extends ConcurrentQueryPhaseSearcher {
        private ConcurrentQueryPhaseSearcherWithEmptyQueryCollectorContext() {}

        @Override
        protected boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) throws IOException {
            return searchWithCollector(
                searchContext,
                searcher,
                query,
                collectors,
                QueryCollectorContext.EMPTY_CONTEXT,
                hasFilterCollector,
                hasTimeout
            );
        }
    }

    /**
     * Class that inherits DefaultQueryPhaseSearcher implementation but calls its search with only
     * empty query collector context
     */
    final class DefaultQueryPhaseSearcherWithEmptyQueryCollectorContext extends QueryPhase.DefaultQueryPhaseSearcher {
        private DefaultQueryPhaseSearcherWithEmptyQueryCollectorContext() {}

        @Override
        protected boolean searchWithCollector(
            SearchContext searchContext,
            ContextIndexSearcher searcher,
            Query query,
            LinkedList<QueryCollectorContext> collectors,
            boolean hasFilterCollector,
            boolean hasTimeout
        ) throws IOException {
            return searchWithCollector(
                searchContext,
                searcher,
                query,
                collectors,
                QueryCollectorContext.EMPTY_CONTEXT,
                hasFilterCollector,
                hasTimeout
            );
        }
    }

    public static boolean isHybridQuery(final Query query, final SearchContext searchContext) {
        if (query instanceof HybridQuery
            || (Objects.nonNull(searchContext.parsedQuery()) && searchContext.parsedQuery().query() instanceof HybridQuery)) {
            return true;
        }
        return false;
    }

    /**
     * This method checks whether hybrid query is wrapped under boolean query object
     */
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
}
