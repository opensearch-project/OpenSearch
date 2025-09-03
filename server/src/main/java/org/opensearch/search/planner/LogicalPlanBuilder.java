/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.planner.nodes.BooleanPlanNode;
import org.opensearch.search.planner.nodes.GenericPlanNode;
import org.opensearch.search.planner.nodes.MatchAllPlanNode;
import org.opensearch.search.planner.nodes.MatchPlanNode;
import org.opensearch.search.planner.nodes.RangePlanNode;
import org.opensearch.search.planner.nodes.TermPlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Builds a logical query plan from OpenSearch QueryBuilder.
 * This converts the query structure into a tree of QueryPlanNodes
 * that can be analyzed and optimized.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LogicalPlanBuilder {

    private final QueryShardContext queryShardContext;
    private final CostEstimator costEstimator;

    public LogicalPlanBuilder(QueryShardContext queryShardContext) {
        this.queryShardContext = queryShardContext;
        this.costEstimator = new CostEstimator(queryShardContext);
    }

    /**
     * Builds a logical plan from a QueryBuilder.
     *
     * @param queryBuilder The query to build a plan for
     * @return The root node of the query plan
     * @throws Exception If the query cannot be converted to Lucene query
     */
    public QueryPlanNode build(QueryBuilder queryBuilder) throws Exception {
        if (queryBuilder == null) {
            return null;
        }

        // Try to build directly from QueryBuilder for known types
        if (queryBuilder instanceof BoolQueryBuilder
            || queryBuilder instanceof TermQueryBuilder
            || queryBuilder instanceof RangeQueryBuilder
            || queryBuilder instanceof MatchQueryBuilder) {
            return buildFromQueryBuilder(queryBuilder);
        }

        // Fallback: Rewrite and convert to Lucene query
        QueryBuilder rewritten = queryBuilder.rewrite(queryShardContext);
        Query luceneQuery = rewritten.toQuery(queryShardContext);
        return buildNode(luceneQuery, rewritten);
    }

    private QueryPlanNode buildNode(Query query, QueryBuilder queryBuilder) throws Exception {
        if (query instanceof BooleanQuery) {
            return buildBooleanNode((BooleanQuery) query, queryBuilder);
        } else if (query instanceof TermQuery) {
            return buildTermNode((TermQuery) query, queryBuilder);
        } else if (query instanceof PointRangeQuery || query instanceof TermRangeQuery) {
            return buildRangeNode(query, queryBuilder);
        } else if (query instanceof MatchAllDocsQuery) {
            return buildMatchAllNode(query);
        } else {
            // For other query types, create a generic node
            return buildGenericNode(query, queryBuilder);
        }
    }

    private QueryPlanNode buildBooleanNode(BooleanQuery boolQuery, QueryBuilder queryBuilder) throws Exception {
        // If we have the original BoolQueryBuilder, use it to preserve context
        if (queryBuilder instanceof BoolQueryBuilder) {
            return buildFromBoolQueryBuilder((BoolQueryBuilder) queryBuilder);
        }

        // Otherwise fall back to processing Lucene query
        List<QueryPlanNode> mustClauses = new ArrayList<>();
        List<QueryPlanNode> filterClauses = new ArrayList<>();
        List<QueryPlanNode> shouldClauses = new ArrayList<>();
        List<QueryPlanNode> mustNotClauses = new ArrayList<>();

        // Process each clause
        for (BooleanClause clause : boolQuery.clauses()) {
            Query clauseQuery = clause.query();
            QueryPlanNode clauseNode = buildNode(clauseQuery, null);

            switch (clause.occur()) {
                case MUST:
                    mustClauses.add(clauseNode);
                    break;
                case FILTER:
                    filterClauses.add(clauseNode);
                    break;
                case SHOULD:
                    shouldClauses.add(clauseNode);
                    break;
                case MUST_NOT:
                    mustNotClauses.add(clauseNode);
                    break;
            }
        }

        int minimumShouldMatch = boolQuery.getMinimumNumberShouldMatch();

        return new BooleanPlanNode(boolQuery, mustClauses, filterClauses, shouldClauses, mustNotClauses, minimumShouldMatch);
    }

    private QueryPlanNode buildTermNode(TermQuery termQuery, QueryBuilder queryBuilder) {
        // If we have the original TermQueryBuilder, use it
        if (queryBuilder instanceof TermQueryBuilder) {
            try {
                return buildFromTermQueryBuilder((TermQueryBuilder) queryBuilder);
            } catch (Exception e) {
                // Fall back to Lucene query processing
            }
        }

        String field = termQuery.getTerm().field();
        String value = termQuery.getTerm().text();

        // Estimate document frequency
        long estimatedDocFreq = costEstimator.estimateTermDocFrequency(field, value);

        return new TermPlanNode(termQuery, field, value, estimatedDocFreq);
    }

    private QueryPlanNode buildMatchAllNode(Query query) {
        return new MatchAllPlanNode(query, costEstimator.getTotalDocs());
    }

    private QueryPlanNode buildRangeNode(Query query, QueryBuilder queryBuilder) {
        // If we have the original RangeQueryBuilder, use it
        if (queryBuilder instanceof RangeQueryBuilder) {
            try {
                return buildFromRangeQueryBuilder((RangeQueryBuilder) queryBuilder);
            } catch (Exception e) {
                // Fall back to Lucene query processing
            }
        }

        // Try to extract field information from the query
        String field = null;
        Object from = null;
        Object to = null;
        boolean includeFrom = true;
        boolean includeTo = true;
        
        if (query instanceof PointRangeQuery) {
            PointRangeQuery prq = (PointRangeQuery) query;
            field = prq.getField();
            // Point range queries use byte arrays - would need field type info for proper conversion
        } else if (query instanceof TermRangeQuery) {
            TermRangeQuery trq = (TermRangeQuery) query;
            field = trq.getField();
            from = trq.getLowerTerm() != null ? trq.getLowerTerm().utf8ToString() : null;
            to = trq.getUpperTerm() != null ? trq.getUpperTerm().utf8ToString() : null;
            includeFrom = trq.includesLower();
            includeTo = trq.includesUpper();
        }
        
        long estimatedDocs = costEstimator.estimateGenericCost(query);
        return new RangePlanNode(query, field, from, to, includeFrom, includeTo, estimatedDocs);
    }

    private QueryPlanNode buildGenericNode(Query query, QueryBuilder queryBuilder) {
        // For queries we don't have specific nodes for yet
        return new GenericPlanNode(query, determineNodeType(query), costEstimator.estimateGenericCost(query));
    }

    private QueryNodeType determineNodeType(Query query) {
        String className = query.getClass().getSimpleName();

        // Map query class names to node types
        for (QueryNodeType type : QueryNodeType.values()) {
            if (type == QueryNodeType.OTHER) continue;

            String typeName = type.name();
            if (type == QueryNodeType.VECTOR && className.contains("KNN")) {
                return type;
            }

            // Convert enum name to class name pattern (e.g., FUNCTION_SCORE -> FunctionScore)
            String pattern = typeName.replace("_", "");
            if (className.toUpperCase(Locale.ROOT).contains(pattern)) {
                return type;
            }
        }

        return QueryNodeType.OTHER;
    }

    /**
     * Builds a query plan node directly from a QueryBuilder without converting to Lucene query.
     * This preserves field names, values, and other metadata.
     */
    private QueryPlanNode buildFromQueryBuilder(QueryBuilder queryBuilder) throws Exception {
        if (queryBuilder instanceof BoolQueryBuilder) {
            return buildFromBoolQueryBuilder((BoolQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof TermQueryBuilder) {
            return buildFromTermQueryBuilder((TermQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof RangeQueryBuilder) {
            return buildFromRangeQueryBuilder((RangeQueryBuilder) queryBuilder);
        } else if (queryBuilder instanceof MatchQueryBuilder) {
            return buildFromMatchQueryBuilder((MatchQueryBuilder) queryBuilder);
        }

        // Fallback to Lucene conversion
        QueryBuilder rewritten = queryBuilder.rewrite(queryShardContext);
        Query luceneQuery = rewritten.toQuery(queryShardContext);
        return buildNode(luceneQuery, rewritten);
    }

    private QueryPlanNode buildFromBoolQueryBuilder(BoolQueryBuilder boolQuery) throws Exception {
        List<QueryPlanNode> mustClauses = new ArrayList<>();
        List<QueryPlanNode> filterClauses = new ArrayList<>();
        List<QueryPlanNode> shouldClauses = new ArrayList<>();
        List<QueryPlanNode> mustNotClauses = new ArrayList<>();

        // Build child nodes for each clause type
        for (QueryBuilder must : boolQuery.must()) {
            mustClauses.add(buildFromQueryBuilder(must));
        }
        for (QueryBuilder filter : boolQuery.filter()) {
            filterClauses.add(buildFromQueryBuilder(filter));
        }
        for (QueryBuilder should : boolQuery.should()) {
            shouldClauses.add(buildFromQueryBuilder(should));
        }
        for (QueryBuilder mustNot : boolQuery.mustNot()) {
            mustNotClauses.add(buildFromQueryBuilder(mustNot));
        }

        // Get minimum should match
        String minimumShouldMatch = boolQuery.minimumShouldMatch();
        int minShouldMatch = 0;
        if (minimumShouldMatch != null) {
            try {
                minShouldMatch = Integer.parseInt(minimumShouldMatch);
            } catch (NumberFormatException e) {
                // Handle percentage or other formats - for now default to 0
            }
        }

        Query luceneQuery = boolQuery.rewrite(queryShardContext).toQuery(queryShardContext);

        return new BooleanPlanNode((BooleanQuery) luceneQuery, mustClauses, filterClauses, shouldClauses, mustNotClauses, minShouldMatch);
    }

    private QueryPlanNode buildFromTermQueryBuilder(TermQueryBuilder termQuery) throws Exception {
        String field = termQuery.fieldName();
        Object value = termQuery.value();

        // Estimate document frequency
        long estimatedDocFreq = costEstimator.estimateTermDocFrequency(field, value.toString());

        // Create Lucene query
        Query luceneQuery = termQuery.rewrite(queryShardContext).toQuery(queryShardContext);

        return new TermPlanNode(luceneQuery, field, value, estimatedDocFreq);
    }

    private QueryPlanNode buildFromRangeQueryBuilder(RangeQueryBuilder rangeQuery) throws Exception {
        String field = rangeQuery.fieldName();
        Object from = rangeQuery.from();
        Object to = rangeQuery.to();
        boolean includeFrom = rangeQuery.includeLower();
        boolean includeTo = rangeQuery.includeUpper();

        Query luceneQuery = rangeQuery.rewrite(queryShardContext).toQuery(queryShardContext);
        
        long estimatedDocs;
        try {
            estimatedDocs = costEstimator.estimateGenericCost(luceneQuery);
        } catch (Exception e) {
            estimatedDocs = costEstimator.getTotalDocs() / 10;
        }

        return new RangePlanNode(luceneQuery, field, from, to, includeFrom, includeTo, estimatedDocs);
    }

    private QueryPlanNode buildFromMatchQueryBuilder(MatchQueryBuilder matchQuery) throws Exception {
        String field = matchQuery.fieldName();
        String text = matchQuery.value().toString();
        String analyzer = matchQuery.analyzer();

        int termCount = Math.max(1, Math.min(10, text.split("\\s+").length));
        
        Query luceneQuery = matchQuery.rewrite(queryShardContext).toQuery(queryShardContext);
        
        long estimatedDocs;
        try {
            estimatedDocs = costEstimator.estimateGenericCost(luceneQuery);
        } catch (Exception e) {
            estimatedDocs = costEstimator.getTotalDocs() / 20;
        }

        return new MatchPlanNode(luceneQuery, field, text, analyzer, termCount, estimatedDocs);
    }
}
