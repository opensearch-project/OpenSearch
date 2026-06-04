/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.GeoBoundingBoxQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.search.query.QueryRewriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites must clauses to filter clauses when they don't affect scoring.
 * This improves performance by avoiding unnecessary scoring calculations.
 *
 * For example:
 * <pre>
 * {"bool": {"must": [
 *   {"range": {"date": {"gte": "2024-01-01"}}},
 *   {"term": {"status": "active"}}
 * ]}}
 * </pre>
 * becomes:
 * <pre>
 * {"bool": {
 *   "filter": [{"range": {"date": {"gte": "2024-01-01"}}}],
 *   "must": [{"term": {"status": "active"}}]
 * }}
 * </pre>
 *
 * @opensearch.internal
 */
public class MustToFilterRewriter implements QueryRewriter {

    public static final MustToFilterRewriter INSTANCE = new MustToFilterRewriter();

    private MustToFilterRewriter() {
        // Singleton
    }

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (!(query instanceof BoolQueryBuilder boolQuery)) {
            return query;
        }

        // If there are no must clauses, nothing to rewrite
        if (boolQuery.must().isEmpty()) {
            return query;
        }

        // First, rewrite all clauses recursively
        List<QueryBuilder> rewrittenMustClauses = new ArrayList<>();
        List<QueryBuilder> mustClausesToMove = new ArrayList<>();

        for (QueryBuilder clause : boolQuery.must()) {
            QueryBuilder rewrittenClause = rewriteIfNeeded(clause, context);
            rewrittenMustClauses.add(rewrittenClause);

            if (isClauseIrrelevantToScoring(rewrittenClause, context)) {
                mustClausesToMove.add(rewrittenClause);
            }
        }

        // Check if anything changed - either clauses to move or nested rewrites
        boolean hasChanges = !mustClausesToMove.isEmpty();
        for (int i = 0; i < boolQuery.must().size(); i++) {
            if (boolQuery.must().get(i) != rewrittenMustClauses.get(i)) {
                hasChanges = true;
                break;
            }
        }

        if (!hasChanges) {
            return query;
        }

        // Create a new BoolQueryBuilder with moved clauses
        BoolQueryBuilder rewritten = new BoolQueryBuilder();

        // Copy all properties
        rewritten.boost(boolQuery.boost());
        rewritten.queryName(boolQuery.queryName());
        rewritten.minimumShouldMatch(boolQuery.minimumShouldMatch());
        rewritten.adjustPureNegative(boolQuery.adjustPureNegative());

        // Copy must clauses except the ones we're moving
        for (QueryBuilder rewrittenClause : rewrittenMustClauses) {
            if (!mustClausesToMove.contains(rewrittenClause)) {
                rewritten.must(rewrittenClause);
            }
        }

        // Add the moved clauses to filter
        for (QueryBuilder movedClause : mustClausesToMove) {
            rewritten.filter(movedClause);
        }

        // Copy existing filter clauses
        for (QueryBuilder filterClause : boolQuery.filter()) {
            rewritten.filter(rewriteIfNeeded(filterClause, context));
        }

        // Copy should and mustNot clauses
        for (QueryBuilder shouldClause : boolQuery.should()) {
            rewritten.should(rewriteIfNeeded(shouldClause, context));
        }
        for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
            rewritten.mustNot(rewriteIfNeeded(mustNotClause, context));
        }

        return rewritten;
    }

    private QueryBuilder rewriteIfNeeded(QueryBuilder query, QueryShardContext context) {
        // Recursively rewrite nested boolean queries
        if (query instanceof BoolQueryBuilder boolQueryBuilder) {
            return rewrite(boolQueryBuilder, context);
        }
        return query;
    }

    private boolean isClauseIrrelevantToScoring(QueryBuilder clause, QueryShardContext context) {
        // This is an incomplete list of clauses this might apply for; it can be expanded in future.

        // If a clause is purely numeric, for example a date range, its score is unimportant as
        // it'll be the same for all returned docs
        if (clause instanceof RangeQueryBuilder) return true;
        if (clause instanceof GeoBoundingBoxQueryBuilder) return true;

        // Further optimizations depend on knowing whether the field is numeric.
        // Skip moving these clauses if we don't have the shard context.
        if (context == null) return false;

        if (!(clause instanceof WithFieldName wfn)) return false;

        MappedFieldType fieldType = context.fieldMapper(wfn.fieldName());

        if (!(fieldType instanceof NumberFieldMapper.NumberFieldType)) return false;

        // Numeric field queries have constant scores
        if (clause instanceof MatchQueryBuilder) return true;
        if (clause instanceof TermQueryBuilder) return true;
        if (clause instanceof TermsQueryBuilder) return true;

        return false;
    }

    @Override
    public int priority() {
        // Run after boolean flattening (100) but before terms merging (200)
        return 150;
    }

    @Override
    public String name() {
        return "must_to_filter";
    }
}
