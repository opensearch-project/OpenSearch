/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.query.QueryRewriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites nested boolean queries to flatten unnecessary nesting levels.
 * For example:
 * <pre>
 * {"bool": {"filter": [{"bool": {"filter": [{"term": {"field": "value"}}]}}]}}
 * </pre>
 * becomes:
 * <pre>
 * {"bool": {"filter": [{"term": {"field": "value"}}]}}
 * </pre>
 *
 * Note: While Lucene's BooleanQuery does flatten pure disjunctions (SHOULD-only clauses)
 * for WAND optimization, it does NOT flatten other nested structures like filter-in-filter
 * or must-in-must. This rewriter handles those additional patterns that are common in
 * user-generated and template-based queries but not optimized by Lucene.
 *
 * @opensearch.internal
 */
public class BooleanFlatteningRewriter implements QueryRewriter {

    public static final BooleanFlatteningRewriter INSTANCE = new BooleanFlatteningRewriter();

    private BooleanFlatteningRewriter() {
        // Singleton
    }

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (!(query instanceof BoolQueryBuilder boolQuery)) {
            return query;
        }

        // First check if flattening is needed
        if (!needsFlattening(boolQuery)) {
            return query;
        }

        return flattenBoolQuery(boolQuery);
    }

    private boolean needsFlattening(BoolQueryBuilder boolQuery) {
        // Check all clause types for nested bool queries that can be flattened
        if (hasFlattenableBool(boolQuery.must(), ClauseType.MUST)
            || hasFlattenableBool(boolQuery.filter(), ClauseType.FILTER)
            || hasFlattenableBool(boolQuery.should(), ClauseType.SHOULD)
            || hasFlattenableBool(boolQuery.mustNot(), ClauseType.MUST_NOT)) {
            return true;
        }

        // Check if any nested bool queries need flattening
        return hasNestedBoolThatNeedsFlattening(boolQuery);
    }

    private boolean hasFlattenableBool(List<QueryBuilder> clauses, ClauseType parentType) {
        for (QueryBuilder clause : clauses) {
            if (clause instanceof BoolQueryBuilder nestedBool) {
                // Can flatten if nested bool only has one type of clause matching parent
                if (canFlatten(nestedBool, parentType)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasNestedBoolThatNeedsFlattening(BoolQueryBuilder boolQuery) {
        for (QueryBuilder clause : boolQuery.must()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsFlattening(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder clause : boolQuery.filter()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsFlattening(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder clause : boolQuery.should()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsFlattening(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder clause : boolQuery.mustNot()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsFlattening(boolQueryBuilder)) {
                return true;
            }
        }
        return false;
    }

    private BoolQueryBuilder flattenBoolQuery(BoolQueryBuilder original) {
        BoolQueryBuilder flattened = new BoolQueryBuilder();

        flattened.boost(original.boost());
        flattened.queryName(original.queryName());
        flattened.minimumShouldMatch(original.minimumShouldMatch());
        flattened.adjustPureNegative(original.adjustPureNegative());

        flattenClauses(original.must(), flattened, ClauseType.MUST);
        flattenClauses(original.filter(), flattened, ClauseType.FILTER);
        flattenClauses(original.should(), flattened, ClauseType.SHOULD);
        flattenClauses(original.mustNot(), flattened, ClauseType.MUST_NOT);

        return flattened;
    }

    private void flattenClauses(List<QueryBuilder> clauses, BoolQueryBuilder target, ClauseType clauseType) {
        for (QueryBuilder clause : clauses) {
            if (clause instanceof BoolQueryBuilder nestedBool) {
                if (canFlatten(nestedBool, clauseType)) {
                    // General flattening for same-clause-type nesting
                    List<QueryBuilder> nestedClauses = getClausesForType(nestedBool, clauseType);
                    for (QueryBuilder nestedClause : nestedClauses) {
                        if (nestedClause instanceof BoolQueryBuilder nestedBoolQueryBuilder) {
                            nestedClause = flattenBoolQuery(nestedBoolQueryBuilder);
                        }
                        addClauseBasedOnType(target, nestedClause, clauseType);
                    }
                } else {
                    // Can't flatten this bool, but recursively flatten its contents
                    BoolQueryBuilder flattenedNested = flattenBoolQuery(nestedBool);
                    addClauseBasedOnType(target, flattenedNested, clauseType);
                }
            } else {
                // Non-boolean clause, add as-is
                addClauseBasedOnType(target, clause, clauseType);
            }
        }
    }

    private boolean canFlatten(BoolQueryBuilder nestedBool, ClauseType parentType) {
        // Can only flatten if:
        // 1. The nested bool has the same properties as default (boost=1, no queryName, etc.)
        // 2. The nested bool only has clauses of the same type as the parent

        if (nestedBool.boost() != 1.0f || nestedBool.queryName() != null) {
            return false;
        }

        // Never flatten under MUST_NOT to preserve semantics and avoid non-idempotent transforms
        if (parentType == ClauseType.MUST_NOT) {
            return false;
        }

        // Check if only has clauses matching parent type
        switch (parentType) {
            case MUST:
                return !nestedBool.must().isEmpty()
                    && nestedBool.filter().isEmpty()
                    && nestedBool.should().isEmpty()
                    && nestedBool.mustNot().isEmpty();
            case FILTER:
                return nestedBool.must().isEmpty()
                    && !nestedBool.filter().isEmpty()
                    && nestedBool.should().isEmpty()
                    && nestedBool.mustNot().isEmpty();
            case SHOULD:
                return nestedBool.must().isEmpty()
                    && nestedBool.filter().isEmpty()
                    && !nestedBool.should().isEmpty()
                    && nestedBool.mustNot().isEmpty()
                    && nestedBool.minimumShouldMatch() == null;
            default:
                return false;
        }
    }

    private List<QueryBuilder> getClausesForType(BoolQueryBuilder bool, ClauseType type) {
        switch (type) {
            case MUST:
                return bool.must();
            case FILTER:
                return bool.filter();
            case SHOULD:
                return bool.should();
            case MUST_NOT:
                return bool.mustNot();
            default:
                return new ArrayList<>();
        }
    }

    private void addClauseBasedOnType(BoolQueryBuilder target, QueryBuilder clause, ClauseType type) {
        switch (type) {
            case MUST:
                target.must(clause);
                break;
            case FILTER:
                target.filter(clause);
                break;
            case SHOULD:
                target.should(clause);
                break;
            case MUST_NOT:
                target.mustNot(clause);
                break;
        }
    }

    @Override
    public int priority() {
        return 100;
    }

    @Override
    public String name() {
        return "boolean_flattening";
    }

    private enum ClauseType {
        MUST,
        FILTER,
        SHOULD,
        MUST_NOT
    }
}
