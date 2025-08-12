/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

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
 * @opensearch.internal
 */
public class BooleanFlatteningRewriter implements QueryRewriter {

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (!(query instanceof BoolQueryBuilder)) {
            return query;
        }

        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        return flattenBoolQuery(boolQuery);
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

    private void flattenClauses(List<QueryBuilder> clauses, BoolQueryBuilder target, ClauseType type) {
        for (QueryBuilder clause : clauses) {
            if (clause instanceof BoolQueryBuilder && canFlatten((BoolQueryBuilder) clause, type)) {
                BoolQueryBuilder innerBool = (BoolQueryBuilder) clause;
                if (type == ClauseType.FILTER) {
                    for (QueryBuilder innerClause : innerBool.filter()) {
                        target.filter(processClause(innerClause));
                    }
                    if (!innerBool.must().isEmpty() || !innerBool.should().isEmpty() || !innerBool.mustNot().isEmpty()) {
                        BoolQueryBuilder preservedBool = new BoolQueryBuilder();
                        preservedBool.boost(innerBool.boost());
                        flattenClauses(innerBool.must(), preservedBool, ClauseType.MUST);
                        flattenClauses(innerBool.should(), preservedBool, ClauseType.SHOULD);
                        flattenClauses(innerBool.mustNot(), preservedBool, ClauseType.MUST_NOT);
                        if (preservedBool.hasClauses()) {
                            target.filter(preservedBool);
                        }
                    }
                } else {
                    addClausesBasedOnType(innerBool, target, type);
                }
            } else {
                addClauseToTarget(target, processClause(clause), type);
            }
        }
    }

    private QueryBuilder processClause(QueryBuilder clause) {
        if (clause instanceof BoolQueryBuilder) {
            return flattenBoolQuery((BoolQueryBuilder) clause);
        }
        return clause;
    }

    private boolean canFlatten(BoolQueryBuilder boolQuery, ClauseType parentType) {
        if (boolQuery.boost() != 1.0f || boolQuery.minimumShouldMatch() != null) {
            return false;
        }

        if (parentType == ClauseType.FILTER) {
            return !boolQuery.filter().isEmpty();
        }
        int clauseTypeCount = 0;
        if (!boolQuery.must().isEmpty()) clauseTypeCount++;
        if (!boolQuery.filter().isEmpty()) clauseTypeCount++;
        if (!boolQuery.should().isEmpty()) clauseTypeCount++;
        if (!boolQuery.mustNot().isEmpty()) clauseTypeCount++;

        return clauseTypeCount == 1;
    }

    private void addClausesBasedOnType(BoolQueryBuilder source, BoolQueryBuilder target, ClauseType type) {
        List<QueryBuilder> relevantClauses = getClausesForType(source, type);

        boolean hasOnlyThisClauseType = (type == ClauseType.MUST
            && !source.must().isEmpty()
            && source.filter().isEmpty()
            && source.should().isEmpty()
            && source.mustNot().isEmpty())
            || (type == ClauseType.SHOULD
                && source.must().isEmpty()
                && source.filter().isEmpty()
                && !source.should().isEmpty()
                && source.mustNot().isEmpty())
            || (type == ClauseType.MUST_NOT
                && source.must().isEmpty()
                && source.filter().isEmpty()
                && source.should().isEmpty()
                && !source.mustNot().isEmpty());

        if (hasOnlyThisClauseType && !relevantClauses.isEmpty()) {
            for (QueryBuilder clause : relevantClauses) {
                addClauseToTarget(target, processClause(clause), type);
            }
        } else {
            addClauseToTarget(target, flattenBoolQuery(source), type);
        }
    }

    private List<QueryBuilder> getClausesForType(BoolQueryBuilder source, ClauseType type) {
        switch (type) {
            case MUST:
                return source.must();
            case FILTER:
                return source.filter();
            case SHOULD:
                return source.should();
            case MUST_NOT:
                return source.mustNot();
            default:
                return new ArrayList<>();
        }
    }

    private void addClauseToTarget(BoolQueryBuilder target, QueryBuilder clause, ClauseType type) {
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
