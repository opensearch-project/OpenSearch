/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.util.List;

/**
 * Removes unnecessary match_all queries from boolean contexts where they have no effect.
 *
 * @opensearch.internal
 */
public class MatchAllRemovalRewriter implements QueryRewriter {

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (query instanceof BoolQueryBuilder) {
            return rewriteBoolQuery((BoolQueryBuilder) query);
        }
        return query;
    }

    private QueryBuilder rewriteBoolQuery(BoolQueryBuilder original) {
        // Special case: bool query with only match_all queries and no should/mustNot
        if (original.should().isEmpty() && original.mustNot().isEmpty()) {
            boolean onlyMatchAll = true;
            int matchAllCount = 0;

            for (QueryBuilder q : original.must()) {
                if (q instanceof MatchAllQueryBuilder) {
                    matchAllCount++;
                } else {
                    onlyMatchAll = false;
                    break;
                }
            }

            if (onlyMatchAll) {
                for (QueryBuilder q : original.filter()) {
                    if (q instanceof MatchAllQueryBuilder) {
                        matchAllCount++;
                    } else {
                        onlyMatchAll = false;
                        break;
                    }
                }
            }

            if (onlyMatchAll && matchAllCount > 0) {
                // Convert to single match_all, preserving boost
                MatchAllQueryBuilder matchAll = new MatchAllQueryBuilder();
                if (original.boost() != 1.0f) {
                    matchAll.boost(original.boost());
                }
                return matchAll;
            }
        }

        // Check if we need rewriting
        boolean needsRewrite = shouldRewrite(original);

        if (!needsRewrite) {
            return original;
        }

        // Create rewritten query
        BoolQueryBuilder rewritten = new BoolQueryBuilder();
        rewritten.boost(original.boost());
        rewritten.queryName(original.queryName());
        rewritten.minimumShouldMatch(original.minimumShouldMatch());
        rewritten.adjustPureNegative(original.adjustPureNegative());

        // Process all clauses
        processClauses(original.must(), rewritten::must, true, original);
        processClauses(original.filter(), rewritten::filter, true, original);
        processClauses(original.should(), rewritten::should, false, original);
        processClauses(original.mustNot(), rewritten::mustNot, false, original);

        return rewritten;
    }

    private boolean shouldRewrite(BoolQueryBuilder bool) {
        // Check if any must/filter has match_all
        if (hasMatchAll(bool.must()) || hasMatchAll(bool.filter())) {
            return true;
        }

        // Check nested bool queries
        return hasNestedBoolThatNeedsRewrite(bool);
    }

    private boolean hasMatchAll(List<QueryBuilder> clauses) {
        for (QueryBuilder q : clauses) {
            if (q instanceof MatchAllQueryBuilder) {
                return true;
            }
        }
        return false;
    }

    private boolean hasNestedBoolThatNeedsRewrite(BoolQueryBuilder bool) {
        for (QueryBuilder q : bool.must()) {
            if (q instanceof BoolQueryBuilder && shouldRewrite((BoolQueryBuilder) q)) {
                return true;
            }
        }
        for (QueryBuilder q : bool.filter()) {
            if (q instanceof BoolQueryBuilder && shouldRewrite((BoolQueryBuilder) q)) {
                return true;
            }
        }
        for (QueryBuilder q : bool.should()) {
            if (q instanceof BoolQueryBuilder && shouldRewrite((BoolQueryBuilder) q)) {
                return true;
            }
        }
        for (QueryBuilder q : bool.mustNot()) {
            if (q instanceof BoolQueryBuilder && shouldRewrite((BoolQueryBuilder) q)) {
                return true;
            }
        }
        return false;
    }

    private void processClauses(List<QueryBuilder> clauses, ClauseAdder adder, boolean removeMatchAll, BoolQueryBuilder original) {
        if (!removeMatchAll) {
            // For should/mustNot, don't remove match_all
            for (QueryBuilder clause : clauses) {
                if (clause instanceof BoolQueryBuilder) {
                    adder.addClause(rewriteBoolQuery((BoolQueryBuilder) clause));
                } else {
                    adder.addClause(clause);
                }
            }
            return;
        }

        // For must/filter, remove match_all if:
        // 1. There are other non-match_all clauses in the same list OR
        // 2. There are clauses in other lists (must, filter, should, mustNot)
        boolean hasOtherClauses = hasNonMatchAllInSameList(clauses) || hasClausesInOtherLists(original);

        for (QueryBuilder clause : clauses) {
            if (clause instanceof BoolQueryBuilder) {
                adder.addClause(rewriteBoolQuery((BoolQueryBuilder) clause));
            } else if (clause instanceof MatchAllQueryBuilder && hasOtherClauses) {
                // Skip match_all
                continue;
            } else {
                adder.addClause(clause);
            }
        }
    }

    private boolean hasNonMatchAllInSameList(List<QueryBuilder> clauses) {
        for (QueryBuilder q : clauses) {
            if (!(q instanceof MatchAllQueryBuilder)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasClausesInOtherLists(BoolQueryBuilder bool) {
        // Check if there are any clauses in any list
        return !bool.must().isEmpty() || !bool.filter().isEmpty() || !bool.should().isEmpty() || !bool.mustNot().isEmpty();
    }

    @Override
    public int priority() {
        return 300;
    }

    @Override
    public String name() {
        return "match_all_removal";
    }

    @FunctionalInterface
    private interface ClauseAdder {
        void addClause(QueryBuilder clause);
    }
}
