/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.query.QueryRewriter;

import java.util.List;

/**
 * Removes unnecessary match_all queries from boolean contexts where they have no effect.
 *
 * @opensearch.internal
 */
public class MatchAllRemovalRewriter implements QueryRewriter {

    public static final MatchAllRemovalRewriter INSTANCE = new MatchAllRemovalRewriter();

    private MatchAllRemovalRewriter() {
        // Singleton
    }

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (query instanceof BoolQueryBuilder boolQueryBuilder) {
            return rewriteBoolQuery(boolQueryBuilder);
        }
        return query;
    }

    private QueryBuilder rewriteBoolQuery(BoolQueryBuilder original) {
        // Special case: bool query with only match_all queries and no should/mustNot
        if (original.should().isEmpty() && original.mustNot().isEmpty()) {
            boolean onlyMatchAll = true;
            int matchAllCount = 0;
            int matchAllInMust = 0;

            for (QueryBuilder q : original.must()) {
                if (q instanceof MatchAllQueryBuilder) {
                    matchAllCount++;
                    matchAllInMust++;
                } else {
                    // Don't treat constant score queries or any other queries as match_all
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

            // Only convert to single match_all if there are no must clauses
            // (to preserve scoring) or if there's only one match_all total
            if (onlyMatchAll && matchAllCount > 0 && (matchAllInMust == 0 || matchAllCount == 1)) {
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

        // Clone the query structure
        BoolQueryBuilder rewritten = new BoolQueryBuilder();
        rewritten.boost(original.boost());
        rewritten.queryName(original.queryName());
        rewritten.minimumShouldMatch(original.minimumShouldMatch());
        rewritten.adjustPureNegative(original.adjustPureNegative());

        // Process each clause type with different match_all removal logic:
        // - must: Remove match_all only if other queries exist (preserves scoring semantics)
        // - filter: Always remove match_all (it's redundant in non-scoring context)
        // - should: Keep match_all (changes OR semantics if removed)
        // - mustNot: Keep match_all (excluding all docs is meaningful)
        processClausesWithContext(original.must(), rewritten::must, true, original, true);
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
            if (q instanceof BoolQueryBuilder boolQueryBuilder && shouldRewrite(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder q : bool.filter()) {
            if (q instanceof BoolQueryBuilder boolQueryBuilder && shouldRewrite(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder q : bool.should()) {
            if (q instanceof BoolQueryBuilder boolQueryBuilder && shouldRewrite(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder q : bool.mustNot()) {
            if (q instanceof BoolQueryBuilder boolQueryBuilder && shouldRewrite(boolQueryBuilder)) {
                return true;
            }
        }
        return false;
    }

    private void processClausesWithContext(
        List<QueryBuilder> clauses,
        ClauseAdder adder,
        boolean removeMatchAll,
        BoolQueryBuilder original,
        boolean isMustClause
    ) {
        if (!removeMatchAll) {
            processClauses(clauses, adder, false, original);
            return;
        }

        // For must clauses, only remove match_all if there are other non-match_all queries
        if (isMustClause) {
            boolean hasNonMatchAll = clauses.stream().anyMatch(q -> !(q instanceof MatchAllQueryBuilder));

            // Also check if we're in a scoring context (no filter/should/mustNot clauses)
            boolean isScoringContext = original.filter().isEmpty() && original.should().isEmpty() && original.mustNot().isEmpty();

            if (!hasNonMatchAll || isScoringContext) {
                // All queries are match_all or we're in a scoring context, don't remove any to preserve scoring
                processClauses(clauses, adder, false, original);
                return;
            }
        }

        // Otherwise, use normal processing
        processClauses(clauses, adder, removeMatchAll, original);
    }

    private void processClauses(List<QueryBuilder> clauses, ClauseAdder adder, boolean removeMatchAll, BoolQueryBuilder original) {
        if (!removeMatchAll) {
            // For should/mustNot, don't remove match_all
            for (QueryBuilder clause : clauses) {
                if (clause instanceof BoolQueryBuilder boolQueryBuilder) {
                    adder.addClause(rewriteBoolQuery(boolQueryBuilder));
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
            if (clause instanceof BoolQueryBuilder boolQueryBuilder) {
                adder.addClause(rewriteBoolQuery(boolQueryBuilder));
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
