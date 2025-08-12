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

import java.util.ArrayList;
import java.util.List;

/**
 * Removes unnecessary match_all queries from boolean contexts where they have no effect.
 * For example:
 * <pre>
 * {"bool": {"must": [
 *   {"match_all": {}},
 *   {"term": {"field": "value"}}
 * ]}}
 * </pre>
 * becomes:
 * <pre>
 * {"bool": {"must": [
 *   {"term": {"field": "value"}}
 * ]}}
 * </pre>
 *
 * @opensearch.internal
 */
public class MatchAllRemovalRewriter implements QueryRewriter {

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (query instanceof BoolQueryBuilder) {
            return rewriteBoolQuery((BoolQueryBuilder) query);
        } else if (query instanceof MatchAllQueryBuilder) {
            // Top-level match_all is valid, don't remove
            return query;
        }
        return query;
    }

    private BoolQueryBuilder rewriteBoolQuery(BoolQueryBuilder original) {
        BoolQueryBuilder rewritten = new BoolQueryBuilder();

        rewritten.boost(original.boost());
        rewritten.queryName(original.queryName());
        rewritten.minimumShouldMatch(original.minimumShouldMatch());
        rewritten.adjustPureNegative(original.adjustPureNegative());
        processClausesWithMatchAllRemoval(original.must(), rewritten::must, true);
        processClausesWithMatchAllRemoval(original.filter(), rewritten::filter, true);
        processClausesWithMatchAllRemoval(original.should(), rewritten::should, false);
        processClausesWithMatchAllRemoval(original.mustNot(), rewritten::mustNot, false);

        if (!rewritten.hasClauses() && hadMatchAllClauses(original)) {
            return new BoolQueryBuilder().must(new MatchAllQueryBuilder());
        }

        return rewritten;
    }

    private void processClausesWithMatchAllRemoval(List<QueryBuilder> clauses, ClauseAdder adder, boolean removeMatchAll) {
        List<QueryBuilder> processedClauses = new ArrayList<>();
        boolean hasNonMatchAll = false;

        for (QueryBuilder clause : clauses) {
            if (clause instanceof MatchAllQueryBuilder && removeMatchAll) {
                continue;
            } else if (clause instanceof BoolQueryBuilder) {
                QueryBuilder rewritten = rewriteBoolQuery((BoolQueryBuilder) clause);
                processedClauses.add(rewritten);
                hasNonMatchAll = true;
            } else {
                processedClauses.add(clause);
                if (!(clause instanceof MatchAllQueryBuilder)) {
                    hasNonMatchAll = true;
                }
            }
        }

        // Add all processed clauses
        for (QueryBuilder clause : processedClauses) {
            // In must/filter context, only add match_all if there are no other clauses
            if (clause instanceof MatchAllQueryBuilder && removeMatchAll && hasNonMatchAll) {
                continue;
            }
            adder.addClause(clause);
        }
    }

    private boolean hadMatchAllClauses(BoolQueryBuilder query) {
        return hasMatchAll(query.must()) || hasMatchAll(query.filter()) || hasMatchAll(query.should()) || hasMatchAll(query.mustNot());
    }

    private boolean hasMatchAll(List<QueryBuilder> clauses) {
        for (QueryBuilder clause : clauses) {
            if (clause instanceof MatchAllQueryBuilder) {
                return true;
            }
        }
        return false;
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
