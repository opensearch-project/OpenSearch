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
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrites multiple term queries on the same field into a single terms query.
 * For example:
 * <pre>
 * {"bool": {"filter": [
 *   {"term": {"status": "active"}},
 *   {"term": {"status": "pending"}},
 *   {"term": {"category": "A"}}
 * ]}}
 * </pre>
 * becomes:
 * <pre>
 * {"bool": {"filter": [
 *   {"terms": {"status": ["active", "pending"]}},
 *   {"term": {"category": "A"}}
 * ]}}
 * </pre>
 *
 * @opensearch.internal
 */
public class TermsMergingRewriter implements QueryRewriter {

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (!(query instanceof BoolQueryBuilder)) {
            return query;
        }

        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        BoolQueryBuilder rewritten = new BoolQueryBuilder();

        rewritten.boost(boolQuery.boost());
        rewritten.queryName(boolQuery.queryName());
        rewritten.minimumShouldMatch(boolQuery.minimumShouldMatch());
        rewritten.adjustPureNegative(boolQuery.adjustPureNegative());
        // Only merge terms in contexts where it's semantically safe
        rewriteClausesNoMerge(boolQuery.must(), rewritten::must);  // Don't merge in must
        rewriteClauses(boolQuery.filter(), rewritten::filter);      // Safe to merge
        rewriteClauses(boolQuery.should(), rewritten::should);      // Safe to merge
        rewriteClausesNoMerge(boolQuery.mustNot(), rewritten::mustNot); // Don't merge in mustNot

        return rewritten;
    }

    private void rewriteClauses(List<QueryBuilder> clauses, ClauseAdder adder) {
        Map<String, List<TermQueryBuilder>> termsByField = new HashMap<>();
        List<QueryBuilder> nonTermClauses = new ArrayList<>();

        for (QueryBuilder clause : clauses) {
            if (clause instanceof TermQueryBuilder) {
                TermQueryBuilder termQuery = (TermQueryBuilder) clause;
                termsByField.computeIfAbsent(termQuery.fieldName(), k -> new ArrayList<>()).add(termQuery);
            } else if (clause instanceof BoolQueryBuilder) {
                nonTermClauses.add(rewrite(clause, null));
            } else {
                nonTermClauses.add(clause);
            }
        }
        for (Map.Entry<String, List<TermQueryBuilder>> entry : termsByField.entrySet()) {
            List<TermQueryBuilder> terms = entry.getValue();
            if (terms.size() > 1) {
                TermsQueryBuilder termsQuery = createTermsQuery(entry.getKey(), terms);
                adder.addClause(termsQuery);
            } else {
                adder.addClause(terms.get(0));
            }
        }
        for (QueryBuilder clause : nonTermClauses) {
            adder.addClause(clause);
        }
    }

    private TermsQueryBuilder createTermsQuery(String field, List<TermQueryBuilder> termQueries) {
        List<Object> values = new ArrayList<>();
        float boost = 1.0f;
        String queryName = null;

        for (TermQueryBuilder termQuery : termQueries) {
            values.add(termQuery.value());
            if (termQuery.boost() != 1.0f) {
                if (boost == 1.0f) {
                    boost = termQuery.boost();
                } else if (boost != termQuery.boost()) {
                    boost = 1.0f;
                }
            }
            if (queryName == null && termQuery.queryName() != null) {
                queryName = termQuery.queryName();
            }
        }

        TermsQueryBuilder termsQuery = new TermsQueryBuilder(field, values);
        termsQuery.boost(boost);
        if (queryName != null) {
            termsQuery.queryName(queryName);
        }

        return termsQuery;
    }

    @Override
    public int priority() {
        return 200;
    }

    @Override
    public String name() {
        return "terms_merging";
    }

    private void rewriteClausesNoMerge(List<QueryBuilder> clauses, ClauseAdder adder) {
        for (QueryBuilder clause : clauses) {
            if (clause instanceof BoolQueryBuilder) {
                adder.addClause(rewrite(clause, null));
            } else {
                adder.addClause(clause);
            }
        }
    }

    @FunctionalInterface
    private interface ClauseAdder {
        void addClause(QueryBuilder clause);
    }
}
