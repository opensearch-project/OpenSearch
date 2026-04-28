/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.SearchService;
import org.opensearch.search.query.QueryRewriter;

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
 * Note: Terms are only merged when there are enough terms to benefit from
 * the terms query's bit set optimization (default threshold: 16 terms).
 * This avoids performance regressions for small numbers of terms where
 * individual term queries may perform better.
 *
 * @opensearch.internal
 */
public class TermsMergingRewriter implements QueryRewriter {

    public static final TermsMergingRewriter INSTANCE = new TermsMergingRewriter();

    /**
     * Default minimum number of terms to merge. Below this threshold, individual
     * term queries may perform better than a terms query.
     * Based on Lucene's TermInSetQuery optimization characteristics.
     */
    private static final int DEFAULT_MINIMUM_TERMS_TO_MERGE = 16;

    /**
     * The minimum number of terms to merge.
     */
    private volatile int minimumTermsToMerge = DEFAULT_MINIMUM_TERMS_TO_MERGE;

    /**
     * Creates a new rewriter.
     */
    private TermsMergingRewriter() {
        // Singleton
    }

    /**
     * Initialize this rewriter with cluster settings.
     * This registers an update consumer to keep the threshold in sync with the cluster setting.
     *
     * @param settings Initial settings
     * @param clusterSettings Cluster settings to register update consumer
     */
    public void initialize(Settings settings, ClusterSettings clusterSettings) {
        this.minimumTermsToMerge = SearchService.QUERY_REWRITING_TERMS_THRESHOLD_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            SearchService.QUERY_REWRITING_TERMS_THRESHOLD_SETTING,
            threshold -> this.minimumTermsToMerge = threshold
        );
    }

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (!(query instanceof BoolQueryBuilder boolQuery)) {
            return query;
        }

        // First check if merging is needed
        if (!needsMerging(boolQuery)) {
            return query;
        }

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

    private boolean needsMerging(BoolQueryBuilder boolQuery) {
        // Check filter and should clauses for mergeable terms
        if (hasMergeableTerms(boolQuery.filter()) || hasMergeableTerms(boolQuery.should())) {
            return true;
        }

        // Check nested bool queries
        return hasNestedBoolThatNeedsMerging(boolQuery);
    }

    private boolean hasMergeableTerms(List<QueryBuilder> clauses) {
        Map<String, List<Float>> fieldBoosts = new HashMap<>();

        for (QueryBuilder clause : clauses) {
            if (clause instanceof TermQueryBuilder termQuery) {
                String field = termQuery.fieldName();
                float boost = termQuery.boost();

                fieldBoosts.computeIfAbsent(field, k -> new ArrayList<>()).add(boost);

                List<Float> boosts = fieldBoosts.get(field);
                if (boosts.size() >= minimumTermsToMerge) {
                    // Check if all boosts are the same
                    float firstBoost = boosts.get(0);
                    boolean sameBoost = boosts.stream().allMatch(b -> b == firstBoost);
                    if (sameBoost) {
                        return true;
                    }
                }
            } else if (clause instanceof TermsQueryBuilder termsQuery) {
                // Check if there are enough term queries that can be merged with this terms query
                String field = termsQuery.fieldName();
                int additionalTerms = 0;

                for (QueryBuilder other : clauses) {
                    if (other != clause && other instanceof TermQueryBuilder termQuery) {
                        if (field.equals(termQuery.fieldName()) && termsQuery.boost() == termQuery.boost()) {
                            additionalTerms++;
                        }
                    }
                }

                // Only worth merging if the combined size would meet the threshold
                if (termsQuery.values().size() + additionalTerms >= minimumTermsToMerge) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean hasNestedBoolThatNeedsMerging(BoolQueryBuilder boolQuery) {
        for (QueryBuilder clause : boolQuery.must()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsMerging(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder clause : boolQuery.filter()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsMerging(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder clause : boolQuery.should()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsMerging(boolQueryBuilder)) {
                return true;
            }
        }
        for (QueryBuilder clause : boolQuery.mustNot()) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder && needsMerging(boolQueryBuilder)) {
                return true;
            }
        }
        return false;
    }

    private void rewriteClauses(List<QueryBuilder> clauses, ClauseAdder adder) {
        Map<String, TermsInfo> termsMap = new HashMap<>();
        List<QueryBuilder> nonTermClauses = new ArrayList<>();

        // Group term queries by field
        for (QueryBuilder clause : clauses) {
            if (clause instanceof TermQueryBuilder termQuery) {
                String field = termQuery.fieldName();
                float boost = termQuery.boost();

                TermsInfo info = termsMap.get(field);
                if (info != null && info.boost != boost) {
                    // Different boost, can't merge - add as single term
                    nonTermClauses.add(clause);
                } else {
                    termsMap.computeIfAbsent(field, k -> new TermsInfo(boost)).addValue(termQuery.value());
                }
            } else if (clause instanceof TermsQueryBuilder termsQuery) {
                // Existing terms query - add to it
                String field = termsQuery.fieldName();
                float boost = termsQuery.boost();

                TermsInfo info = termsMap.get(field);
                if (info != null && info.boost != boost) {
                    // Different boost, can't merge
                    nonTermClauses.add(clause);
                } else {
                    info = termsMap.computeIfAbsent(field, k -> new TermsInfo(boost));
                    for (Object value : termsQuery.values()) {
                        info.addValue(value);
                    }
                }
            } else if (clause instanceof BoolQueryBuilder boolQueryBuilder) {
                // Recursively rewrite nested bool queries
                nonTermClauses.add(rewrite(boolQueryBuilder, null));
            } else {
                nonTermClauses.add(clause);
            }
        }

        // Create terms queries for fields with multiple values
        for (Map.Entry<String, TermsInfo> entry : termsMap.entrySet()) {
            String field = entry.getKey();
            TermsInfo info = entry.getValue();

            if (info.values.size() == 1) {
                // Single value, keep as term query
                TermQueryBuilder termQuery = new TermQueryBuilder(field, info.values.get(0));
                if (info.boost != 1.0f) {
                    termQuery.boost(info.boost);
                }
                adder.addClause(termQuery);
            } else if (info.values.size() >= minimumTermsToMerge) {
                // Many values, merge into terms query for better performance
                TermsQueryBuilder termsQuery = new TermsQueryBuilder(field, info.values);
                if (info.boost != 1.0f) {
                    termsQuery.boost(info.boost);
                }
                adder.addClause(termsQuery);
            } else {
                // Few values, keep as individual term queries for better performance
                for (Object value : info.values) {
                    TermQueryBuilder termQuery = new TermQueryBuilder(field, value);
                    if (info.boost != 1.0f) {
                        termQuery.boost(info.boost);
                    }
                    adder.addClause(termQuery);
                }
            }
        }

        // Add non-term clauses
        for (QueryBuilder clause : nonTermClauses) {
            adder.addClause(clause);
        }
    }

    private void rewriteClausesNoMerge(List<QueryBuilder> clauses, ClauseAdder adder) {
        for (QueryBuilder clause : clauses) {
            if (clause instanceof BoolQueryBuilder boolQueryBuilder) {
                // Recursively rewrite nested bool queries
                adder.addClause(rewrite(boolQueryBuilder, null));
            } else {
                adder.addClause(clause);
            }
        }
    }

    @Override
    public int priority() {
        return 200;
    }

    @Override
    public String name() {
        return "terms_merging";
    }

    @FunctionalInterface
    private interface ClauseAdder {
        void addClause(QueryBuilder clause);
    }

    private static class TermsInfo {
        final float boost;
        final List<Object> values = new ArrayList<>();

        TermsInfo(float boost) {
            this.boost = boost;
        }

        void addValue(Object value) {
            values.add(value);
        }
    }
}
