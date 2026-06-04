/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ComplementAwareQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.WithFieldName;
import org.opensearch.search.query.QueryRewriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrites must_not clauses to should clauses when possible.
 * This improves performance by transforming negative queries into positive ones.
 *
 * For example:
 * <pre>
 * {"bool": {"must_not": [{"range": {"age": {"gte": 18, "lte": 65}}}]}}
 * </pre>
 * becomes:
 * <pre>
 * {"bool": {"must": [{"bool": {"should": [
 *   {"range": {"age": {"lt": 18}}},
 *   {"range": {"age": {"gt": 65}}}
 * ], "minimum_should_match": 1}}]}}
 * </pre>
 *
 * This optimization applies to:
 * - RangeQueryBuilder
 * - TermQueryBuilder (on numeric fields)
 * - TermsQueryBuilder (on numeric fields)
 * - MatchQueryBuilder (on numeric fields)
 *
 * @opensearch.internal
 */
public class MustNotToShouldRewriter implements QueryRewriter {

    public static final MustNotToShouldRewriter INSTANCE = new MustNotToShouldRewriter();

    private MustNotToShouldRewriter() {
        // Singleton
    }

    @Override
    public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
        if (!(query instanceof BoolQueryBuilder boolQuery)) {
            return query;
        }

        // We need LeafReaderContexts to verify single-valued fields (only for must_not rewriting)
        List<LeafReaderContext> leafReaderContexts = null;
        List<QueryBuilder> mustNotClausesToRewrite = new ArrayList<>();

        // Only process must_not clauses if they exist
        if (!boolQuery.mustNot().isEmpty()) {
            leafReaderContexts = getLeafReaderContexts(context);
            if (leafReaderContexts != null && !leafReaderContexts.isEmpty()) {
                Map<String, Integer> fieldCounts = new HashMap<>();

                // Find complement-aware queries that can be rewritten
                for (QueryBuilder clause : boolQuery.mustNot()) {
                    if (clause instanceof ComplementAwareQueryBuilder && clause instanceof WithFieldName wfn) {
                        fieldCounts.merge(wfn.fieldName(), 1, Integer::sum);
                    }
                }

                // For now, only handle the case where there's exactly 1 complement-aware query per field
                for (QueryBuilder clause : boolQuery.mustNot()) {
                    if (clause instanceof ComplementAwareQueryBuilder && clause instanceof WithFieldName wfn) {
                        String fieldName = wfn.fieldName();

                        if (fieldCounts.getOrDefault(fieldName, 0) == 1) {
                            // Check that all docs on this field have exactly 1 value
                            if (checkAllDocsHaveOneValue(leafReaderContexts, fieldName)) {
                                mustNotClausesToRewrite.add(clause);
                            }
                        }
                    }
                }
            }
        }

        // Create a new BoolQueryBuilder with rewritten clauses
        BoolQueryBuilder rewritten = new BoolQueryBuilder();

        // Copy all properties
        rewritten.boost(boolQuery.boost());
        rewritten.queryName(boolQuery.queryName());
        rewritten.minimumShouldMatch(boolQuery.minimumShouldMatch());
        rewritten.adjustPureNegative(boolQuery.adjustPureNegative());

        // Copy must clauses (rewrite nested queries first)
        for (QueryBuilder mustClause : boolQuery.must()) {
            rewritten.must(rewrite(mustClause, context));
        }

        // Copy filter clauses (rewrite nested queries first)
        for (QueryBuilder filterClause : boolQuery.filter()) {
            rewritten.filter(rewrite(filterClause, context));
        }

        // Copy should clauses (rewrite nested queries first)
        for (QueryBuilder shouldClause : boolQuery.should()) {
            rewritten.should(rewrite(shouldClause, context));
        }

        // Process must_not clauses
        boolean changed = false;
        for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
            if (mustNotClausesToRewrite.contains(mustNotClause)) {
                // Rewrite this clause
                ComplementAwareQueryBuilder caq = (ComplementAwareQueryBuilder) mustNotClause;
                List<? extends QueryBuilder> complement = caq.getComplement(context);

                if (complement != null && !complement.isEmpty()) {
                    BoolQueryBuilder nestedBoolQuery = new BoolQueryBuilder();
                    nestedBoolQuery.minimumShouldMatch(1);
                    for (QueryBuilder complementComponent : complement) {
                        nestedBoolQuery.should(complementComponent);
                    }
                    rewritten.must(nestedBoolQuery);
                    changed = true;
                } else {
                    // If complement couldn't be determined, keep original
                    rewritten.mustNot(mustNotClause);
                }
            } else {
                // Keep clauses we're not rewriting
                rewritten.mustNot(rewrite(mustNotClause, context));
            }
        }

        // Handle minimumShouldMatch adjustment
        if (changed && boolQuery.minimumShouldMatch() == null) {
            if (!boolQuery.should().isEmpty() && boolQuery.must().isEmpty() && boolQuery.filter().isEmpty()) {
                // If there were originally should clauses and no must/filter clauses,
                // null minimumShouldMatch defaults to 1 in Lucene.
                // But if there was originally a must or filter clause, the default is 0.
                // If we added a must clause due to this rewrite, we should respect the original default.
                rewritten.minimumShouldMatch(1);
            }
        }

        // Check if any nested queries were rewritten
        boolean nestedQueriesChanged = false;
        for (QueryBuilder mustClause : boolQuery.must()) {
            if (mustClause instanceof BoolQueryBuilder && rewritten.must().contains(mustClause) == false) {
                nestedQueriesChanged = true;
                break;
            }
        }
        if (!nestedQueriesChanged) {
            for (QueryBuilder filterClause : boolQuery.filter()) {
                if (filterClause instanceof BoolQueryBuilder && rewritten.filter().contains(filterClause) == false) {
                    nestedQueriesChanged = true;
                    break;
                }
            }
        }
        if (!nestedQueriesChanged) {
            for (QueryBuilder shouldClause : boolQuery.should()) {
                if (shouldClause instanceof BoolQueryBuilder && rewritten.should().contains(shouldClause) == false) {
                    nestedQueriesChanged = true;
                    break;
                }
            }
        }
        if (!nestedQueriesChanged) {
            for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
                if (mustNotClause instanceof BoolQueryBuilder && rewritten.mustNot().contains(mustNotClause) == false) {
                    nestedQueriesChanged = true;
                    break;
                }
            }
        }

        return (changed || nestedQueriesChanged) ? rewritten : query;
    }

    private List<LeafReaderContext> getLeafReaderContexts(QueryShardContext context) {
        if (context == null) {
            return null;
        }
        try {
            return context.getIndexReader().leaves();
        } catch (Exception e) {
            return null;
        }
    }

    private boolean checkAllDocsHaveOneValue(List<LeafReaderContext> leafReaderContexts, String fieldName) {
        try {
            for (LeafReaderContext leafReaderContext : leafReaderContexts) {
                PointValues pointValues = leafReaderContext.reader().getPointValues(fieldName);
                if (pointValues != null) {
                    int docCount = pointValues.getDocCount();
                    long valueCount = pointValues.size();
                    // Check if all documents have exactly one value
                    if (docCount != valueCount) {
                        return false;
                    }
                    // Also check if all documents in the segment have a value for this field
                    // If some documents are missing the field, we can't do the optimization
                    // because the semantics change (missing values won't match positive queries)
                    int maxDoc = leafReaderContext.reader().maxDoc();
                    if (docCount != maxDoc) {
                        return false;
                    }
                } else {
                    // If there are no point values but there are documents, some docs are missing the field
                    if (leafReaderContext.reader().maxDoc() > 0) {
                        return false;
                    }
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public int priority() {
        // Run after boolean flattening (100) and must-to-filter (150)
        // but before terms merging (200)
        return 175;
    }

    @Override
    public String name() {
        return "must_not_to_should";
    }
}
