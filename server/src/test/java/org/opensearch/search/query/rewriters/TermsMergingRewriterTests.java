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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class TermsMergingRewriterTests extends OpenSearchTestCase {

    private final TermsMergingRewriter rewriter = TermsMergingRewriter.INSTANCE;
    private final QueryShardContext context = mock(QueryShardContext.class);

    public void testSimpleTermMergingBelowThreshold() {
        // Few term queries on same field should NOT be merged (below threshold)
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("status", "active"))
            .filter(QueryBuilders.termQuery("status", "pending"))
            .filter(QueryBuilders.termQuery("status", "approved"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes expected
    }

    public void testTermMergingAboveThreshold() {
        // Many term queries on same field should be merged (above threshold of 16)
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        // Add 20 term queries for the same field
        for (int i = 0; i < 20; i++) {
            query.filter(QueryBuilders.termQuery("category", "cat_" + i));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have one terms query instead of 20 term queries
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        assertThat(rewrittenBool.filter().get(0), instanceOf(TermsQueryBuilder.class));

        TermsQueryBuilder termsQuery = (TermsQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(termsQuery.fieldName(), equalTo("category"));
        assertThat(termsQuery.values().size(), equalTo(20));
    }

    public void testShouldMergingWithThreshold() {
        // Ensure should clauses merge when exceeding threshold
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 17; i++) {
            query.should(QueryBuilders.termQuery("tag", "t" + i));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        BoolQueryBuilder b = (BoolQueryBuilder) rewritten;
        assertThat(b.should().size(), equalTo(1));
        assertThat(b.should().get(0), instanceOf(TermsQueryBuilder.class));
    }

    public void testMixedFieldsAboveThresholdOnlyMergesPerField() {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 18; i++) {
            query.filter(QueryBuilders.termQuery("f1", "v" + i));
        }
        for (int i = 0; i < 5; i++) {
            query.filter(QueryBuilders.termQuery("f2", "v" + i));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        BoolQueryBuilder b = (BoolQueryBuilder) rewritten;
        // one terms for f1, and 5 single terms for f2
        assertThat(b.filter().size(), equalTo(6));
    }

    public void testMustClauseNoMerging() {
        // Term queries in must clauses should NOT be merged (different semantics)
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("category", "electronics"))
            .must(QueryBuilders.termQuery("category", "computers"))
            .must(QueryBuilders.rangeQuery("price").gt(100));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have 3 queries: NO merging in must clause
        assertThat(rewrittenBool.must().size(), equalTo(3));
    }

    public void testShouldClauseMergingBelowThreshold() {
        // Should clauses with few terms should NOT be merged
        QueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("color", "red"))
            .should(QueryBuilders.termQuery("color", "blue"))
            .should(QueryBuilders.termQuery("size", "large"))
            .should(QueryBuilders.termQuery("size", "medium"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes expected
    }

    public void testShouldClauseMergingAboveThreshold() {
        // Should clauses with many terms should be merged
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        // Add 20 color terms
        for (int i = 0; i < 20; i++) {
            query.should(QueryBuilders.termQuery("color", "color_" + i));
        }

        // Add 18 size terms
        for (int i = 0; i < 18; i++) {
            query.should(QueryBuilders.termQuery("size", "size_" + i));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have 2 terms queries: one for color, one for size
        assertThat(rewrittenBool.should().size(), equalTo(2));
        assertThat(rewrittenBool.should().get(0), instanceOf(TermsQueryBuilder.class));
        assertThat(rewrittenBool.should().get(1), instanceOf(TermsQueryBuilder.class));
    }

    public void testMixedFieldsNoMerging() {
        // Term queries on different fields should not be merged
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("field1", "value1"))
            .filter(QueryBuilders.termQuery("field2", "value2"))
            .filter(QueryBuilders.termQuery("field3", "value3"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes expected
    }

    public void testExistingTermsQueryExpansionBelowThreshold() {
        // Existing terms query with few additional terms should NOT be expanded (below threshold)
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("status", "active", "pending"))
            .filter(QueryBuilders.termQuery("status", "approved"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes expected
    }

    public void testExistingTermsQueryExpansionAboveThreshold() {
        // Existing terms query should be expanded when total terms exceed threshold
        String[] initialTerms = new String[14];
        for (int i = 0; i < 14; i++) {
            initialTerms[i] = "status_" + i;
        }

        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("status", initialTerms));

        // Add 5 more term queries to exceed threshold
        for (int i = 14; i < 19; i++) {
            query.filter(QueryBuilders.termQuery("status", "status_" + i));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have one terms query with all values
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        TermsQueryBuilder termsQuery = (TermsQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(termsQuery.values().size(), equalTo(19));
    }

    public void testSingleTermQuery() {
        // Single term query should not be converted to terms query
        QueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field", "value"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testMustNotClauseNoMerging() {
        // Must_not clauses should not be merged
        QueryBuilder query = QueryBuilders.boolQuery()
            .mustNot(QueryBuilders.termQuery("status", "deleted"))
            .mustNot(QueryBuilders.termQuery("status", "archived"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes to must_not
    }

    public void testNestedBooleanQuery() {
        // Should handle nested boolean queries with many terms
        BoolQueryBuilder nested = QueryBuilders.boolQuery();
        // Add 20 term queries to exceed threshold
        for (int i = 0; i < 20; i++) {
            nested.filter(QueryBuilders.termQuery("status", "status_" + i));
        }

        QueryBuilder query = QueryBuilders.boolQuery().must(nested).filter(QueryBuilders.termQuery("type", "product"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Nested bool should also be rewritten
        assertThat(rewrittenBool.must().size(), equalTo(1));
        BoolQueryBuilder nestedRewritten = (BoolQueryBuilder) rewrittenBool.must().get(0);
        assertThat(nestedRewritten.filter().size(), equalTo(1));
        assertThat(nestedRewritten.filter().get(0), instanceOf(TermsQueryBuilder.class));

        TermsQueryBuilder termsQuery = (TermsQueryBuilder) nestedRewritten.filter().get(0);
        assertThat(termsQuery.values().size(), equalTo(20));
    }

    public void testEmptyBooleanQuery() {
        // Empty boolean query should not cause issues
        QueryBuilder query = QueryBuilders.boolQuery();
        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testNonBooleanQuery() {
        // Non-boolean queries should be returned as-is
        QueryBuilder query = QueryBuilders.termQuery("field", "value");
        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testBoostPreservation() {
        // Boost values should be preserved when merging many terms
        BoolQueryBuilder query = QueryBuilders.boolQuery();

        // Add 20 terms with same boost
        for (int i = 0; i < 20; i++) {
            query.filter(QueryBuilders.termQuery("status", "status_" + i).boost(2.0f));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        assertThat(rewrittenBool.filter().size(), equalTo(1));
        TermsQueryBuilder termsQuery = (TermsQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(termsQuery.boost(), equalTo(2.0f));
        assertThat(termsQuery.values().size(), equalTo(20));
    }

    public void testMixedBoostNoMerging() {
        // Different boost values should prevent merging
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("status", "active").boost(1.0f))
            .filter(QueryBuilders.termQuery("status", "pending").boost(2.0f));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes due to different boosts
    }

    public void testLargeTermsMerging() {
        // Test merging a large number of term queries
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 50; i++) {
            query.filter(QueryBuilders.termQuery("field", "value" + i));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        BoolQueryBuilder result = (BoolQueryBuilder) rewritten;

        assertThat(result.filter().size(), equalTo(1));
        assertThat(result.filter().get(0), instanceOf(TermsQueryBuilder.class));
        TermsQueryBuilder terms = (TermsQueryBuilder) result.filter().get(0);
        assertThat(terms.values().size(), equalTo(50));
    }

    public void testMixedTermsAndTermQueriesBelowThreshold() {
        // Mix of existing terms queries and term queries with few values
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("field", "v1", "v2"))
            .filter(QueryBuilders.termQuery("field", "v3"))
            .filter(QueryBuilders.termsQuery("field", "v4", "v5"))
            .filter(QueryBuilders.termQuery("field", "v6"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes expected (total 6 values < 16)
    }

    public void testMixedTermsAndTermQueriesAboveThreshold() {
        // Mix of existing terms queries and term queries with many values
        String[] initialValues = new String[10];
        for (int i = 0; i < 10; i++) {
            initialValues[i] = "v" + i;
        }

        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery("field", initialValues));

        // Add more term queries to exceed threshold
        for (int i = 10; i < 20; i++) {
            query.filter(QueryBuilders.termQuery("field", "v" + i));
        }

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        BoolQueryBuilder result = (BoolQueryBuilder) rewritten;

        // Should merge all into a single terms query
        assertThat(result.filter().size(), equalTo(1));
        assertThat(result.filter().get(0), instanceOf(TermsQueryBuilder.class));
        TermsQueryBuilder merged = (TermsQueryBuilder) result.filter().get(0);
        assertThat(merged.values().size(), equalTo(20));
    }

}
