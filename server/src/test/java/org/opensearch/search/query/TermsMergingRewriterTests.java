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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class TermsMergingRewriterTests extends OpenSearchTestCase {

    private final TermsMergingRewriter rewriter = new TermsMergingRewriter();
    private final QueryShardContext context = mock(QueryShardContext.class);

    public void testSimpleTermMerging() {
        // Multiple term queries on same field should be merged
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("status", "active"))
            .filter(QueryBuilders.termQuery("status", "pending"))
            .filter(QueryBuilders.termQuery("status", "approved"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have one terms query instead of three term queries
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        assertThat(rewrittenBool.filter().get(0), instanceOf(TermsQueryBuilder.class));

        TermsQueryBuilder termsQuery = (TermsQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(termsQuery.fieldName(), equalTo("status"));
        assertThat(termsQuery.values().size(), equalTo(3));
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

    public void testShouldClauseMerging() {
        // Should clauses should be merged independently
        QueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("color", "red"))
            .should(QueryBuilders.termQuery("color", "blue"))
            .should(QueryBuilders.termQuery("size", "large"))
            .should(QueryBuilders.termQuery("size", "medium"));

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

    public void testExistingTermsQueryExpansion() {
        // Existing terms query should be expanded with additional term queries
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("status", "active", "pending"))
            .filter(QueryBuilders.termQuery("status", "approved"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have one terms query with all values
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        TermsQueryBuilder termsQuery = (TermsQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(termsQuery.values().size(), equalTo(3));
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
        // Should handle nested boolean queries
        QueryBuilder nested = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("status", "active"))
            .filter(QueryBuilders.termQuery("status", "pending"));

        QueryBuilder query = QueryBuilders.boolQuery().must(nested).filter(QueryBuilders.termQuery("type", "product"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Nested bool should also be rewritten
        assertThat(rewrittenBool.must().size(), equalTo(1));
        BoolQueryBuilder nestedRewritten = (BoolQueryBuilder) rewrittenBool.must().get(0);
        assertThat(nestedRewritten.filter().size(), equalTo(1));
        assertThat(nestedRewritten.filter().get(0), instanceOf(TermsQueryBuilder.class));
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
        // Boost values should be preserved when merging
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("status", "active").boost(2.0f))
            .filter(QueryBuilders.termQuery("status", "pending").boost(2.0f));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        TermsQueryBuilder termsQuery = (TermsQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(termsQuery.boost(), equalTo(2.0f));
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

    public void testMixedTermsAndTermQueries() {
        // Mix of existing terms queries and term queries
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termsQuery("field", "v1", "v2"))
            .filter(QueryBuilders.termQuery("field", "v3"))
            .filter(QueryBuilders.termsQuery("field", "v4", "v5"))
            .filter(QueryBuilders.termQuery("field", "v6"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        BoolQueryBuilder result = (BoolQueryBuilder) rewritten;

        // Should merge all into a single terms query
        assertThat(result.filter().size(), equalTo(1));
        assertThat(result.filter().get(0), instanceOf(TermsQueryBuilder.class));
        TermsQueryBuilder merged = (TermsQueryBuilder) result.filter().get(0);
        assertThat(merged.values().size(), equalTo(6));
    }
}
