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
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class MatchAllRemovalRewriterTests extends OpenSearchTestCase {

    private final MatchAllRemovalRewriter rewriter = MatchAllRemovalRewriter.INSTANCE;
    private final QueryShardContext context = mock(QueryShardContext.class);

    public void testRemoveMatchAllFromMust() {
        // match_all in must clause should NOT be removed in scoring context
        QueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).must(QueryBuilders.termQuery("field", "value"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // match_all should be kept in scoring context
        assertThat(rewrittenBool.must().size(), equalTo(2));
    }

    public void testRemoveMatchAllFromFilter() {
        // match_all in filter clause should be removed
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.matchAllQuery())
            .filter(QueryBuilders.rangeQuery("price").gt(100))
            .must(QueryBuilders.termQuery("category", "electronics"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // match_all should be removed from filter
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        assertThat(rewrittenBool.must().size(), equalTo(1));
    }

    public void testKeepMatchAllInShould() {
        // match_all in should clause should be kept
        QueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.matchAllQuery())
            .should(QueryBuilders.termQuery("field", "value"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes for should clause
    }

    public void testKeepMatchAllInMustNot() {
        // match_all in must_not clause should be kept (it's meaningful)
        QueryBuilder query = QueryBuilders.boolQuery()
            .mustNot(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.termQuery("field", "value"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // No changes for must_not clause
    }

    public void testOnlyMatchAllQuery() {
        // Boolean query with only match_all should be simplified to match_all
        QueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(QueryBuilders.matchAllQuery().getClass()));
    }

    public void testMultipleMatchAllQueries() {
        // Multiple match_all queries should all be removed
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.matchAllQuery())
            .filter(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.termQuery("field", "value"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // With filter clause present, this is not a pure scoring context
        // Since there are non-match_all queries in must, match_all should be removed
        assertThat(rewrittenBool.must().size(), equalTo(1)); // only term query remains
        assertThat(rewrittenBool.filter().size(), equalTo(0));
    }

    public void testMustMatchAllRemovedWhenFilterPresent() {
        // must contains match_all + term, and there is a filter clause => remove match_all from must
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.termQuery("field", "v"))
            .filter(QueryBuilders.termQuery("f2", "x"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;
        // match_all removed from must because non-scoring context exists (filter present)
        assertThat(rewrittenBool.must().size(), equalTo(1));
    }

    public void testNestedFilterMatchAllRemoved() {
        // match_all inside nested bool under filter should be removed
        QueryBuilder nested = QueryBuilders.boolQuery().filter(QueryBuilders.matchAllQuery()).filter(QueryBuilders.termQuery("a", "b"));
        QueryBuilder query = QueryBuilders.boolQuery().filter(nested);

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;
        BoolQueryBuilder nestedRewritten = (BoolQueryBuilder) rewrittenBool.filter().get(0);
        // Only the real filter remains
        assertThat(nestedRewritten.filter().size(), equalTo(1));
    }

    public void testNestedBooleanWithMatchAll() {
        // Nested boolean queries should also have match_all removed
        QueryBuilder nested = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.termQuery("field1", "value1"));

        QueryBuilder query = QueryBuilders.boolQuery().must(nested).filter(QueryBuilders.termQuery("field2", "value2"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Nested bool keeps match_all in scoring context
        BoolQueryBuilder nestedRewritten = (BoolQueryBuilder) rewrittenBool.must().get(0);
        assertThat(nestedRewritten.must().size(), equalTo(2)); // match_all + term
    }

    public void testEmptyBoolAfterRemoval() {
        // Bool with only match_all in must/filter - keeps match_all in must in scoring context
        QueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).filter(QueryBuilders.matchAllQuery());

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // match_all in must is kept in scoring context, match_all in filter is removed
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.filter().size(), equalTo(0));
    }

    public void testBoolWithOnlyMustNotAfterRemoval() {
        // Bool with only must_not after removal should not be converted to match_all
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchAllQuery())
            .mustNot(QueryBuilders.termQuery("status", "deleted"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // must clause keeps match_all in scoring context, must_not preserved
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().size(), equalTo(1));
    }

    public void testNonBooleanQuery() {
        // Non-boolean queries should be returned as-is
        QueryBuilder query = QueryBuilders.matchAllQuery();
        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testEmptyBooleanQuery() {
        // Empty boolean query should not be converted
        QueryBuilder query = QueryBuilders.boolQuery();
        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testBoostPreservation() {
        // When converting bool with only match_all to match_all, preserve boost
        QueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).boost(2.0f);

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(QueryBuilders.matchAllQuery().getClass()));
        assertThat(rewritten.boost(), equalTo(2.0f));
    }
}
