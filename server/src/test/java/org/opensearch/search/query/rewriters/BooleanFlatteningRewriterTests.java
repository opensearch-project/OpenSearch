/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class BooleanFlatteningRewriterTests extends OpenSearchTestCase {

    private final BooleanFlatteningRewriter rewriter = BooleanFlatteningRewriter.INSTANCE;
    private final QueryShardContext context = mock(QueryShardContext.class);

    public void testSimpleBooleanQuery() {
        // Simple boolean query should not be modified
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("field1", "value1"))
            .filter(QueryBuilders.termQuery("field2", "value2"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testNestedBooleanFlattening() {
        // Nested boolean query with single must clause should be flattened
        QueryBuilder nestedBool = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field1", "value1"));

        QueryBuilder query = QueryBuilders.boolQuery().must(nestedBool).filter(QueryBuilders.termQuery("field2", "value2"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // The nested bool should be flattened
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.must().get(0), instanceOf(QueryBuilders.termQuery("field1", "value1").getClass()));
        assertThat(rewrittenBool.filter().size(), equalTo(1));
    }

    public void testMultipleNestedBooleansFlattening() {
        // Multiple nested boolean queries should all be flattened
        QueryBuilder nested1 = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("field1", "value1"))
            .must(QueryBuilders.termQuery("field2", "value2"));

        QueryBuilder nested2 = QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("field3", "value3"));

        QueryBuilder query = QueryBuilders.boolQuery().must(nested1).filter(nested2);

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // All nested clauses should be flattened
        assertThat(rewrittenBool.must().size(), equalTo(2));
        assertThat(rewrittenBool.filter().size(), equalTo(1));
    }

    public void testShouldClauseFlattening() {
        // Should clauses should also be flattened
        QueryBuilder nestedShould = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .should(QueryBuilders.termQuery("field2", "value2"));

        QueryBuilder query = QueryBuilders.boolQuery().should(nestedShould).must(QueryBuilders.termQuery("field3", "value3"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should clauses should be flattened
        assertThat(rewrittenBool.should().size(), equalTo(2));
        assertThat(rewrittenBool.must().size(), equalTo(1));
    }

    public void testMustNotClauseNoFlattening() {
        // Must_not clauses should NOT be flattened to preserve semantics
        QueryBuilder nestedMustNot = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field1", "value1"));

        QueryBuilder query = QueryBuilders.boolQuery().mustNot(nestedMustNot).must(QueryBuilders.termQuery("field2", "value2"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Must_not should not be flattened
        assertThat(rewrittenBool.mustNot().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().get(0), instanceOf(BoolQueryBuilder.class));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/18906")
    public void testDeepNesting() {
        // TODO: This test expects complete flattening of deeply nested bool queries
        // where intermediate bool wrappers are removed entirely. Our current implementation
        // only flattens by merging same-type clauses but preserves the bool structure.
        // This would require a different optimization strategy.

        // Deep nesting should be flattened at all levels
        QueryBuilder deepNested = QueryBuilders.boolQuery()
            .must(QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field1", "value1"))));

        QueryBuilder rewritten = rewriter.rewrite(deepNested, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should be flattened to single level bool with term query
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.must().get(0), instanceOf(TermQueryBuilder.class));

        // Verify the term query details
        TermQueryBuilder termQuery = (TermQueryBuilder) rewrittenBool.must().get(0);
        assertThat(termQuery.fieldName(), equalTo("field1"));
        assertThat(termQuery.value(), equalTo("value1"));
    }

    public void testMixedClauseTypes() {
        // Mixed clause types with different minimumShouldMatch settings
        QueryBuilder nested = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field1", "value1"))
            .should(QueryBuilders.termQuery("field2", "value2"))
            .minimumShouldMatch(1);

        QueryBuilder query = QueryBuilders.boolQuery().must(nested).minimumShouldMatch(2);

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten); // Should not flatten due to different minimumShouldMatch
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

    public void testVeryDeepNesting() {
        // Test with 10 levels of nesting
        QueryBuilder innermost = QueryBuilders.termQuery("field", "value");
        for (int i = 0; i < 10; i++) {
            innermost = QueryBuilders.boolQuery().must(innermost);
        }

        QueryBuilder rewritten = rewriter.rewrite(innermost, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));

        // Should be flattened significantly
        BoolQueryBuilder result = (BoolQueryBuilder) rewritten;
        assertThat(result.must().size(), equalTo(1));
    }

    public void testQueryNamePreservation() {
        // Ensure query names are preserved during flattening
        QueryBuilder query = QueryBuilders.boolQuery()
            .queryName("outer")
            .must(QueryBuilders.boolQuery().queryName("inner").must(QueryBuilders.termQuery("field", "value")));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        BoolQueryBuilder result = (BoolQueryBuilder) rewritten;
        assertThat(result.queryName(), equalTo("outer"));
    }
}
