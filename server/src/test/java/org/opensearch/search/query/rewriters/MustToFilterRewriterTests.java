/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.rewriters;

import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MustToFilterRewriterTests extends OpenSearchTestCase {

    private final MustToFilterRewriter rewriter = MustToFilterRewriter.INSTANCE;
    private QueryShardContext context;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        context = mock(QueryShardContext.class);

        // Setup numeric field types
        NumberFieldMapper.NumberFieldType intFieldType = mock(NumberFieldMapper.NumberFieldType.class);
        when(context.fieldMapper("age")).thenReturn(intFieldType);
        when(context.fieldMapper("price")).thenReturn(intFieldType);
        when(context.fieldMapper("count")).thenReturn(intFieldType);
        when(context.fieldMapper("user_id")).thenReturn(intFieldType);

        // Setup non-numeric field types
        MappedFieldType textFieldType = mock(MappedFieldType.class);
        when(context.fieldMapper("name")).thenReturn(textFieldType);
        when(context.fieldMapper("description")).thenReturn(textFieldType);
        when(context.fieldMapper("category")).thenReturn(textFieldType);
        when(context.fieldMapper("status_code")).thenReturn(textFieldType);
        when(context.fieldMapper("active")).thenReturn(textFieldType);
    }

    public void testRangeQueryMovedToFilter() {
        // Range queries should always be moved to filter
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("price").gte(100).lte(500))
            .must(QueryBuilders.termQuery("category", "electronics"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Range query should be moved to filter
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        assertThat(rewrittenBool.filter().get(0), instanceOf(RangeQueryBuilder.class));

        // Term query on text field should remain in must
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.must().get(0), instanceOf(TermQueryBuilder.class));
    }

    public void testNumericTermQueryMovedToFilter() {
        // Term queries on numeric fields should be moved to filter
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("age", 25))
            .must(QueryBuilders.termQuery("name", "John"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Numeric term query should be moved to filter
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        TermQueryBuilder filterClause = (TermQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(filterClause.fieldName(), equalTo("age"));

        // Text term query should remain in must
        assertThat(rewrittenBool.must().size(), equalTo(1));
        TermQueryBuilder mustClause = (TermQueryBuilder) rewrittenBool.must().get(0);
        assertThat(mustClause.fieldName(), equalTo("name"));
    }

    public void testNumericTermsQueryMovedToFilter() {
        // Terms queries on numeric fields should be moved to filter
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termsQuery("count", new Object[] { 1, 2, 3 }))
            .must(QueryBuilders.termsQuery("category", "A", "B", "C"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Numeric terms query should be moved to filter
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        TermsQueryBuilder filterClause = (TermsQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(filterClause.fieldName(), equalTo("count"));

        // Text terms query should remain in must
        assertThat(rewrittenBool.must().size(), equalTo(1));
        TermsQueryBuilder mustClause = (TermsQueryBuilder) rewrittenBool.must().get(0);
        assertThat(mustClause.fieldName(), equalTo("category"));
    }

    public void testNumericMatchQueryMovedToFilter() {
        // Match queries on numeric fields should be moved to filter
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("price", 99.99))
            .must(QueryBuilders.matchQuery("description", "high quality"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Numeric match query should be moved to filter
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        MatchQueryBuilder filterClause = (MatchQueryBuilder) rewrittenBool.filter().get(0);
        assertThat(filterClause.fieldName(), equalTo("price"));

        // Text match query should remain in must
        assertThat(rewrittenBool.must().size(), equalTo(1));
        MatchQueryBuilder mustClause = (MatchQueryBuilder) rewrittenBool.must().get(0);
        assertThat(mustClause.fieldName(), equalTo("description"));
    }

    public void testExistingFilterClausesPreserved() {
        // Existing filter clauses should be preserved
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("price").gte(100))
            .filter(QueryBuilders.termQuery("status", "active"))
            .filter(QueryBuilders.existsQuery("description"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have 3 filter clauses: moved range query + 2 existing
        assertThat(rewrittenBool.filter().size(), equalTo(3));
        assertThat(rewrittenBool.must().size(), equalTo(0));
    }

    public void testShouldAndMustNotClausesUnchanged() {
        // Should and must_not clauses should not be affected
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("price").gte(100))
            .should(QueryBuilders.termQuery("featured", true))
            .mustNot(QueryBuilders.termQuery("deleted", true));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Range query moved to filter
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        assertThat(rewrittenBool.must().size(), equalTo(0));

        // Should and must_not unchanged
        assertThat(rewrittenBool.should().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().size(), equalTo(1));
    }

    public void testNestedBooleanQueriesRewritten() {
        // Nested boolean queries should also be rewritten
        QueryBuilder nested = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("age").gte(18))
            .must(QueryBuilders.termQuery("active", true));

        QueryBuilder query = QueryBuilders.boolQuery().must(nested).must(QueryBuilders.matchQuery("name", "test"));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // The nested bool should be rewritten
        assertThat(rewrittenBool.must().size(), equalTo(2));

        // Find the nested bool query
        BoolQueryBuilder nestedRewritten = null;
        for (QueryBuilder clause : rewrittenBool.must()) {
            if (clause instanceof BoolQueryBuilder) {
                nestedRewritten = (BoolQueryBuilder) clause;
                break;
            }
        }

        assertNotNull(nestedRewritten);
        // The range query in the nested bool should be moved to filter
        assertThat(nestedRewritten.filter().size(), equalTo(1));
        assertThat(nestedRewritten.filter().get(0), instanceOf(RangeQueryBuilder.class));
        // The term query should remain in must
        assertThat(nestedRewritten.must().size(), equalTo(1));
        assertThat(nestedRewritten.must().get(0), instanceOf(TermQueryBuilder.class));
    }

    public void testBoostedNumericQueriesMovedToFilter() {
        // Even with boosts, numeric queries should be moved (boost irrelevant in filter)
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("age", 42).boost(3.0f))
            .must(QueryBuilders.rangeQuery("price").gte(10).boost(1.5f))
            .must(QueryBuilders.matchQuery("name", "foo").boost(2.0f));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        BoolQueryBuilder b = (BoolQueryBuilder) rewritten;
        // two moved
        assertThat(b.filter().size(), equalTo(2));
        // text match remains
        assertThat(b.must().size(), equalTo(1));
        assertThat(b.must().get(0), instanceOf(MatchQueryBuilder.class));
    }

    public void testNoContextDoesNotMoveNumericTerms() {
        // Without context, numeric term/terms cannot be identified; range still moves
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("user_id", 7))
            .must(QueryBuilders.rangeQuery("price").gte(1));

        QueryBuilder rewritten = rewriter.rewrite(query, null);
        BoolQueryBuilder b = (BoolQueryBuilder) rewritten;
        // Range moved, term stayed
        assertThat(b.filter().size(), equalTo(1));
        assertThat(b.must().size(), equalTo(1));
        assertThat(b.must().get(0), instanceOf(TermQueryBuilder.class));
    }

    public void testBoolQueryPropertiesPreserved() {
        // All bool query properties should be preserved
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("price").gte(100))
            .must(QueryBuilders.termQuery("category", "electronics"))
            .boost(2.0f)
            .queryName("my_query")
            .minimumShouldMatch(2)
            .adjustPureNegative(false);

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Properties should be preserved
        assertThat(rewrittenBool.boost(), equalTo(2.0f));
        assertThat(rewrittenBool.queryName(), equalTo("my_query"));
        assertThat(rewrittenBool.minimumShouldMatch(), equalTo("2"));
        assertThat(rewrittenBool.adjustPureNegative(), equalTo(false));
    }

    public void testNoMustClausesNoChanges() {
        // Query without must clauses should not be changed
        QueryBuilder query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.rangeQuery("price").gte(100))
            .should(QueryBuilders.termQuery("featured", true));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testNonBoolQueryUnchanged() {
        // Non-bool queries should not be changed
        QueryBuilder query = QueryBuilders.termQuery("field", "value");
        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertSame(query, rewritten);
    }

    public void testNullContextStillMovesRangeQueries() {
        // With null context, range queries should still be moved
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("price").gte(100))
            .must(QueryBuilders.termQuery("age", 25));

        QueryBuilder rewritten = rewriter.rewrite(query, null);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Range query should be moved to filter even without context
        assertThat(rewrittenBool.filter().size(), equalTo(1));
        assertThat(rewrittenBool.filter().get(0), instanceOf(RangeQueryBuilder.class));

        // Term query stays in must (can't determine if numeric without context)
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.must().get(0), instanceOf(TermQueryBuilder.class));
    }

    public void testAllMustClausesMovedToFilter() {
        // If all must clauses can be moved, they should all go to filter
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("price").gte(100))
            .must(QueryBuilders.rangeQuery("age").gte(18))
            .must(QueryBuilders.termQuery("count", 5));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // All clauses should be in filter
        assertThat(rewrittenBool.filter().size(), equalTo(3));
        assertThat(rewrittenBool.must().size(), equalTo(0));
    }

    public void testComplexMixedQuery() {
        // Complex query with mix of movable and non-movable clauses
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.rangeQuery("created_date").gte("2024-01-01"))
            .must(QueryBuilders.termQuery("user_id", 12345))
            .must(QueryBuilders.matchQuery("title", "opensearch"))
            .must(QueryBuilders.termsQuery("status_code", new Object[] { 200, 201, 204 }))
            .filter(QueryBuilders.existsQuery("description"))
            .should(QueryBuilders.matchQuery("tags", "important"))
            .mustNot(QueryBuilders.termQuery("deleted", true));

        QueryBuilder rewritten = rewriter.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Range and numeric queries moved to filter
        assertThat(rewrittenBool.filter().size(), equalTo(3)); // range + exists + user_id (numeric)

        // Text queries remain in must
        assertThat(rewrittenBool.must().size(), equalTo(2)); // match title + terms status_code (text field)

        // Should and must_not unchanged
        assertThat(rewrittenBool.should().size(), equalTo(1));
        assertThat(rewrittenBool.mustNot().size(), equalTo(1));
    }

    public void testRewriterPriority() {
        // Verify rewriter has correct priority
        assertThat(rewriter.priority(), equalTo(150));
        assertThat(rewriter.name(), equalTo("must_to_filter"));
    }
}
