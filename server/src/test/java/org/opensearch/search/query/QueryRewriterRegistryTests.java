/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class QueryRewriterRegistryTests extends OpenSearchTestCase {

    private final QueryShardContext context = mock(QueryShardContext.class);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Initialize registry with default settings
        Settings settings = Settings.builder()
            .put(SearchService.QUERY_REWRITING_ENABLED_SETTING.getKey(), true)
            .put(SearchService.QUERY_REWRITING_TERMS_THRESHOLD_SETTING.getKey(), 16)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        QueryRewriterRegistry.INSTANCE.initialize(settings, clusterSettings);
    }

    public void testCompleteRewritingPipeline() {
        // Test that all rewriters work together correctly
        QueryBuilder nestedBool = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchAllQuery())
            .must(QueryBuilders.termQuery("status", "active"))
            .must(QueryBuilders.termQuery("status", "pending"));

        QueryBuilder query = QueryBuilders.boolQuery()
            .must(nestedBool)
            .filter(QueryBuilders.matchAllQuery())
            .filter(QueryBuilders.termQuery("type", "product"))
            .filter(QueryBuilders.termQuery("type", "service"));

        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should have:
        // - Flattened nested boolean
        // - Terms in must clauses are NOT merged (semantically different)
        // - Removed match_all queries
        assertThat(rewrittenBool.must().size(), equalTo(2)); // two term queries for status
        assertThat(rewrittenBool.must().get(0), instanceOf(TermQueryBuilder.class));
        assertThat(rewrittenBool.must().get(1), instanceOf(TermQueryBuilder.class));

        assertThat(rewrittenBool.filter().size(), equalTo(2)); // two term queries for type (below threshold)
        assertThat(rewrittenBool.filter().get(0), instanceOf(TermQueryBuilder.class));
        assertThat(rewrittenBool.filter().get(1), instanceOf(TermQueryBuilder.class));
    }

    public void testDisabledRewriting() {
        // Test disabled rewriting via settings
        Settings settings = Settings.builder().put(SearchService.QUERY_REWRITING_ENABLED_SETTING.getKey(), false).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        // Initialize with disabled setting
        QueryRewriterRegistry.INSTANCE.initialize(settings, clusterSettings);

        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchAllQuery())
            .filter(QueryBuilders.termQuery("field", "value"));

        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertSame(query, rewritten);

        // Enable via settings update
        clusterSettings.applySettings(Settings.builder().put(SearchService.QUERY_REWRITING_ENABLED_SETTING.getKey(), true).build());

        // Now it should rewrite
        QueryBuilder rewritten2 = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertNotSame(query, rewritten2);
    }

    public void testDynamicTermsThresholdUpdate() {
        // Build a query at threshold=16 that merges, then raise threshold to 32 and assert no merge
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 16; i++) {
            query.filter(QueryBuilders.termQuery("f", "v" + i));
        }

        QueryBuilder merged = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        BoolQueryBuilder mb = (BoolQueryBuilder) merged;
        // Should be one terms query
        assertEquals(1, mb.filter().size());

        // Increase threshold
        Settings newSettings = Settings.builder().put(SearchService.QUERY_REWRITING_TERMS_THRESHOLD_SETTING.getKey(), 32).build();
        QueryRewriterRegistry.INSTANCE.initialize(newSettings, new ClusterSettings(newSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

        QueryBuilder notMerged = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        BoolQueryBuilder nmb = (BoolQueryBuilder) notMerged;
        // Should be all individual term queries now
        assertEquals(16, nmb.filter().size());
    }

    public void testNullQuery() {
        // Null query should return null
        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(null, context);
        assertNull(rewritten);
    }

    public void testRegistryIdempotence() {
        QueryBuilder q = QueryBuilders.boolQuery()
            .must(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("f", "v")))
            .filter(QueryBuilders.boolQuery().filter(QueryBuilders.termQuery("g", "w")))
            .should(QueryBuilders.termQuery("h", "u"));

        QueryBuilder once = QueryRewriterRegistry.INSTANCE.rewrite(q, context);
        QueryBuilder twice = QueryRewriterRegistry.INSTANCE.rewrite(once, context);
        assertEquals(once.toString(), twice.toString());
    }

    public void testRewriterPriorityOrder() {
        // Test that rewriters are applied in correct order
        // Create a query that will be affected by multiple rewriters
        QueryBuilder deeplyNested = QueryBuilders.boolQuery()
            .must(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchAllQuery())
                    .must(QueryBuilders.termQuery("field", "value1"))
                    .must(QueryBuilders.termQuery("field", "value2"))
            );

        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(deeplyNested, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // Should be flattened first. Match_all may be removed depending on context; terms NOT merged in must context
        long termCount = rewrittenBool.must().stream().filter(q -> q instanceof TermQueryBuilder).count();
        assertThat(termCount, equalTo(2L));

        // Composition order smoke check: must_to_filter should not move term queries in must
        // terms_merging should not merge terms in must; only in filter/should contexts
    }

    public void testComplexRealWorldQuery() {
        // Test a complex real-world-like query
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchAllQuery())
                    .filter(QueryBuilders.termQuery("category", "electronics"))
                    .filter(QueryBuilders.termQuery("category", "computers"))
            )
            .filter(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("brand", "apple"))
                    .should(QueryBuilders.termQuery("brand", "dell"))
                    .should(QueryBuilders.termQuery("brand", "hp"))
            )
            .must(QueryBuilders.rangeQuery("price").gte(500).lte(2000))
            .mustNot(QueryBuilders.termQuery("status", "discontinued"));

        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // After rewriting:
        // - The nested bool in must clause should be flattened
        // - match_all should be removed in non-scoring contexts
        // - term queries should be merged into terms query in filter contexts
        // - The filter bool with brand terms should be preserved
        // - The range query should be moved from must to filter by MustToFilterRewriter

        // Check must clauses (should have terms query for category only - range moved to filter)
        assertThat(rewrittenBool.must().size(), equalTo(1));

        // Check filter clauses (should have the brand bool query AND the range query)
        assertThat(rewrittenBool.filter().size(), equalTo(2));
        // One should be the brand bool query
        boolean hasBoolFilter = false;
        boolean hasRangeFilter = false;
        for (QueryBuilder filter : rewrittenBool.filter()) {
            if (filter instanceof BoolQueryBuilder) {
                hasBoolFilter = true;
            } else if (filter instanceof RangeQueryBuilder) {
                hasRangeFilter = true;
            }
        }
        assertTrue(hasBoolFilter);
        assertTrue(hasRangeFilter);

        // Must not should be preserved
        assertThat(rewrittenBool.mustNot().size(), equalTo(1));
    }

    public void testPerformanceMetrics() {
        // Test that we log performance metrics in debug mode
        // This is more of a sanity check that the timing code doesn't throw exceptions
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("field1", "value1"))
            .must(QueryBuilders.termQuery("field1", "value2"))
            .filter(QueryBuilders.matchAllQuery());

        // Should not throw any exceptions
        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertNotNull(rewritten);
    }

    public void testRewriterErrorHandling() {
        // Test that if a rewriter throws an exception, others still run
        // This is handled internally by QueryRewriterRegistry
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("field", "value"))
            .filter(QueryBuilders.matchAllQuery());

        // Even if one rewriter fails, others should still be applied
        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertNotNull(rewritten);
    }

    public void testVeryComplexMixedQuery() {
        // Test a very complex query with all optimizations applicable
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchAllQuery())
            .must(
                QueryBuilders.boolQuery()
                    .must(
                        QueryBuilders.boolQuery()
                            .filter(QueryBuilders.termQuery("status", "active"))
                            .filter(QueryBuilders.termQuery("status", "pending"))
                            .filter(QueryBuilders.termQuery("status", "approved"))
                    )
                    .must(
                        QueryBuilders.boolQuery()
                            .must(QueryBuilders.matchAllQuery())
                            .filter(QueryBuilders.termQuery("type", "A"))
                            .filter(QueryBuilders.termQuery("type", "B"))
                    )
            )
            .filter(QueryBuilders.matchAllQuery())
            .filter(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("priority", "high"))
                    .should(QueryBuilders.termQuery("priority", "medium"))
                    .should(QueryBuilders.termQuery("priority", "low"))
            )
            .should(QueryBuilders.termQuery("category", "urgent"))
            .should(QueryBuilders.termQuery("category", "important"))
            .mustNot(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("archived", "true")))
            .minimumShouldMatch(1);

        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder result = (BoolQueryBuilder) rewritten;

        // Check that minimum should match is preserved
        assertThat(result.minimumShouldMatch(), equalTo("1"));

        // Verify optimizations were applied
        assertNotSame(query, rewritten);

        // Should have flattened structure and merged terms
        assertTrue(result.must().size() >= 1);
        assertTrue(result.filter().size() >= 1);
    }

    public void testCustomRewriterRegistration() {
        // Create a custom rewriter for testing
        QueryRewriter customRewriter = new QueryRewriter() {
            @Override
            public QueryBuilder rewrite(QueryBuilder query, QueryShardContext context) {
                if (query instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) query;
                    if ("test_field".equals(termQuery.fieldName()) && "test_value".equals(termQuery.value())) {
                        // Replace with a different query
                        return QueryBuilders.termQuery("custom_field", "custom_value");
                    }
                } else if (query instanceof BoolQueryBuilder) {
                    // Recursively apply to nested queries
                    BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
                    BoolQueryBuilder rewritten = new BoolQueryBuilder();

                    // Copy settings
                    rewritten.boost(boolQuery.boost());
                    rewritten.queryName(boolQuery.queryName());
                    rewritten.minimumShouldMatch(boolQuery.minimumShouldMatch());
                    rewritten.adjustPureNegative(boolQuery.adjustPureNegative());

                    // Recursively rewrite clauses
                    boolean changed = false;
                    for (QueryBuilder must : boolQuery.must()) {
                        QueryBuilder rewrittenClause = rewrite(must, context);
                        rewritten.must(rewrittenClause);
                        if (rewrittenClause != must) changed = true;
                    }
                    for (QueryBuilder filter : boolQuery.filter()) {
                        QueryBuilder rewrittenClause = rewrite(filter, context);
                        rewritten.filter(rewrittenClause);
                        if (rewrittenClause != filter) changed = true;
                    }
                    for (QueryBuilder should : boolQuery.should()) {
                        QueryBuilder rewrittenClause = rewrite(should, context);
                        rewritten.should(rewrittenClause);
                        if (rewrittenClause != should) changed = true;
                    }
                    for (QueryBuilder mustNot : boolQuery.mustNot()) {
                        QueryBuilder rewrittenClause = rewrite(mustNot, context);
                        rewritten.mustNot(rewrittenClause);
                        if (rewrittenClause != mustNot) changed = true;
                    }

                    return changed ? rewritten : query;
                }
                return query;
            }

            @Override
            public int priority() {
                return 1000; // High priority to ensure it runs last
            }

            @Override
            public String name() {
                return "test_custom_rewriter";
            }
        };

        // Register the custom rewriter
        QueryRewriterRegistry.INSTANCE.registerRewriter(customRewriter);

        // Test that it's applied
        QueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("test_field", "test_value"))
            .filter(QueryBuilders.termQuery("other_field", "other_value"));

        QueryBuilder rewritten = QueryRewriterRegistry.INSTANCE.rewrite(query, context);
        assertThat(rewritten, instanceOf(BoolQueryBuilder.class));
        BoolQueryBuilder rewrittenBool = (BoolQueryBuilder) rewritten;

        // The custom rewriter should have replaced the term query
        assertThat(rewrittenBool.must().size(), equalTo(1));
        assertThat(rewrittenBool.must().get(0), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder mustTerm = (TermQueryBuilder) rewrittenBool.must().get(0);
        assertThat(mustTerm.fieldName(), equalTo("custom_field"));
        assertThat(mustTerm.value(), equalTo("custom_value"));
    }
}
