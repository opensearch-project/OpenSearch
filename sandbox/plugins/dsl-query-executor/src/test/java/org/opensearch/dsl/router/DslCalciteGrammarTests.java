/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.router;

import org.mockito.Mockito;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class DslCalciteGrammarTests extends OpenSearchTestCase {

    private final AggregationRegistry aggRegistry = AggregationRegistryFactory.create();

    /** Grammar with only the translators actually registered on this branch. */
    private final DslCalciteGrammar grammar = new DslCalciteGrammar(QueryRegistryFactory.create(), aggRegistry);

    /**
     * Grammar with extra query types treated as if their translators were registered.
     * Used to exercise per-parameter checks for range/prefix/wildcard whose translators
     * live behind pending PRs (#22525, #22526).
     */
    @SafeVarargs
    private DslCalciteGrammar grammarWith(Class<? extends QueryBuilder>... registeredExtras) {
        QueryRegistry spy = Mockito.spy(QueryRegistryFactory.create());
        for (Class<? extends QueryBuilder> c : registeredExtras) {
            Mockito.doReturn(true).when(spy).hasTranslator(c);
        }
        return new DslCalciteGrammar(spy, aggRegistry);
    }

    // ---- source-level ----

    public void testNullSourceRejected() {
        RouteDecision decision = grammar.validate(null);
        assertFalse(decision.supported());
        assertEquals(1, decision.unsupportedFeatures().size());
        assertEquals("source:null", decision.unsupportedFeatures().get(0));
    }

    public void testEmptySourceSupported() {
        assertTrue(grammar.validate(new SearchSourceBuilder()).supported());
    }

    // ---- query walker: registry gate ----

    public void testRegisteredLeafQuerySupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("name", "laptop"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testUnregisteredLeafQueryRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchQuery("name", "laptop"));
        RouteDecision decision = grammar.validate(source);
        assertFalse(decision.supported());
        assertEquals("query:match", decision.unsupportedFeatures().get(0));
    }

    public void testBoolRecursesIntoRegisteredChildren() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("brand", "Acme"))
                .filter(QueryBuilders.existsQuery("price"))
        );
        assertTrue(grammar.validate(source).supported());
    }

    public void testBoolRejectsIfAnyChildRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("brand", "Acme"))
                .must(QueryBuilders.matchQuery("desc", "fast")) // unregistered
        );
        RouteDecision decision = grammar.validate(source);
        assertFalse(decision.supported());
        assertEquals("query:match", decision.unsupportedFeatures().get(0));
    }

    public void testConstantScoreRecurses() {
        SearchSourceBuilder ok = new SearchSourceBuilder().query(
            QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("brand", "Acme"))
        );
        assertTrue(grammar.validate(ok).supported());

        SearchSourceBuilder bad = new SearchSourceBuilder().query(
            QueryBuilders.constantScoreQuery(QueryBuilders.matchQuery("desc", "fast"))
        );
        assertFalse(grammar.validate(bad).supported());
    }

    // ---- range per-param (uses a spy since translator isn't merged yet) ----

    public void testRangeSupported() {
        DslCalciteGrammar g = grammarWith(RangeQueryBuilder.class);
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("price").gt(100).lt(500));
        assertTrue(g.validate(source).supported());
    }

    public void testRangeRejectsBoost() {
        DslCalciteGrammar g = grammarWith(RangeQueryBuilder.class);
        RangeQueryBuilder q = QueryBuilders.rangeQuery("price").gt(100).boost(2.0f);
        RouteDecision decision = g.validate(new SearchSourceBuilder().query(q));
        assertFalse(decision.supported());
        assertEquals("range.boost", decision.unsupportedFeatures().get(0));
    }

    public void testRangeRejectsName() {
        DslCalciteGrammar g = grammarWith(RangeQueryBuilder.class);
        RangeQueryBuilder q = QueryBuilders.rangeQuery("price").gt(100).queryName("my_range");
        RouteDecision decision = g.validate(new SearchSourceBuilder().query(q));
        assertFalse(decision.supported());
        assertEquals("range.name", decision.unsupportedFeatures().get(0));
    }

    public void testRangeRejectsNoBounds() {
        DslCalciteGrammar g = grammarWith(RangeQueryBuilder.class);
        RangeQueryBuilder q = QueryBuilders.rangeQuery("price");
        RouteDecision decision = g.validate(new SearchSourceBuilder().query(q));
        assertFalse(decision.supported());
        assertEquals("range.no_bounds", decision.unsupportedFeatures().get(0));
    }

    /** Covers the {@code from != null, to == null} branch of the compound bounds check. */
    public void testRangeWithOnlyLowerBoundSupported() {
        DslCalciteGrammar g = grammarWith(RangeQueryBuilder.class);
        assertTrue(g.validate(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("price").gt(100))).supported());
    }

    /** Covers the {@code from == null, to != null} branch of the compound bounds check. */
    public void testRangeWithOnlyUpperBoundSupported() {
        DslCalciteGrammar g = grammarWith(RangeQueryBuilder.class);
        assertTrue(g.validate(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("price").lt(500))).supported());
    }

    // Note: DISJOINT is rejected by RangeQueryBuilder.relation() at construction time
    // (see isRelationAllowed) — INTERSECTS/CONTAINS/WITHIN are the only reachable values,
    // and all three are accepted by the translator. So the grammar has nothing to check
    // for range.relation, and no test can produce a builder carrying DISJOINT.

    // ---- terms query per-param ----

    public void testTermsQuerySupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termsQuery("brand", "Acme", "Bravo"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testTermsRejectsBoost() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", "Acme").boost(2.0f);
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.boost", d.unsupportedFeatures().get(0));
    }

    public void testTermsRejectsName() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", "Acme").queryName("labelled");
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.name", d.unsupportedFeatures().get(0));
    }

    public void testTermsRejectsEmptyValues() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", new String[0]);
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.no_values", d.unsupportedFeatures().get(0));
    }

    public void testTermsRejectsTermsLookup() {
        TermsQueryBuilder q = new TermsQueryBuilder(
            "brand",
            new org.opensearch.indices.TermsLookup("lookup_idx", "1", "brands")
        );
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.terms_lookup", d.unsupportedFeatures().get(0));
    }

    public void testTermsRejectsNonDefaultValueType() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", "Acme").valueType(TermsQueryBuilder.ValueType.BITMAP);
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertTrue(d.unsupportedFeatures().get(0).startsWith("terms.value_type:"));
    }

    // ---- exists per-param ----

    public void testExistsSupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.existsQuery("price"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testExistsRejectsBoost() {
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(QueryBuilders.existsQuery("price").boost(3.0f)));
        assertFalse(d.supported());
        assertEquals("exists.boost", d.unsupportedFeatures().get(0));
    }

    // ---- prefix per-param (spy) ----

    public void testPrefixSupported() {
        DslCalciteGrammar g = grammarWith(PrefixQueryBuilder.class);
        assertTrue(g.validate(new SearchSourceBuilder().query(QueryBuilders.prefixQuery("name", "lap"))).supported());
    }

    public void testPrefixCaseInsensitiveSupported() {
        // Consumed by translator (folds to LOWER), not rejected.
        DslCalciteGrammar g = grammarWith(PrefixQueryBuilder.class);
        assertTrue(
            g.validate(new SearchSourceBuilder().query(QueryBuilders.prefixQuery("name", "lap").caseInsensitive(true))).supported()
        );
    }

    public void testPrefixRejectsBoost() {
        DslCalciteGrammar g = grammarWith(PrefixQueryBuilder.class);
        RouteDecision d = g.validate(new SearchSourceBuilder().query(QueryBuilders.prefixQuery("name", "lap").boost(2.0f)));
        assertFalse(d.supported());
        assertEquals("prefix.boost", d.unsupportedFeatures().get(0));
    }

    public void testPrefixRejectsRewrite() {
        DslCalciteGrammar g = grammarWith(PrefixQueryBuilder.class);
        RouteDecision d = g.validate(new SearchSourceBuilder().query(QueryBuilders.prefixQuery("name", "lap").rewrite("constant_score")));
        assertFalse(d.supported());
        assertEquals("prefix.rewrite", d.unsupportedFeatures().get(0));
    }

    // ---- wildcard per-param (spy) ----

    public void testWildcardSupported() {
        DslCalciteGrammar g = grammarWith(WildcardQueryBuilder.class);
        assertTrue(g.validate(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "lap*"))).supported());
    }

    public void testWildcardRejectsBoost() {
        DslCalciteGrammar g = grammarWith(WildcardQueryBuilder.class);
        RouteDecision d = g.validate(new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "lap*").boost(2.0f)));
        assertFalse(d.supported());
        assertEquals("wildcard.boost", d.unsupportedFeatures().get(0));
    }

    public void testWildcardRejectsRewrite() {
        DslCalciteGrammar g = grammarWith(WildcardQueryBuilder.class);
        RouteDecision d = g.validate(
            new SearchSourceBuilder().query(QueryBuilders.wildcardQuery("name", "lap*").rewrite("constant_score"))
        );
        assertFalse(d.supported());
        assertEquals("wildcard.rewrite", d.unsupportedFeatures().get(0));
    }

    // ---- aggregation walker ----

    public void testRegisteredAggSupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.avg("avg_price").field("price"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testUnregisteredAggRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.cardinality("card_brand").field("brand"));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("agg:cardinality", d.unsupportedFeatures().get(0));
    }

    public void testNestedSubAggWalked() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("by_brand").field("brand")
                    .subAggregation(AggregationBuilders.cardinality("card").field("sku")) // unregistered
            );
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("agg:cardinality", d.unsupportedFeatures().get(0));
    }

    // ---- pipeline agg ----

    public void testTopLevelPipelineAggRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.terms("by_brand").field("brand"))
            .aggregation(PipelineAggregatorBuilders.maxBucket("max_bucket", "by_brand>_count"));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertTrue(d.unsupportedFeatures().get(0).startsWith("pipeline_agg:"));
    }

    public void testNestedPipelineAggRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("by_brand").field("brand")
                    .subAggregation(AggregationBuilders.sum("sales").field("price"))
                    .subAggregation(PipelineAggregatorBuilders.cumulativeSum("cum", "sales"))
            );
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertTrue(d.unsupportedFeatures().get(0).startsWith("pipeline_agg:"));
    }

    // ---- short-circuit ----

    public void testQueryFailureShortCircuitsBeforeAggWalk() {
        // Aggs contain an unregistered agg too, but the query fails first — we should
        // only see the query reason.
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchQuery("desc", "fast"))
            .aggregation(AggregationBuilders.cardinality("card").field("sku"));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals(1, d.unsupportedFeatures().size());
        assertEquals("query:match", d.unsupportedFeatures().get(0));
    }
}
