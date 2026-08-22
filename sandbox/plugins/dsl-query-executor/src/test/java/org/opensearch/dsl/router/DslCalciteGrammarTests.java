/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.router;

import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationRegistryFactory;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryRegistryFactory;
import org.opensearch.dsl.query.QueryTranslator;
import org.opensearch.dsl.query.ValidationResult;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class DslCalciteGrammarTests extends OpenSearchTestCase {

    private final AggregationRegistry aggRegistry = AggregationRegistryFactory.create();

    /** Grammar with only the translators actually registered on this branch. */
    private final DslCalciteGrammar grammar = new DslCalciteGrammar(QueryRegistryFactory.create(), aggRegistry);

    private DslCalciteGrammar grammarWith(QueryTranslator... extraTranslators) {
        // Fresh registry (not the shared create() singleton) so registering extra translators
        // here never mutates process-wide state seen by other callers/tests.
        QueryRegistry registry = QueryRegistryFactory.newInstance();
        for (QueryTranslator translator : extraTranslators) {
            registry.register(translator);
        }
        return new DslCalciteGrammar(registry, aggRegistry);
    }

    private QueryTranslator acceptingTranslatorFor(Class<? extends QueryBuilder> queryType) {
        return new QueryTranslator() {
            @Override
            public Class<? extends QueryBuilder> getQueryType() {
                return queryType;
            }

            @Override
            public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
                throw new UnsupportedOperationException("Grammar test helper");
            }
        };
    }

    private QueryTranslator rejectingTranslatorFor(Class<? extends QueryBuilder> queryType, String reasonCode) {
        return new QueryTranslator() {
            @Override
            public Class<? extends QueryBuilder> getQueryType() {
                return queryType;
            }

            @Override
            public ValidationResult validate(QueryBuilder query) {
                return ValidationResult.rejected(reasonCode, "test rejection");
            }

            @Override
            public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
                throw new UnsupportedOperationException("Grammar test helper");
            }
        };
    }

    // ---- source-level ----

    public void testNullSourceSupported() {
        // A bodyless _search is an implicit match_all — accepted and handled by Calcite,
        // consistent with an empty "{}" body (see testEmptySourceSupported).
        assertTrue(grammar.validate(null).supported());
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
        assertEquals("query:match", decision.rejectionReasons().get(0));
    }

    public void testBoolRecursesIntoRegisteredChildren() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brand", "Acme")).filter(QueryBuilders.existsQuery("price"))
        );
        assertTrue(grammar.validate(source).supported());
    }

    public void testBoolRejectsIfAnyChildRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brand", "Acme")).must(QueryBuilders.matchQuery("desc", "fast")) // unregistered
        );
        RouteDecision decision = grammar.validate(source);
        assertFalse(decision.supported());
        assertEquals("query:match", decision.rejectionReasons().get(0));
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

    public void testDeeplyNestedUnsupportedLeafRejected() {
        // constant_score -> bool -> match: the unregistered "match" leaf sits two containers
        // deep and must still be found and rejected (structural recursion, not a hardcoded switch).
        BoolQueryBuilder inner = QueryBuilders.boolQuery();
        inner.must(QueryBuilders.termQuery("brand", "Acme"));
        inner.should(QueryBuilders.matchQuery("desc", "fast"));
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.constantScoreQuery(inner));
        RouteDecision decision = grammar.validate(source);
        assertFalse(decision.supported());
        assertEquals("query:match", decision.rejectionReasons().get(0));
    }

    public void testDeeplyNestedSupportedTreeAccepted() {
        // constant_score -> bool with only registered leaves at depth is accepted.
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.constantScoreQuery(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brand", "Acme")).filter(QueryBuilders.existsQuery("price"))
            )
        );
        assertTrue(grammar.validate(source).supported());
    }

    // ---- translator-backed leaf validation ----

    public void testRangeSupported() {
        DslCalciteGrammar g = grammarWith(acceptingTranslatorFor(RangeQueryBuilder.class));
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("price").gt(100).lt(500));
        assertTrue(g.validate(source).supported());
    }

    public void testRangeUsesTranslatorValidationReason() {
        DslCalciteGrammar g = grammarWith(rejectingTranslatorFor(RangeQueryBuilder.class, "range.boost"));
        RangeQueryBuilder q = QueryBuilders.rangeQuery("price").gt(100).boost(2.0f);
        RouteDecision decision = g.validate(new SearchSourceBuilder().query(q));
        assertFalse(decision.supported());
        assertEquals("range.boost", decision.rejectionReasons().get(0));
    }

    // ---- terms query per-param ----

    public void testTermsQuerySupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termsQuery("brand", "Acme", "Bravo"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testTermsRejectsBoost() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", "Acme").boost(2.0f);
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.boost", d.rejectionReasons().get(0));
    }

    public void testTermsRejectsName() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", "Acme").queryName("labelled");
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.name", d.rejectionReasons().get(0));
    }

    public void testTermsRejectsEmptyValues() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", new String[0]);
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.no_values", d.rejectionReasons().get(0));
    }

    public void testTermsRejectsTermsLookup() {
        TermsQueryBuilder q = new TermsQueryBuilder("brand", new org.opensearch.indices.TermsLookup("lookup_idx", "1", "brands"));
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertEquals("terms.terms_lookup", d.rejectionReasons().get(0));
    }

    public void testTermsRejectsNonDefaultValueType() {
        TermsQueryBuilder q = QueryBuilders.termsQuery("brand", "Acme").valueType(TermsQueryBuilder.ValueType.BITMAP);
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(q));
        assertFalse(d.supported());
        assertTrue(d.rejectionReasons().get(0).startsWith("terms.value_type:"));
    }

    // ---- exists per-param ----

    public void testExistsSupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.existsQuery("price"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testExistsRejectsBoost() {
        RouteDecision d = grammar.validate(new SearchSourceBuilder().query(QueryBuilders.existsQuery("price").boost(3.0f)));
        assertFalse(d.supported());
        assertEquals("exists.boost", d.rejectionReasons().get(0));
    }

    // ---- aggregation walker ----

    public void testRegisteredAggSupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.avg("avg_price").field("price"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testTopLevelBucketAggSupported() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.terms("by_brand").field("brand"));
        assertTrue(grammar.validate(source).supported());
    }

    public void testUnregisteredAggRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.cardinality("card_brand").field("brand"));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("agg:cardinality", d.rejectionReasons().get(0));
    }

    public void testNestedAggTreeRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("by_brand").field("brand").subAggregation(AggregationBuilders.cardinality("card").field("sku")) // unregistered
            );
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("agg.nested", d.rejectionReasons().get(0));
    }

    public void testNestedAggTreeRejectedEvenWhenAllAggTypesAreRegistered() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("by_brand").field("brand").subAggregation(AggregationBuilders.sum("sales").field("price"))
            );
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("agg.nested", d.rejectionReasons().get(0));
    }

    // ---- pipeline agg ----

    public void testTopLevelPipelineAggRejected() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.terms("by_brand").field("brand"))
            .aggregation(PipelineAggregatorBuilders.maxBucket("max_bucket", "by_brand>_count"));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertTrue(d.rejectionReasons().get(0).startsWith("pipeline_agg:"));
    }

    public void testNestedPipelineAggTreeRejectedBeforePipelineValidation() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("by_brand")
                    .field("brand")
                    .subAggregation(AggregationBuilders.sum("sales").field("price"))
                    .subAggregation(PipelineAggregatorBuilders.cumulativeSum("cum", "sales"))
            );
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("agg.nested", d.rejectionReasons().get(0));
    }

    // ---- short-circuit ----

    public void testQueryFailureShortCircuitsBeforeAggWalk() {
        // Aggs contain an unregistered agg too, but the query fails first — we should
        // only see the query reason.
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchQuery("desc", "fast"))
            .aggregation(AggregationBuilders.cardinality("card").field("sku"));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals(1, d.rejectionReasons().size());
        assertEquals("query:match", d.rejectionReasons().get(0));
    }
}
