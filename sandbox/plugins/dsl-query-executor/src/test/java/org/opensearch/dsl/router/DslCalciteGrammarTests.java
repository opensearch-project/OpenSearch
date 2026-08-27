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
            public ValidationResult validate(QueryBuilder query) {
                return ValidationResult.accepted();
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

    public void testBoolRejectedWithoutTranslator() {
        // Invariant: supported <=> has a translator. bool has none on this branch (pending its own
        // BoolQueryTranslator), so a bool is rejected at its own node — before its children are seen.
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brand", "Acme"))
        );
        RouteDecision decision = grammar.validate(source);
        assertFalse(decision.supported());
        assertEquals("query:bool", decision.rejectionReasons().get(0));
    }

    public void testWalkDescendsIntoCompoundChildren() {
        // A stub stands in for a compound translator (bool gets a real one in #22604): once the
        // compound node is accepted, QueryBuilder#visit descends and the unsupported "match" child is
        // found and rejected. Asserting a rejection proves the descent without implying bool is routable.
        DslCalciteGrammar g = grammarWith(acceptingTranslatorFor(BoolQueryBuilder.class));
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brand", "Acme")).must(QueryBuilders.matchQuery("desc", "fast")) // unregistered
        );
        RouteDecision decision = g.validate(source);
        assertFalse(decision.supported());
        assertEquals("query:match", decision.rejectionReasons().get(0));
    }

    public void testConstantScoreRejected() {
        // constant_score has no handler (scoring not supported on the RelNode path) → codec,
        // regardless of whether its inner query would be supported.
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("brand", "Acme"))
        );
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("query:constant_score", d.rejectionReasons().get(0));
    }

    public void testDeeplyNestedUnsupportedLeafRejected() {
        // bool -> bool -> match: with a stub bool translator so the walk descends, the unregistered
        // "match" leaf two bool levels deep is still found and rejected (QueryBuilder#visit descends).
        DslCalciteGrammar g = grammarWith(acceptingTranslatorFor(BoolQueryBuilder.class));
        BoolQueryBuilder inner = QueryBuilders.boolQuery();
        inner.must(QueryBuilders.termQuery("brand", "Acme"));
        inner.should(QueryBuilders.matchQuery("desc", "fast"));
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.boolQuery().must(inner));
        RouteDecision decision = g.validate(source);
        assertFalse(decision.supported());
        assertEquals("query:match", decision.rejectionReasons().get(0));
    }

    // ---- reject-unless-validated ----

    public void testRangeRejectedUntilItValidates() {
        // Reject-unless-supported: RangeQueryTranslator has no validate() override yet, so the
        // reject-by-default contract routes range to codec until range validation is implemented.
        RouteDecision decision = grammar.validate(new SearchSourceBuilder().query(QueryBuilders.rangeQuery("price").gt(100)));
        assertFalse(decision.supported());
        assertEquals("range.unvalidated", decision.rejectionReasons().get(0));
    }

    public void testCompoundTranslatorParamRejectionHonored() {
        // A compound's own translator.validate() runs on the node before the walk descends; when it
        // rejects, first-failing-node-wins surfaces the compound's reason, not a child's. (Stub stands
        // in for a compound translator such as bool's, arriving in #22604.)
        DslCalciteGrammar g = grammarWith(rejectingTranslatorFor(BoolQueryBuilder.class, "bool.minimum_should_match"));
        SearchSourceBuilder source = new SearchSourceBuilder().query(
            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brand", "Acme")).minimumShouldMatch(1)
        );
        RouteDecision decision = g.validate(source);
        assertFalse(decision.supported());
        assertEquals("bool.minimum_should_match", decision.rejectionReasons().get(0));
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
        assertEquals("terms._name", d.rejectionReasons().get(0));
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

    public void testMetricAggUnsupportedParamRejected() {
        // 'missing' is unsupported on metric aggs; the grammar runs the same validate() the converter
        // does and rejects up front (to codec) with the translator's per-parameter reason code.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.avg("avg_price").field("price").missing(0));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("avg.missing", d.rejectionReasons().get(0));
    }

    public void testBucketAggUnsupportedParamRejected() {
        // min_doc_count:0 is unsupported on terms (zero-count buckets need the term dictionary);
        // rejected up front to codec with the translator's reason code.
        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.terms("by_brand").field("brand").minDocCount(0));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("terms.min_doc_count", d.rejectionReasons().get(0));
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

    // ---- top-level field gating ----

    public void testSupportedScalarTopLevelFieldsAccepted() {
        SearchSourceBuilder source = new SearchSourceBuilder().size(10).from(5).trackTotalHits(true);
        assertTrue(grammar.validate(source).supported());
    }

    public void testUnsupportedTopLevelFieldRejected() {
        // post_filter has no handler yet → reject-unless-supported routes it to codec.
        SearchSourceBuilder source = new SearchSourceBuilder().postFilter(QueryBuilders.termQuery("brand", "Acme"));
        RouteDecision d = grammar.validate(source);
        assertFalse(d.supported());
        assertEquals("source.post_filter", d.rejectionReasons().get(0));
    }

    public void testSortRejectedUntilSupported() {
        // sort has no handler yet (visitSort pending) → routed to codec.
        RouteDecision d = grammar.validate(new SearchSourceBuilder().sort("price"));
        assertFalse(d.supported());
        assertEquals("source.sort", d.rejectionReasons().get(0));
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
