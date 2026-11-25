/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.converter;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * The unsupported-parameter allow-list: requests using features this path does not implement
 * are rejected with a single {@link ConversionException} (mapped to HTTP 400 by the transport)
 * naming every offending parameter, instead of silently ignoring them.
 */
public class UnsupportedSearchParametersTests extends OpenSearchTestCase {

    private SearchSourceConverter converter() {
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("brand", "VARCHAR");
        mapping.put("price", "INTEGER");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);
        return new SearchSourceConverter(infra.schema());
    }

    /** Every rejected parameter, table-driven: each must 400 with its REST name in the message. */
    public void testEachRejectedParameter() {
        Map<String, Consumer<SearchSourceBuilder>> params = new LinkedHashMap<>();
        params.put("post_filter", s -> s.postFilter(QueryBuilders.termQuery("brand", "a")));
        params.put("explain", s -> s.explain(true));
        params.put("version", s -> s.version(true));
        params.put("seq_no_primary_term", s -> s.seqNoAndPrimaryTerm(true));
        params.put("include_named_queries_score", s -> s.includeNamedQueriesScores(true));
        params.put("search_after", s -> s.searchAfter(new Object[] { 42 }));
        params.put("slice", s -> s.slice(new SliceBuilder(0, 2)));
        params.put("min_score", s -> s.minScore(0.5f));
        params.put("terminate_after", s -> s.terminateAfter(100));
        params.put("stored_fields", s -> s.storedField("brand"));
        params.put("docvalue_fields", s -> s.docValueField("price"));
        params.put("script_fields", s -> s.scriptField("x", new Script("doc['price'].value")));
        params.put("derived", s -> s.derivedField("d", "keyword", new Script("emit('x')")));
        params.put("fields", s -> s.fetchField("brand"));
        params.put("highlight", s -> s.highlighter(new HighlightBuilder().field("brand")));
        params.put("suggest", s -> s.suggest(new SuggestBuilder()));
        params.put("rescore", s -> s.addRescorer(new QueryRescorerBuilder(QueryBuilders.termQuery("brand", "a"))));
        params.put("indices_boost", s -> s.indexBoost("products", 2.0f));
        params.put("profile", s -> s.profile(true));
        params.put("collapse", s -> s.collapse(new CollapseBuilder("brand")));
        params.put("pit", s -> s.pointInTimeBuilder(new PointInTimeBuilder("pit-id")));
        params.put("search_pipeline", s -> s.pipeline("my-pipeline"));
        params.put("verbose_pipeline", s -> {
            s.pipeline("my-pipeline");
            s.verbosePipeline(true);
        });

        for (Map.Entry<String, Consumer<SearchSourceBuilder>> e : params.entrySet()) {
            SearchSourceBuilder source = new SearchSourceBuilder();
            e.getValue().accept(source);
            ConversionException ex = expectThrows(ConversionException.class, () -> converter().convert(source, "products"));
            assertTrue(
                "[" + e.getKey() + "] must be named in: " + ex.getMessage(),
                ex.getMessage().contains("not supported") && ex.getMessage().contains(e.getKey())
            );
        }
    }

    // ── Aggregate behaviors ───────────────────────────────────────────────

    /** All offenders are reported in one message, not first-fail. */
    public void testAllOffendersListedTogether() {
        SearchSourceBuilder source = new SearchSourceBuilder().highlighter(new HighlightBuilder().field("brand"))
            .searchAfter(new Object[] { 1 })
            .minScore(1.0f);
        ConversionException e = expectThrows(ConversionException.class, () -> converter().convert(source, "products"));
        assertTrue(e.getMessage().contains("highlight"));
        assertTrue(e.getMessage().contains("search_after"));
        assertTrue(e.getMessage().contains("min_score"));
    }

    /** A request using the full supported surface converts without rejection. */
    public void testFullySupportedRequestPasses() throws Exception {
        // Note: the sort field must survive the _source projection (the conversion pipeline
        // projects before sorting — pre-existing behavior, independent of this check).
        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.termQuery("brand", "a"))
            .from(5)
            .size(10)
            .sort("price", SortOrder.DESC)
            .fetchSource(new FetchSourceContext(true, new String[] { "brand", "price" }, null))
            .aggregation(AggregationBuilders.terms("b").field("brand"))
            .trackTotalHitsUpTo(1000);
        assertNotNull(converter().convert(source, "products"));
    }

    /** Deliberately ignored hints do not reject (documented in UnsupportedSearchParameters). */
    public void testIgnoredHintsPass() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().timeout(TimeValue.timeValueSeconds(30))
            .trackScores(true)
            .stats(List.of("group1"));
        assertNotNull(converter().convert(source, "products"));
    }

    /** Explicit false on flag parameters is not a use of the feature. */
    public void testExplicitFalseFlagsPass() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().explain(false).version(false).seqNoAndPrimaryTerm(false).profile(false);
        assertNotNull(converter().convert(source, "products"));
    }

    // ── The allow-list enforcement ────────────────────────────────────────

    /**
     * Every zero-arg public getter on {@code SearchSourceBuilder} must be classified:
     * supported (converted), rejected ({@code UnsupportedSearchParameters}), deliberately
     * ignored, or a non-feature utility. When core adds a new search feature this test fails,
     * forcing a decision instead of letting the new parameter be silently ignored in
     * production. (Public-API reflection only — forbidden-apis bans getDeclaredFields.)
     */
    public void testEverySearchSourceGetterIsClassified() {
        Set<String> supported = Set.of("query", "from", "size", "sorts", "fetchSource", "aggregations", "trackTotalHitsUpTo");
        Set<String> rejected = Set.of(
            "postFilter",
            "explain",
            "version",
            "seqNoAndPrimaryTerm",
            "includeNamedQueriesScore",
            "searchAfter",
            "slice",
            "minScore",
            "terminateAfter",
            "storedFields",
            "docValueFields",
            "scriptFields",
            "getDerivedFieldsObject",
            "getDerivedFields",
            "fetchFields",
            "highlighter",
            "suggest",
            "rescores",
            "indexBoosts",
            "ext",
            "profile",
            "collapse",
            "pointInTimeBuilder",
            "searchPipelineSource",
            "pipeline",
            "verbosePipeline"
        );
        Set<String> ignored = Set.of("timeout", "trackScores", "stats");
        // Not search features: object plumbing, serialization, derived views of other getters.
        Set<String> utility = Set.of("toString", "hashCode", "shallowCopy", "isSuggestOnly", "toXContent", "clearRescorers");
        List<String> unclassified = new ArrayList<>();

        for (java.lang.reflect.Method method : SearchSourceBuilder.class.getMethods()) {
            if (method.getDeclaringClass() != SearchSourceBuilder.class
                || method.getParameterCount() != 0
                || Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            String name = method.getName();
            if (!supported.contains(name) && !rejected.contains(name) && !ignored.contains(name) && !utility.contains(name)) {
                unclassified.add(name);
            }
        }
        assertTrue(
            "SearchSourceBuilder getters "
                + unclassified
                + " are not classified as supported/rejected/ignored/utility. "
                + "Decide how this search path treats each feature: convert it, reject it in "
                + "UnsupportedSearchParameters, or document why ignoring it is safe - "
                + "then add it to the matching set in this test.",
            unclassified.isEmpty()
        );
    }
}
