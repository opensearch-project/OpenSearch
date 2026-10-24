/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.golden.GoldenFileLoader;
import org.opensearch.dsl.golden.GoldenTestCase;
import org.opensearch.dsl.golden.TestMapperServices;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SearchResponseBuilderTests extends OpenSearchTestCase {

    public void testBuildWithNoResults() throws Exception {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 42L);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(0, response.getHits().getHits().length);
        assertEquals(42L, response.getTook().millis());
        assertNull(response.getAggregations());
        assertEquals(1, response.getTotalShards());
        assertEquals(1, response.getSuccessfulShards());
    }

    public void testBuildWithEmptyRequest() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 100L);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(100L, response.getTook().millis());
        assertNull(response.getAggregations());
    }

    public void testBuildWithNullSource() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 50L);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertNull(response.getAggregations());
        assertEquals(1, response.getTotalShards());
    }

    public void testBuildWithAggregationsButNoResults() throws Exception {
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.avg("avg_price").field("price"));
        request.source(source);

        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 75L);

        assertNotNull(response);
        assertEquals(75L, response.getTook().millis());
        assertNull(response.getAggregations());
    }

    public void testShardCountsWithNoAggregations() throws Exception {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 10L);

        assertEquals(1, response.getTotalShards());
        assertEquals(1, response.getSuccessfulShards());
        assertEquals(0, response.getSkippedShards());
        assertEquals(0, response.getFailedShards());
    }

    public void testTimingPreserved() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response1 = SearchResponseBuilder.build(List.of(), request, registry, 0L);
        assertEquals(0L, response1.getTook().millis());

        SearchResponse response2 = SearchResponseBuilder.build(List.of(), request, registry, 999L);
        assertEquals(999L, response2.getTook().millis());
    }

    public void testEmptyHitsAlwaysReturned() throws Exception {
        SearchRequest request = new SearchRequest();
        AggregationRegistry registry = new AggregationRegistry();

        SearchResponse response = SearchResponseBuilder.build(List.of(), request, registry, 10L);

        assertNotNull(response.getHits());
        assertEquals(0, response.getHits().getHits().length);
        assertNotNull(response.getHits().getTotalHits());
    }

    /**
     * Regression: granularity keys must match even when the schema's column order opposes
     * the request's nesting order (schema declares category before brand; request nests
     * brand → category). Before key canonicalization this returned empty aggregations.
     */
    public void testNestedGroupFieldsWithOpposingSchemaOrder() throws Exception {
        Map<String, String> mapping = new java.util.LinkedHashMap<>();
        mapping.put("category", "VARCHAR"); // lower column index than brand — the crux
        mapping.put("brand", "VARCHAR");
        mapping.put("price", "INTEGER");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);

        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("by_brand")
                    .field("brand")
                    .subAggregation(
                        AggregationBuilders.terms("by_category")
                            .field("category")
                            .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                    )
            );

        SearchSourceConverter converter = new SearchSourceConverter(infra.schema(), TestMapperServices.fromSqlMapping(mapping));
        QueryPlans plans = converter.convert(source, "products");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());

        List<ExecutionResult> results = new ArrayList<>();
        for (QueryPlans.QueryPlan plan : aggPlans) {
            List<String> fields = plan.relNode().getRowType().getFieldNames();
            if (fields.contains("avg_price")) {
                // Group columns arrive in schema order (category first) despite brand-first
                // nesting; the per-parent eligible total rides the bounded child plan's rows.
                assertEquals(List.of("category", "brand", "avg_price", "_count", "_parent_eligible"), fields);
                results.add(
                    new ExecutionResult(
                        plan,
                        List.of(new Object[] { "Cat1", "BrandA", 850.0, 2L, 5L }, new Object[] { "Cat2", "BrandA", 700.0, 1L, 5L })
                    )
                );
            } else {
                assertEquals(List.of("brand", "_count"), fields);
                results.add(new ExecutionResult(plan, List.<Object[]>of(new Object[] { "BrandA", 3L })));
            }
        }
        addCountResult(plans, results, Map.of(QueryPlans.COUNT_TOTAL_COLUMN, 3, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand", 3));

        SearchRequest request = new SearchRequest("products");
        request.source(source);
        SearchResponse response = SearchResponseBuilder.build(results, request, converter.getAggregationRegistry(), 1L);

        StringTerms byBrand = response.getAggregations().get("by_brand");
        assertNotNull("by_brand must be present", byBrand);
        assertEquals(1, byBrand.getBuckets().size());
        assertEquals("BrandA", byBrand.getBuckets().get(0).getKeyAsString());
        assertEquals(3L, byBrand.getBuckets().get(0).getDocCount());

        StringTerms byCategory = byBrand.getBuckets().get(0).getAggregations().get("by_category");
        assertNotNull("by_category must be present inside the brand bucket", byCategory);
        assertEquals(2, byCategory.getBuckets().size());
        assertEquals("Cat1", byCategory.getBuckets().get(0).getKeyAsString());
        assertEquals(2L, byCategory.getBuckets().get(0).getDocCount());
        InternalAvg avg1 = byCategory.getBuckets().get(0).getAggregations().get("avg_price");
        assertEquals(850.0, avg1.getValue(), 0.0);
        InternalAvg avg2 = byCategory.getBuckets().get(1).getAggregations().get("avg_price");
        assertEquals(700.0, avg2.getValue(), 0.0);
        // per-parent eligible total 5 vs 3 rendered docs → 2 in truncated child groups
        assertEquals(2L, byCategory.getSumOfOtherDocCounts());
    }

    /**
     * Regression: sibling aggregation trees over the same field SET but opposite nesting order
     * (brand→category vs category→brand) produce two distinct plans and must resolve to two
     * distinct results. Under the old order-insensitive (sorted) granularity keys, both deep
     * plans collapsed onto one map slot: the second plan's result silently overwrote the
     * first's, and the losing tree's metric came back empty ({@code "value": null}) beneath
     * correct-looking buckets.
     */
    public void testReversedNestingSiblingTreesDoNotCollide() throws Exception {
        Map<String, String> mapping = new java.util.LinkedHashMap<>();
        mapping.put("brand", "VARCHAR");
        mapping.put("category", "VARCHAR");
        mapping.put("price", "INTEGER");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);
        Supplier<MapperService> mappers = TestMapperServices.fromSqlMapping(mapping);

        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("brand_first")
                    .field("brand")
                    .subAggregation(
                        AggregationBuilders.terms("by_category")
                            .field("category")
                            .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                    )
            )
            .aggregation(
                AggregationBuilders.terms("cat_first")
                    .field("category")
                    .subAggregation(
                        AggregationBuilders.terms("by_brand")
                            .field("brand")
                            .subAggregation(AggregationBuilders.sum("sum_price").field("price"))
                    )
            );

        SearchSourceConverter converter = new SearchSourceConverter(infra.schema(), mappers);
        QueryPlans plans = converter.convert(source, "products");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(4, aggPlans.size());

        // Data story: 3 docs — BrandA/Cat1/800, BrandA/Cat1/900, BrandA/Cat2/700.
        List<ExecutionResult> results = new ArrayList<>();
        for (QueryPlans.QueryPlan plan : aggPlans) {
            List<String> fields = plan.relNode().getRowType().getFieldNames();
            if (fields.contains("avg_price")) {
                // Both deep plans group by {brand, category} and emit columns in schema order —
                // identical layouts, distinguished ONLY by the metadata's aggregation-name path.
                assertEquals(List.of("brand", "category", "avg_price", "_count", "_parent_eligible"), fields);
                results.add(
                    new ExecutionResult(
                        plan,
                        List.of(new Object[] { "BrandA", "Cat1", 850.0, 2L, 3L }, new Object[] { "BrandA", "Cat2", 700.0, 1L, 3L })
                    )
                );
            } else if (fields.contains("sum_price")) {
                // cat_first tree: the parent is category, so each row carries its category's total
                assertEquals(List.of("brand", "category", "sum_price", "_count", "_parent_eligible"), fields);
                results.add(
                    new ExecutionResult(
                        plan,
                        List.of(new Object[] { "BrandA", "Cat1", 1700.0, 2L, 2L }, new Object[] { "BrandA", "Cat2", 700.0, 1L, 1L })
                    )
                );
            } else if (fields.equals(List.of("brand", "_count"))) {
                results.add(new ExecutionResult(plan, List.<Object[]>of(new Object[] { "BrandA", 3L })));
            } else {
                assertEquals(List.of("category", "_count"), fields);
                results.add(new ExecutionResult(plan, List.of(new Object[] { "Cat1", 2L }, new Object[] { "Cat2", 1L })));
            }
        }
        addCountResult(
            plans,
            results,
            Map.of(
                QueryPlans.COUNT_TOTAL_COLUMN,
                3,
                QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "brand_first",
                3,
                QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "cat_first",
                3
            )
        );

        SearchRequest request = new SearchRequest("products");
        request.source(source);
        SearchResponse response = SearchResponseBuilder.build(results, request, converter.getAggregationRegistry(), 1L);

        // brand_first → BrandA → by_category → Cat1 (avg 850), Cat2 (avg 700)
        StringTerms brandFirst = response.getAggregations().get("brand_first");
        assertNotNull("brand_first must be present", brandFirst);
        assertEquals(1, brandFirst.getBuckets().size());
        StringTerms byCategory = brandFirst.getBuckets().get(0).getAggregations().get("by_category");
        assertEquals(2, byCategory.getBuckets().size());
        InternalAvg avgCat1 = byCategory.getBuckets().get(0).getAggregations().get("avg_price");
        assertEquals("brand_first's metric must not be lost to the sibling tree", 850.0, avgCat1.getValue(), 0.0);
        InternalAvg avgCat2 = byCategory.getBuckets().get(1).getAggregations().get("avg_price");
        assertEquals(700.0, avgCat2.getValue(), 0.0);

        // cat_first → Cat1 → by_brand → BrandA (sum 1700); Cat2 → by_brand → BrandA (sum 700)
        StringTerms catFirst = response.getAggregations().get("cat_first");
        assertNotNull("cat_first must be present", catFirst);
        assertEquals(2, catFirst.getBuckets().size());
        StringTerms byBrandCat1 = catFirst.getBuckets().get(0).getAggregations().get("by_brand");
        InternalSum sumCat1 = byBrandCat1.getBuckets().get(0).getAggregations().get("sum_price");
        assertEquals(1700.0, sumCat1.getValue(), 0.0);
        StringTerms byBrandCat2 = catFirst.getBuckets().get(1).getAggregations().get("by_brand");
        InternalSum sumCat2 = byBrandCat2.getBuckets().get(0).getAggregations().get("sum_price");
        assertEquals(700.0, sumCat2.getValue(), 0.0);
    }

    /**
     * Terms {@code size} is enforced by the plan's LIMIT — the engine returns only the top
     * bucket — and {@code sum_other_doc_count} comes from the count plan's eligible count
     * ({@code eligible − Σ rendered}), since the tail never leaves the engine.
     */
    public void testTermsSizeTruncation() throws Exception {
        Map<String, String> mapping = new java.util.LinkedHashMap<>();
        mapping.put("brand", "VARCHAR");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);

        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(AggregationBuilders.terms("by_brand").field("brand").size(1));

        SearchSourceConverter converter = new SearchSourceConverter(infra.schema(), TestMapperServices.fromSqlMapping(mapping));
        QueryPlans plans = converter.convert(source, "products");

        List<ExecutionResult> results = new ArrayList<>();
        for (QueryPlans.QueryPlan plan : plans.get(QueryPlans.Type.AGGREGATION)) {
            assertEquals(List.of("brand", "_count"), plan.relNode().getRowType().getFieldNames());
            assertTrue("plan must be bounded to size", plan.relNode().explain().contains("fetch=[1]"));
            // the engine honors the LIMIT: only the top bucket comes back
            results.add(new ExecutionResult(plan, List.<Object[]>of(new Object[] { "BrandA", 3L })));
        }
        addCountResult(plans, results, Map.of(QueryPlans.COUNT_TOTAL_COLUMN, 5, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand", 5));

        SearchRequest request = new SearchRequest("products");
        request.source(source);
        SearchResponse response = SearchResponseBuilder.build(results, request, converter.getAggregationRegistry(), 1L);

        StringTerms byBrand = response.getAggregations().get("by_brand");
        assertEquals(1, byBrand.getBuckets().size());
        assertEquals("BrandA", byBrand.getBuckets().get(0).getKeyAsString());
        assertEquals(3L, byBrand.getBuckets().get(0).getDocCount());
        assertEquals(2L, byBrand.getSumOfOtherDocCounts());
    }

    /** A result table missing a requested metric's column is a broken invariant — throw, don't render {@code "value": null}. */
    public void testMissingMetricColumnThrows() throws Exception {
        Map<String, String> mapping = new java.util.LinkedHashMap<>();
        mapping.put("brand", "VARCHAR");
        mapping.put("price", "INTEGER");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);

        // Results computed for a metric named "other"
        SearchSourceBuilder executedSource = new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.avg("other").field("price"));
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(executedSource, "products");

        List<ExecutionResult> results = new ArrayList<>();
        for (QueryPlans.QueryPlan plan : plans.get(QueryPlans.Type.AGGREGATION)) {
            int columnCount = plan.relNode().getRowType().getFieldCount();
            results.add(new ExecutionResult(plan, List.<Object[]>of(new Object[columnCount])));
        }

        // Response requested for "avg_price" — same granularity, but no such column in the result
        SearchRequest request = new SearchRequest("products");
        request.source(new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.avg("avg_price").field("price")));

        expectThrows(
            ConversionException.class,
            () -> SearchResponseBuilder.build(results, request, converter.getAggregationRegistry(), 1L)
        );
    }

    /**
     * User-supplied {@code meta} on an aggregation request must be echoed back verbatim on the
     * corresponding response aggregation — classic search parity — for both bucket and metric
     * aggregations, through the full plan/response round trip.
     */
    public void testUserMetaEchoedInAggregations() throws Exception {
        Map<String, String> mapping = new java.util.LinkedHashMap<>();
        mapping.put("brand", "VARCHAR");
        mapping.put("price", "INTEGER");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);

        Map<String, Object> termsMeta = Map.of("source", "dashboard");
        Map<String, Object> avgMeta = Map.of("owner", "pricing-team");

        SearchSourceBuilder source = new SearchSourceBuilder().size(0)
            .aggregation(
                AggregationBuilders.terms("by_brand")
                    .field("brand")
                    .setMetadata(termsMeta)
                    .subAggregation(AggregationBuilders.avg("avg_price").field("price").setMetadata(avgMeta))
            );

        SearchSourceConverter converter = new SearchSourceConverter(infra.schema(), TestMapperServices.fromSqlMapping(mapping));
        QueryPlans plans = converter.convert(source, "products");

        List<ExecutionResult> results = new ArrayList<>();
        for (QueryPlans.QueryPlan plan : plans.get(QueryPlans.Type.AGGREGATION)) {
            List<String> fields = plan.relNode().getRowType().getFieldNames();
            Object[] row = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                if ("brand".equals(fields.get(i))) {
                    row[i] = "BrandA";
                } else if ("avg_price".equals(fields.get(i))) {
                    row[i] = 850.0;
                } else if ("_count".equals(fields.get(i))) {
                    row[i] = 3L;
                } else {
                    fail("Unexpected column: " + fields.get(i));
                }
            }
            results.add(new ExecutionResult(plan, List.<Object[]>of(row)));
        }
        addCountResult(plans, results, Map.of(QueryPlans.COUNT_TOTAL_COLUMN, 3, QueryPlans.COUNT_ELIGIBLE_COLUMN_PREFIX + "by_brand", 3));

        SearchRequest request = new SearchRequest("products");
        request.source(source);
        SearchResponse response = SearchResponseBuilder.build(results, request, converter.getAggregationRegistry(), 1L);

        StringTerms byBrand = response.getAggregations().get("by_brand");
        assertNotNull(byBrand);
        assertEquals(termsMeta, byBrand.getMetadata());

        InternalAvg avg = byBrand.getBuckets().get(0).getAggregations().get("avg_price");
        assertNotNull(avg);
        assertEquals(avgMeta, avg.getMetadata());
        assertEquals(850.0, avg.getValue(), 0.0);
    }

    // ---- hits.total: classic track_total_hits semantics from the COUNT plan ----

    public void testHitsTotalExactUnderDefaultThreshold() throws Exception {
        SearchResponse response = countOnlyResponse(new SearchSourceBuilder().size(0), 42L);

        assertEquals(42L, response.getHits().getTotalHits().value());
        assertEquals(TotalHits.Relation.EQUAL_TO, response.getHits().getTotalHits().relation());
    }

    public void testHitsTotalLowerBoundOverDefaultThreshold() throws Exception {
        SearchResponse response = countOnlyResponse(new SearchSourceBuilder().size(0), 25_000L);

        assertEquals(10_000L, response.getHits().getTotalHits().value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, response.getHits().getTotalHits().relation());
    }

    public void testHitsTotalExactWhenTrackTotalHitsTrue() throws Exception {
        SearchResponse response = countOnlyResponse(new SearchSourceBuilder().size(0).trackTotalHits(true), 25_000L);

        assertEquals(25_000L, response.getHits().getTotalHits().value());
        assertEquals(TotalHits.Relation.EQUAL_TO, response.getHits().getTotalHits().relation());
    }

    public void testHitsTotalOmittedWhenTrackTotalHitsFalse() throws Exception {
        Map<String, String> mapping = new java.util.LinkedHashMap<>();
        mapping.put("brand", "VARCHAR");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);

        SearchSourceBuilder source = new SearchSourceBuilder().size(0).trackTotalHits(false);
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(source, "products");
        assertTrue("nothing to count, nothing to run", plans.getAll().isEmpty());

        SearchRequest request = new SearchRequest("products");
        request.source(source);
        SearchResponse response = SearchResponseBuilder.build(List.of(), request, converter.getAggregationRegistry(), 1L);

        assertNull(response.getHits().getTotalHits());
    }

    /** Runs a count-only request (size 0, no aggs) with a fabricated engine count. */
    private SearchResponse countOnlyResponse(SearchSourceBuilder source, long total) throws Exception {
        Map<String, String> mapping = new java.util.LinkedHashMap<>();
        mapping.put("brand", "VARCHAR");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping("products", mapping);

        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(source, "products");

        List<ExecutionResult> results = new ArrayList<>();
        addCountResult(plans, results, Map.of(QueryPlans.COUNT_TOTAL_COLUMN, total));

        SearchRequest request = new SearchRequest("products");
        request.source(source);
        return SearchResponseBuilder.build(results, request, converter.getAggregationRegistry(), 1L);
    }

    // ---- Golden file driven SearchResponse generation tests ----

    /**
     * Auto-discovers all golden JSON files and validates that mock execution
     * rows produce the expected SearchResponse JSON via SearchResponseBuilder.build().
     */
    public void testGoldenFileSearchResponseGeneration() throws Exception {
        URL goldenDir = getClass().getClassLoader().getResource("golden");
        assertNotNull("Golden file resource directory not found", goldenDir);

        List<Path> goldenFiles;
        try (var stream = Files.list(Path.of(goldenDir.toURI()))) {
            goldenFiles = stream.filter(p -> p.toString().endsWith(".json")).collect(Collectors.toList());
        }
        assertFalse("No golden files found", goldenFiles.isEmpty());

        List<String> failures = new ArrayList<>();
        for (Path file : goldenFiles) {
            String fileName = file.getFileName().toString();
            try {
                GoldenTestCase tc = GoldenFileLoader.load(fileName);
                CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

                // Build QueryPlan via forward path (needed to construct ExecutionResult)
                SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
                SearchSourceConverter converter = new SearchSourceConverter(
                    infra.schema(),
                    TestMapperServices.fromSqlMapping(tc.getIndexMapping())
                );
                QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

                QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
                List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
                if (matchingPlans.isEmpty()) {
                    failures.add(fileName + ": No " + expectedType + " plan produced");
                    continue;
                }

                // Build ExecutionResult from mock rows
                List<Object[]> rows = new ArrayList<>();
                for (List<Object> row : tc.getMockResultRows()) {
                    rows.add(row.toArray());
                }
                List<ExecutionResult> allResults = new ArrayList<>();
                allResults.add(new ExecutionResult(matchingPlans.get(0), rows));
                if (tc.getMockCountRow() != null) {
                    addCountResult(plans, allResults, tc.getMockCountRow());
                }

                // Build and serialize SearchResponse
                SearchRequest searchRequest = new SearchRequest(tc.getIndexName());
                searchRequest.source(searchSource);
                SearchResponse response = SearchResponseBuilder.build(allResults, searchRequest, converter.getAggregationRegistry(), 0L);
                String responseJson = Strings.toString(MediaTypeRegistry.JSON, response);

                Map<String, Object> actualOutput = XContentHelper.convertToMap(JsonXContent.jsonXContent, responseJson, false);

                // Deep copy expected to avoid mutating GoldenTestCase
                String expectedJson;
                try (var builder = JsonXContent.contentBuilder()) {
                    builder.map(tc.getExpectedOutputDsl());
                    expectedJson = builder.toString();
                }
                Map<String, Object> expectedOutput = XContentHelper.convertToMap(JsonXContent.jsonXContent, expectedJson, false);

                stripNonDeterministicFields(actualOutput);
                stripNonDeterministicFields(expectedOutput);

                if ("AGGREGATION".equals(tc.getPlanType())) {
                    normalizeAggregationBuckets(actualOutput);
                    normalizeAggregationBuckets(expectedOutput);
                }

                if (!expectedOutput.equals(actualOutput)) {
                    String expectedPretty, actualPretty;
                    try (var b = JsonXContent.contentBuilder().prettyPrint()) {
                        b.map(expectedOutput);
                        expectedPretty = b.toString();
                    }
                    try (var b = JsonXContent.contentBuilder().prettyPrint()) {
                        b.map(actualOutput);
                        actualPretty = b.toString();
                    }
                    failures.add(fileName + ": SearchResponse mismatch\n  Expected: " + expectedPretty + "\n  Actual:   " + actualPretty);
                }
            } catch (Exception e) {
                failures.add(fileName + ": " + e.getClass().getSimpleName() + " - " + e.getMessage());
            }
        }

        if (!failures.isEmpty()) {
            fail("Golden file SearchResponse generation failures:\n" + String.join("\n", failures));
        }
    }

    // ---- Helpers ----

    /**
     * Fabricates the COUNT plan's single-row result from a columnName→value map, mirroring
     * what the engine would return for it. Fails the test if the plan carries a column the
     * map does not provide.
     */
    private static void addCountResult(QueryPlans plans, List<ExecutionResult> results, Map<String, Object> countsByColumn) {
        for (QueryPlans.QueryPlan plan : plans.get(QueryPlans.Type.COUNT)) {
            List<String> fields = plan.relNode().getRowType().getFieldNames();
            Object[] row = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                Object value = countsByColumn.get(fields.get(i));
                assertNotNull("count fabrication missing column: " + fields.get(i), value);
                row[i] = value instanceof Number n ? n.longValue() : value;
            }
            results.add(new ExecutionResult(plan, List.<Object[]>of(row)));
        }
    }

    private SearchSourceBuilder parseSearchSource(Map<String, Object> inputDsl) throws IOException {
        String json;
        try (var builder = JsonXContent.contentBuilder()) {
            builder.map(inputDsl);
            json = builder.toString();
        }
        NamedXContentRegistry registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents()
        );
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(registry, DeprecationHandler.IGNORE_DEPRECATIONS, json)) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    @SuppressWarnings("unchecked")
    private void stripNonDeterministicFields(Map<String, Object> responseMap) {
        responseMap.remove("took");
        responseMap.remove("timed_out");
        responseMap.remove("_shards");
    }

    @SuppressWarnings("unchecked")
    private void normalizeAggregationBuckets(Map<String, Object> map) {
        Object aggs = map.get("aggregations");
        if (aggs instanceof Map) {
            normalizeBucketsRecursive((Map<String, Object>) aggs);
        }
    }

    /** Recursively sorts aggregation bucket lists by key for order-insensitive comparison. */
    @SuppressWarnings("unchecked")
    private void normalizeBucketsRecursive(Map<String, Object> aggMap) {
        for (Map.Entry<String, Object> entry : aggMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                Map<String, Object> aggBody = (Map<String, Object>) value;
                Object buckets = aggBody.get("buckets");
                if (buckets instanceof List) {
                    List<Map<String, Object>> bucketList = (List<Map<String, Object>>) buckets;
                    bucketList.sort(Comparator.comparing(b -> String.valueOf(b.get("key"))));
                    for (Map<String, Object> bucket : bucketList) {
                        for (Map.Entry<String, Object> bucketEntry : bucket.entrySet()) {
                            if (bucketEntry.getValue() instanceof Map) {
                                Map<String, Object> subAgg = (Map<String, Object>) bucketEntry.getValue();
                                if (subAgg.containsKey("buckets")) {
                                    normalizeBucketsRecursive(Map.of(bucketEntry.getKey(), subAgg));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
