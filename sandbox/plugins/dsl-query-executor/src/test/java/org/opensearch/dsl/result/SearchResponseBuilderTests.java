/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

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
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.golden.GoldenFileLoader;
import org.opensearch.dsl.golden.GoldenTestCase;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.metrics.InternalAvg;
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

        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(source, "products");
        List<QueryPlans.QueryPlan> aggPlans = plans.get(QueryPlans.Type.AGGREGATION);
        assertEquals(2, aggPlans.size());

        List<ExecutionResult> results = new ArrayList<>();
        for (QueryPlans.QueryPlan plan : aggPlans) {
            List<String> fields = plan.relNode().getRowType().getFieldNames();
            if (fields.contains("avg_price")) {
                // Group columns arrive in schema order (category first) despite brand-first nesting.
                assertEquals(List.of("category", "brand", "avg_price", "_count"), fields);
                results.add(
                    new ExecutionResult(
                        plan,
                        List.of(new Object[] { "Cat1", "BrandA", 850.0, 2L }, new Object[] { "Cat2", "BrandA", 700.0, 1L })
                    )
                );
            } else {
                assertEquals(List.of("brand", "_count"), fields);
                results.add(new ExecutionResult(plan, List.<Object[]>of(new Object[] { "BrandA", 3L })));
            }
        }

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
                SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
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
                ExecutionResult result = new ExecutionResult(matchingPlans.get(0), rows);

                // Build and serialize SearchResponse
                SearchRequest searchRequest = new SearchRequest(tc.getIndexName());
                searchRequest.source(searchSource);
                SearchResponse response = SearchResponseBuilder.build(
                    List.of(result),
                    searchRequest,
                    converter.getAggregationRegistry(),
                    0L
                );
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
