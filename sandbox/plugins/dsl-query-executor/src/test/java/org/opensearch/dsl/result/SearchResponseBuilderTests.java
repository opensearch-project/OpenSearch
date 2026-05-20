/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.golden.CalciteTestInfra;
import org.opensearch.dsl.golden.GoldenFileLoader;
import org.opensearch.dsl.golden.GoldenTestCase;
import org.opensearch.search.SearchModule;
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

    public void testBuildReturnsEmptyResponse() {
        SearchResponse response = SearchResponseBuilder.build(List.of(), 42L * 1_000_000);

        assertNotNull(response);
        assertEquals(200, response.status().getStatus());
        assertEquals(0, response.getHits().getHits().length);
        assertEquals(42L, response.getTook().millis());
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
                SearchResponse response = SearchResponseBuilder.build(List.of(result), 0L);
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
