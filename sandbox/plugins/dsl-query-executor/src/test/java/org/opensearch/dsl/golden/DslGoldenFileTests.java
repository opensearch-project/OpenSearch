/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.rel.RelNode;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.dsl.converter.SearchSourceConverter;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.dsl.result.ExecutionResult;
import org.opensearch.dsl.result.SearchResponseBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Golden file tests for the DSL query executor plugin.
 *
 * <p>Each test method loads its own specific golden file by name, keeping
 * dependencies explicit. Tests validate the forward path (DSL → RelNode),
 * reverse path (ExecutionResult → SearchResponse), and field name consistency.
 */
public class DslGoldenFileTests extends OpenSearchTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final NamedXContentRegistry X_CONTENT_REGISTRY = new NamedXContentRegistry(
        new SearchModule(org.opensearch.common.settings.Settings.EMPTY, Collections.emptyList()).getNamedXContents()
    );

    // ---- Forward Path Helper ----

    /**
     * Loads a golden file, parses the inputDsl into a SearchSourceBuilder,
     * invokes SearchSourceConverter.convert(), serializes the RelNode via
     * explain(), and compares against expectedRelNodePlan. In update mode,
     * overwrites the golden file instead of asserting.
     */
    private void runForwardPathTest(String goldenFileName) throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load(goldenFileName);
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
        assertFalse("No " + expectedType + " plan produced for " + goldenFileName, matchingPlans.isEmpty());

        RelNode relNode = matchingPlans.get(0).relNode();
        String actualPlan = relNode.explain().trim();

        // Validate schema (row type) field names match executionFieldNames
        List<String> relNodeFields = relNode.getRowType().getFieldNames();
        assertEquals(
            "Forward path schema mismatch for " + goldenFileName
                + "\nRelNode row type fields: " + relNodeFields
                + "\nGolden file executionFieldNames: " + tc.getExecutionFieldNames(),
            tc.getExecutionFieldNames(),
            relNodeFields
        );

        if (GoldenFileUpdater.isUpdateMode()) {
            Path filePath = goldenFilePath(goldenFileName);
            GoldenFileUpdater.updatePlan(filePath, actualPlan);
        } else {
            assertEquals(
                "Forward path mismatch for " + goldenFileName
                    + "\nExpected:\n" + tc.getExpectedRelNodePlan()
                    + "\nActual:\n" + actualPlan,
                tc.getExpectedRelNodePlan().trim(),
                actualPlan
            );
        }
    }

    // ---- Reverse Path Helper ----

    /**
     * Loads a golden file, constructs an ExecutionResult from executionRows
     * and field names, invokes SearchResponseBuilder.build(), serializes the
     * SearchResponse to JSON, and compares against expectedOutputDsl ignoring
     * non-deterministic fields (took, _shards). In update mode, overwrites
     * the golden file instead of asserting.
     */
    private void runReversePathTest(String goldenFileName) throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load(goldenFileName);
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

        // Build the RelNode so we can construct a proper QueryPlan for ExecutionResult
        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        QueryPlans.QueryPlan plan = plans.get(expectedType).get(0);

        // Convert golden file rows to Object[] iterable
        List<Object[]> rows = new ArrayList<>();
        for (List<Object> row : tc.getExecutionRows()) {
            rows.add(row.toArray());
        }
        ExecutionResult result = new ExecutionResult(plan, rows);

        // Build SearchResponse
        var response = SearchResponseBuilder.build(List.of(result), 0L);
        String responseJson = Strings.toString(MediaTypeRegistry.JSON, response);

        @SuppressWarnings("unchecked")
        Map<String, Object> actualOutput = MAPPER.readValue(responseJson, Map.class);

        if (GoldenFileUpdater.isUpdateMode()) {
            Path filePath = goldenFilePath(goldenFileName);
            GoldenFileUpdater.update(filePath, tc.getExpectedRelNodePlan(), actualOutput);
        } else {
            // Defensive copy to avoid mutating GoldenTestCase internal map
            Map<String, Object> expectedOutput = MAPPER.readValue(
                MAPPER.writeValueAsString(tc.getExpectedOutputDsl()),
                new TypeReference<LinkedHashMap<String, Object>>() {}
            );
            // Remove non-deterministic fields before comparison
            stripNonDeterministicFields(actualOutput);
            stripNonDeterministicFields(expectedOutput);

            assertOutputEquals(expectedOutput, actualOutput, tc.getPlanType(), goldenFileName);
        }
    }

    // ---- Consistency Check Helper ----

    /**
     * Loads a golden file, runs the forward path to get the RelNode, and
     * verifies that the RelNode output field names match the executionFieldNames
     * in the golden file.
     */
    private void runConsistencyCheck(String goldenFileName) throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load(goldenFileName);
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(tc.getIndexName(), tc.getIndexMapping());

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        RelNode relNode = plans.get(expectedType).get(0).relNode();
        List<String> relNodeFields = relNode.getRowType().getFieldNames();

        assertEquals(
            "Field name consistency mismatch for " + goldenFileName
                + "\nRelNode fields: " + relNodeFields
                + "\nGolden file executionFieldNames: " + tc.getExecutionFieldNames(),
            tc.getExecutionFieldNames(),
            relNodeFields
        );
    }

    // ---- Utility Methods ----

    /**
     * Parses a golden file inputDsl map into a SearchSourceBuilder using
     * XContentParser with the full SearchModule registry (so query builders
     * like term, range, bool are recognized).
     */
    private SearchSourceBuilder parseSearchSource(Map<String, Object> inputDsl) throws IOException {
        String json = MAPPER.writeValueAsString(inputDsl);
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                X_CONTENT_REGISTRY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                json
            )
        ) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    /**
     * Resolves the file-system path to a golden file for update mode.
     * Falls back to classpath resource resolution.
     */
    private Path goldenFilePath(String goldenFileName) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource("golden/" + goldenFileName);
        if (resource == null) {
            throw new IllegalStateException("Golden file not found on classpath: golden/" + goldenFileName);
        }
        return Path.of(resource.toURI());
    }

    /**
     * Removes non-deterministic fields (took, _shards, _score) from a serialized
     * SearchResponse map so that comparisons are stable.
     */
    @SuppressWarnings("unchecked")
    private void stripNonDeterministicFields(Map<String, Object> responseMap) {
        responseMap.remove("took");
        responseMap.remove("timed_out");
        responseMap.remove("_shards");

        // Strip _score from each hit in hits.hits[]
        Object hitsObj = responseMap.get("hits");
        if (hitsObj instanceof Map) {
            Map<String, Object> hitsMap = (Map<String, Object>) hitsObj;
            Object hitsArray = hitsMap.get("hits");
            if (hitsArray instanceof List) {
                for (Object hit : (List<?>) hitsArray) {
                    if (hit instanceof Map) {
                        ((Map<String, Object>) hit).remove("_score");
                    }
                }
            }
        }
    }

    /**
     * Compares expected and actual output maps. For aggregation responses,
     * uses order-insensitive bucket comparison. For hit-only responses,
     * retains strict ordering.
     */
    private void assertOutputEquals(
        Map<String, Object> expected,
        Map<String, Object> actual,
        String planType,
        String goldenFileName
    ) throws IOException {
        if ("AGGREGATION".equals(planType)) {
            normalizeAggregationBuckets(expected);
            normalizeAggregationBuckets(actual);
        }
        assertEquals(
            "Reverse path mismatch for " + goldenFileName
                + "\nExpected:\n" + MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(expected)
                + "\nActual:\n" + MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(actual),
            expected,
            actual
        );
    }

    /**
     * Recursively sorts aggregation bucket lists by their "key" field so that
     * order-insensitive comparison is possible.
     */
    @SuppressWarnings("unchecked")
    private void normalizeAggregationBuckets(Map<String, Object> map) {
        Object aggs = map.get("aggregations");
        if (aggs instanceof Map) {
            normalizeBucketsRecursive((Map<String, Object>) aggs);
        }
    }

    @SuppressWarnings("unchecked")
    private void normalizeBucketsRecursive(Map<String, Object> aggMap) {
        for (Map.Entry<String, Object> entry : aggMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                Map<String, Object> aggBody = (Map<String, Object>) value;
                Object buckets = aggBody.get("buckets");
                if (buckets instanceof List) {
                    List<Map<String, Object>> bucketList = (List<Map<String, Object>>) buckets;
                    bucketList.sort(Comparator.comparing(
                        b -> String.valueOf(b.get("key"))
                    ));
                    // Recurse into sub-aggregations within each bucket
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

    // ---- Common Runner ----

    /**
     * Runs forward path, reverse path, and consistency checks for a single
     * golden file.
     */
    private void runAllChecks(String goldenFileName) throws Exception {
        runForwardPathTest(goldenFileName);
        runReversePathTest(goldenFileName);
        runConsistencyCheck(goldenFileName);
    }

    // ---- Test Methods ----

    public void testMatchAllHits() throws Exception {
        runAllChecks("match_all_hits.json");
    }

    public void testTermsWithAvgAggregation() throws Exception {
        runAllChecks("terms_with_avg_aggregation.json");
    }
}
