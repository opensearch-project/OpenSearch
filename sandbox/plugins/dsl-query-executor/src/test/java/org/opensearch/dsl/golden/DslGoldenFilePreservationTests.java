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
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.MediaTypeRegistry;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Preservation property tests for the DSL golden file framework.
 *
 * <p>These tests capture baseline behavior of the UNFIXED code for all non-buggy
 * inputs. They MUST PASS on unfixed code, confirming the behavior to preserve
 * after fixes are applied.
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7</b>
 */
public class DslGoldenFilePreservationTests extends OpenSearchTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final NamedXContentRegistry X_CONTENT_REGISTRY = new NamedXContentRegistry(
        new SearchModule(org.opensearch.common.settings.Settings.EMPTY, Collections.emptyList()).getNamedXContents()
    );

    // ---- Property: Valid golden files load successfully (Req 3.1) ----

    /**
     * Property: For all valid golden files with all required fields and valid planType,
     * GoldenFileLoader.load() succeeds without error.
     *
     * <p>Validates: Requirements 3.1
     */
    public void testValidGoldenFileLoadMatchAllHits() {
        GoldenTestCase tc = GoldenFileLoader.load("match_all_hits.json");

        assertNotNull("testName should be populated", tc.getTestName());
        assertEquals("match_all_hits", tc.getTestName());
        assertNotNull("indexName should be populated", tc.getIndexName());
        assertEquals("test-index", tc.getIndexName());
        assertNotNull("indexMapping should be populated", tc.getIndexMapping());
        assertFalse("indexMapping should not be empty", tc.getIndexMapping().isEmpty());
        assertNotNull("inputDsl should be populated", tc.getInputDsl());
        assertNotNull("expectedRelNodePlan should be populated", tc.getExpectedRelNodePlan());
        assertNotNull("executionFieldNames should be populated", tc.getExecutionFieldNames());
        assertEquals(4, tc.getExecutionFieldNames().size());
        assertNotNull("executionRows should be populated", tc.getExecutionRows());
        assertEquals(2, tc.getExecutionRows().size());
        assertNotNull("expectedOutputDsl should be populated", tc.getExpectedOutputDsl());
        assertNotNull("planType should be populated", tc.getPlanType());
        assertEquals("HITS", tc.getPlanType());
    }

    /**
     * Property: For all valid golden files with all required fields and valid planType,
     * GoldenFileLoader.load() succeeds without error.
     *
     * <p>Validates: Requirements 3.1
     */
    public void testValidGoldenFileLoadTermsWithAvgAggregation() {
        GoldenTestCase tc = GoldenFileLoader.load("terms_with_avg_aggregation.json");

        assertNotNull("testName should be populated", tc.getTestName());
        assertEquals("terms_with_avg_aggregation", tc.getTestName());
        assertNotNull("indexName should be populated", tc.getIndexName());
        assertNotNull("indexMapping should be populated", tc.getIndexMapping());
        assertNotNull("inputDsl should be populated", tc.getInputDsl());
        assertNotNull("expectedRelNodePlan should be populated", tc.getExpectedRelNodePlan());
        assertNotNull("executionFieldNames should be populated", tc.getExecutionFieldNames());
        assertEquals(3, tc.getExecutionFieldNames().size());
        assertNotNull("executionRows should be populated", tc.getExecutionRows());
        assertNotNull("expectedOutputDsl should be populated", tc.getExpectedOutputDsl());
        assertNotNull("planType should be populated", tc.getPlanType());
        assertEquals("AGGREGATION", tc.getPlanType());
    }

    // ---- Property: stripNonDeterministicFields removes took, timed_out, _shards (Req 3.5) ----

    /**
     * Property: For all response maps, stripNonDeterministicFields always removes
     * took, timed_out, _shards.
     *
     * <p>Validates: Requirements 3.5
     */
    public void testStripNonDeterministicFieldsRemovesExpectedFields() {
        Map<String, Object> responseMap = new LinkedHashMap<>();
        responseMap.put("took", 42);
        responseMap.put("timed_out", false);
        responseMap.put("_shards", Map.of("total", 5, "successful", 5, "failed", 0));
        responseMap.put("hits", Map.of(
            "total", Map.of("value", 2, "relation", "eq"),
            "max_score", 1.0,
            "hits", List.of()
        ));
        responseMap.put("num_reduce_phases", 0);

        // Replicate the current stripNonDeterministicFields behavior
        stripNonDeterministicFieldsCurrent(responseMap);

        assertNull("took should be removed", responseMap.get("took"));
        assertNull("timed_out should be removed", responseMap.get("timed_out"));
        assertNull("_shards should be removed", responseMap.get("_shards"));
        // Non-deterministic fields removed, but other fields preserved
        assertNotNull("hits should be preserved", responseMap.get("hits"));
        assertNotNull("num_reduce_phases should be preserved", responseMap.get("num_reduce_phases"));
    }

    /**
     * Property: stripNonDeterministicFields works correctly even when some
     * non-deterministic fields are absent from the response map.
     *
     * <p>Validates: Requirements 3.5
     */
    public void testStripNonDeterministicFieldsPartialFields() {
        // Response map with only 'took' present (timed_out and _shards absent)
        Map<String, Object> responseMap = new LinkedHashMap<>();
        responseMap.put("took", 10);
        responseMap.put("hits", Map.of("total", Map.of("value", 0, "relation", "eq"), "hits", List.of()));

        stripNonDeterministicFieldsCurrent(responseMap);

        assertNull("took should be removed", responseMap.get("took"));
        assertNull("timed_out should be null (was never present)", responseMap.get("timed_out"));
        assertNull("_shards should be null (was never present)", responseMap.get("_shards"));
        assertNotNull("hits should be preserved", responseMap.get("hits"));
    }

    /**
     * Property: stripNonDeterministicFields works on empty maps without error.
     *
     * <p>Validates: Requirements 3.5
     */
    public void testStripNonDeterministicFieldsEmptyMap() {
        Map<String, Object> responseMap = new LinkedHashMap<>();
        stripNonDeterministicFieldsCurrent(responseMap);
        assertTrue("Empty map should remain empty after stripping", responseMap.isEmpty());
    }

    // ---- Property: GoldenFileUpdater.update() preserves input fields (Req 3.6) ----

    /**
     * Property: For all valid GoldenFileUpdater.update() calls, input fields
     * (inputDsl, executionFieldNames, executionRows, indexName, indexMapping)
     * are preserved in the output file.
     *
     * <p>Validates: Requirements 3.6
     */
    public void testGoldenFileUpdaterPreservesInputFields() throws Exception {
        // Create a temp golden file with known content
        Map<String, Object> originalContent = new LinkedHashMap<>();
        originalContent.put("testName", "preservation_test");
        originalContent.put("indexName", "my-index");
        originalContent.put("indexMapping", Map.of("name", "VARCHAR", "price", "INTEGER"));
        originalContent.put("planType", "HITS");
        originalContent.put("inputDsl", Map.of("query", Map.of("match_all", Map.of())));
        originalContent.put("expectedRelNodePlan", "OldPlan\n");
        originalContent.put("executionFieldNames", List.of("name", "price"));
        originalContent.put("executionRows", List.of(List.of("laptop", 999), List.of("phone", 699)));
        originalContent.put("expectedOutputDsl", Map.of("old_key", "old_value"));

        Path tempFile = createTempFile("golden_preservation", ".json");
        MAPPER.writeValue(tempFile.toFile(), originalContent);

        // Call update with new expected values
        String newPlan = "NewPlan\n";
        Map<String, Object> newOutputDsl = Map.of("new_key", "new_value");
        GoldenFileUpdater.update(tempFile, newPlan, newOutputDsl);

        // Read back and verify input fields are preserved
        Map<String, Object> updatedContent = MAPPER.readValue(
            Files.readString(tempFile),
            new TypeReference<LinkedHashMap<String, Object>>() {}
        );

        // Input fields must be preserved
        assertEquals("testName preserved", "preservation_test", updatedContent.get("testName"));
        assertEquals("indexName preserved", "my-index", updatedContent.get("indexName"));
        assertEquals("indexMapping preserved",
            Map.of("name", "VARCHAR", "price", "INTEGER"), updatedContent.get("indexMapping"));
        assertEquals("planType preserved", "HITS", updatedContent.get("planType"));
        assertEquals("inputDsl preserved",
            Map.of("query", Map.of("match_all", Map.of())), updatedContent.get("inputDsl"));
        assertEquals("executionFieldNames preserved",
            List.of("name", "price"), updatedContent.get("executionFieldNames"));
        assertEquals("executionRows preserved",
            List.of(List.of("laptop", 999), List.of("phone", 699)), updatedContent.get("executionRows"));

        // Expected fields must be overwritten
        assertEquals("expectedRelNodePlan updated", newPlan, updatedContent.get("expectedRelNodePlan"));
        assertEquals("expectedOutputDsl updated", newOutputDsl, updatedContent.get("expectedOutputDsl"));
    }

    /**
     * Property: GoldenFileUpdater.update() preserves input fields even when
     * the golden file has complex nested structures.
     *
     * <p>Validates: Requirements 3.6
     */
    public void testGoldenFileUpdaterPreservesComplexInputFields() throws Exception {
        Map<String, Object> complexInputDsl = new LinkedHashMap<>();
        complexInputDsl.put("size", 0);
        complexInputDsl.put("aggregations", Map.of(
            "by_brand", Map.of(
                "terms", Map.of("field", "brand"),
                "aggregations", Map.of("avg_price", Map.of("avg", Map.of("field", "price")))
            )
        ));

        Map<String, Object> originalContent = new LinkedHashMap<>();
        originalContent.put("testName", "complex_preservation_test");
        originalContent.put("indexName", "test-index");
        originalContent.put("indexMapping", Map.of("name", "VARCHAR", "price", "INTEGER",
            "brand", "VARCHAR", "rating", "DOUBLE"));
        originalContent.put("planType", "AGGREGATION");
        originalContent.put("inputDsl", complexInputDsl);
        originalContent.put("expectedRelNodePlan", "OldAggPlan\n");
        originalContent.put("executionFieldNames", List.of("brand", "avg_price", "_count"));
        originalContent.put("executionRows", List.of(
            List.of("BrandA", 850.0, 3),
            List.of("BrandB", 1100.0, 2)
        ));
        originalContent.put("expectedOutputDsl", Map.of("old", "data"));

        Path tempFile = createTempFile("golden_complex_preservation", ".json");
        MAPPER.writeValue(tempFile.toFile(), originalContent);

        GoldenFileUpdater.update(tempFile, "NewAggPlan\n", Map.of("new", "data"));

        Map<String, Object> updatedContent = MAPPER.readValue(
            Files.readString(tempFile),
            new TypeReference<LinkedHashMap<String, Object>>() {}
        );

        assertEquals("inputDsl preserved", complexInputDsl, updatedContent.get("inputDsl"));
        assertEquals("executionFieldNames preserved",
            List.of("brand", "avg_price", "_count"), updatedContent.get("executionFieldNames"));
        assertEquals("indexMapping preserved",
            Map.of("name", "VARCHAR", "price", "INTEGER", "brand", "VARCHAR", "rating", "DOUBLE"),
            updatedContent.get("indexMapping"));
    }

    // ---- Property: Forward path plan string comparison works (Req 3.2) ----
    // ---- Property: Reverse path hit comparison produces consistent results (Req 3.3, 3.7) ----
    // ---- Property: runAllChecks executes forward, reverse, and consistency checks (Req 3.4) ----

    /**
     * Property: Forward path plan string comparison works for match_all_hits.json.
     * The plan produced by SearchSourceConverter matches expectedRelNodePlan.
     *
     * <p>Validates: Requirements 3.2
     */
    public void testForwardPathPlanComparisonMatchAllHits() throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load("match_all_hits.json");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(
            tc.getIndexName(), tc.getIndexMapping()
        );

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
        assertFalse("Should produce at least one " + expectedType + " plan", matchingPlans.isEmpty());

        RelNode relNode = matchingPlans.get(0).relNode();
        String actualPlan = relNode.explain().trim();

        assertEquals(
            "Forward path plan should match expectedRelNodePlan for match_all_hits.json",
            tc.getExpectedRelNodePlan().trim(),
            actualPlan
        );
    }

    /**
     * Property: Forward path plan string comparison works for terms_with_avg_aggregation.json.
     *
     * <p>Validates: Requirements 3.2
     */
    public void testForwardPathPlanComparisonTermsWithAvgAggregation() throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load("terms_with_avg_aggregation.json");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(
            tc.getIndexName(), tc.getIndexMapping()
        );

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        List<QueryPlans.QueryPlan> matchingPlans = plans.get(expectedType);
        assertFalse("Should produce at least one " + expectedType + " plan", matchingPlans.isEmpty());

        RelNode relNode = matchingPlans.get(0).relNode();
        String actualPlan = relNode.explain().trim();

        assertEquals(
            "Forward path plan should match expectedRelNodePlan for terms_with_avg_aggregation.json",
            tc.getExpectedRelNodePlan().trim(),
            actualPlan
        );
    }

    /**
     * Property: For hit-only queries with deterministic ordering, reverse path
     * comparison produces consistent results. The SearchResponse built from
     * execution rows is deterministic for hit-only queries.
     *
     * <p>Validates: Requirements 3.3, 3.7
     */
    public void testReversePathHitComparisonDeterministic() throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load("match_all_hits.json");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(
            tc.getIndexName(), tc.getIndexMapping()
        );

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        QueryPlans.QueryPlan plan = plans.get(expectedType).get(0);

        List<Object[]> rows = new ArrayList<>();
        for (List<Object> row : tc.getExecutionRows()) {
            rows.add(row.toArray());
        }
        ExecutionResult result = new ExecutionResult(plan, rows);

        // Run reverse path twice to verify determinism
        var response1 = SearchResponseBuilder.build(List.of(result), 0L);
        String responseJson1 = Strings.toString(MediaTypeRegistry.JSON, response1);

        var response2 = SearchResponseBuilder.build(List.of(result), 0L);
        String responseJson2 = Strings.toString(MediaTypeRegistry.JSON, response2);

        @SuppressWarnings("unchecked")
        Map<String, Object> output1 = MAPPER.readValue(responseJson1, Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> output2 = MAPPER.readValue(responseJson2, Map.class);

        // Strip non-deterministic fields from both
        stripNonDeterministicFieldsCurrent(output1);
        stripNonDeterministicFieldsCurrent(output2);

        assertEquals(
            "Reverse path should produce deterministic output for hit-only queries",
            output1,
            output2
        );
    }

    /**
     * Property: Reverse path comparison for match_all_hits.json matches the
     * expectedOutputDsl after stripping non-deterministic fields.
     *
     * <p>Validates: Requirements 3.3, 3.7
     */
    public void testReversePathMatchesExpectedOutputForHits() throws Exception {
        GoldenTestCase tc = GoldenFileLoader.load("match_all_hits.json");
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(
            tc.getIndexName(), tc.getIndexMapping()
        );

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        QueryPlans.QueryPlan plan = plans.get(expectedType).get(0);

        List<Object[]> rows = new ArrayList<>();
        for (List<Object> row : tc.getExecutionRows()) {
            rows.add(row.toArray());
        }
        ExecutionResult result = new ExecutionResult(plan, rows);

        var response = SearchResponseBuilder.build(List.of(result), 0L);
        String responseJson = Strings.toString(MediaTypeRegistry.JSON, response);

        @SuppressWarnings("unchecked")
        Map<String, Object> actualOutput = MAPPER.readValue(responseJson, Map.class);

        // Deep copy expectedOutputDsl to avoid mutation (we're testing preservation)
        @SuppressWarnings("unchecked")
        Map<String, Object> expectedOutput = MAPPER.readValue(
            MAPPER.writeValueAsString(tc.getExpectedOutputDsl()),
            new TypeReference<LinkedHashMap<String, Object>>() {}
        );

        stripNonDeterministicFieldsCurrent(actualOutput);
        stripNonDeterministicFieldsCurrent(expectedOutput);

        assertEquals(
            "Reverse path output should match expectedOutputDsl for match_all_hits.json",
            expectedOutput,
            actualOutput
        );
    }

    /**
     * Property: runAllChecks executes forward, reverse, and consistency checks
     * in sequence for match_all_hits.json without error.
     *
     * <p>Validates: Requirements 3.4
     */
    public void testRunAllChecksSequenceMatchAllHits() throws Exception {
        // This replicates the runAllChecks flow from DslGoldenFileTests
        String goldenFileName = "match_all_hits.json";

        // Forward path
        GoldenTestCase tc = GoldenFileLoader.load(goldenFileName);
        CalciteTestInfra.InfraResult infra = CalciteTestInfra.buildFromMapping(
            tc.getIndexName(), tc.getIndexMapping()
        );

        SearchSourceBuilder searchSource = parseSearchSource(tc.getInputDsl());
        SearchSourceConverter converter = new SearchSourceConverter(infra.schema());
        QueryPlans plans = converter.convert(searchSource, tc.getIndexName());

        QueryPlans.Type expectedType = QueryPlans.Type.valueOf(tc.getPlanType());
        RelNode relNode = plans.get(expectedType).get(0).relNode();
        String actualPlan = relNode.explain().trim();

        assertEquals("Forward path check", tc.getExpectedRelNodePlan().trim(), actualPlan);

        // Reverse path
        QueryPlans.QueryPlan plan = plans.get(expectedType).get(0);
        List<Object[]> rows = new ArrayList<>();
        for (List<Object> row : tc.getExecutionRows()) {
            rows.add(row.toArray());
        }
        ExecutionResult result = new ExecutionResult(plan, rows);
        var response = SearchResponseBuilder.build(List.of(result), 0L);
        String responseJson = Strings.toString(MediaTypeRegistry.JSON, response);

        @SuppressWarnings("unchecked")
        Map<String, Object> actualOutput = MAPPER.readValue(responseJson, Map.class);
        Map<String, Object> expectedOutput = tc.getExpectedOutputDsl();
        stripNonDeterministicFieldsCurrent(actualOutput);
        stripNonDeterministicFieldsCurrent(expectedOutput);

        assertEquals("Reverse path check", expectedOutput, actualOutput);

        // Consistency check
        List<String> relNodeFields = relNode.getRowType().getFieldNames();
        assertEquals("Consistency check", tc.getExecutionFieldNames(), relNodeFields);
    }

    // ---- Helper Methods ----

    /**
     * Replicates the CURRENT (unfixed) behavior of DslGoldenFileTests.stripNonDeterministicFields.
     * Only removes top-level took, timed_out, _shards.
     */
    private void stripNonDeterministicFieldsCurrent(Map<String, Object> responseMap) {
        responseMap.remove("took");
        responseMap.remove("timed_out");
        responseMap.remove("_shards");
    }

    /**
     * Parses a golden file inputDsl map into a SearchSourceBuilder.
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
}
