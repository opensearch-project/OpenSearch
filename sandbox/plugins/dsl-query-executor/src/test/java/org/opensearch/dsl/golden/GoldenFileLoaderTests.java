/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.golden;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Unit tests for {@link GoldenFileLoader} validation logic.
 */
public class GoldenFileLoaderTests extends OpenSearchTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Verifies that a fixture missing {@code expectedOutputDsl} WITHOUT setting
     * {@code planShapeOnly: true} is rejected with a clear message directing
     * the author to either supply the field or set the flag deliberately.
     */
    public void testValidationRejectsMissingExpectedOutputDslWithoutPlanShapeOnlyFlag() throws IOException {
        // Minimal valid fixture content — has all required fields except expectedOutputDsl,
        // and does NOT set planShapeOnly.
        Map<String, Object> fixture = Map.of(
            "testName",
            "missing_output_no_flag",
            "indexName",
            "test-index",
            "indexMapping",
            Map.of("name", "VARCHAR"),
            "inputDsl",
            Map.of("size", 0),
            "expectedRelNodePlan",
            java.util.List.of("LogicalTableScan(table=[[test-index]])"),
            "mockResultFieldNames",
            java.util.List.of("name"),
            "mockResultRows",
            java.util.List.of(java.util.List.of("Alice")),
            "planType",
            "HITS"
        );

        Path tempFile = createTempFile("golden_test_", ".json");
        Files.writeString(tempFile, MAPPER.writeValueAsString(fixture));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> GoldenFileLoader.load(tempFile));
        assertTrue("Expected message to mention planShapeOnly, got: " + ex.getMessage(), ex.getMessage().contains("planShapeOnly"));
        assertTrue("Expected message to mention expectedOutputDsl, got: " + ex.getMessage(), ex.getMessage().contains("expectedOutputDsl"));
    }

    /**
     * Verifies that setting {@code planShapeOnly: true} AND providing
     * {@code expectedOutputDsl} is rejected as contradictory.
     */
    public void testValidationRejectsPlanShapeOnlyWithExpectedOutputDslPresent() throws IOException {
        Map<String, Object> fixture = Map.ofEntries(
            Map.entry("testName", "contradictory_flag_and_output"),
            Map.entry("indexName", "test-index"),
            Map.entry("indexMapping", Map.of("name", "VARCHAR")),
            Map.entry("inputDsl", Map.of("size", 0)),
            Map.entry("expectedRelNodePlan", java.util.List.of("LogicalTableScan(table=[[test-index]])")),
            Map.entry("mockResultFieldNames", java.util.List.of("name")),
            Map.entry("mockResultRows", java.util.List.of(java.util.List.of("Alice"))),
            Map.entry("planType", "HITS"),
            Map.entry("planShapeOnly", true),
            Map.entry("expectedOutputDsl", Map.of("hits", Map.of()))
        );

        Path tempFile = createTempFile("golden_test_", ".json");
        Files.writeString(tempFile, MAPPER.writeValueAsString(fixture));

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> GoldenFileLoader.load(tempFile));
        assertTrue("Expected message to mention contradictory, got: " + ex.getMessage(), ex.getMessage().contains("contradictory"));
    }

    /**
     * Verifies that a fixture with {@code planShapeOnly: true} and no
     * {@code expectedOutputDsl} passes validation successfully.
     */
    public void testValidationAcceptsPlanShapeOnlyWithoutExpectedOutputDsl() throws IOException {
        Map<String, Object> fixture = Map.of(
            "testName",
            "plan_shape_only_valid",
            "indexName",
            "test-index",
            "indexMapping",
            Map.of("name", "VARCHAR"),
            "inputDsl",
            Map.of("size", 0),
            "expectedRelNodePlan",
            java.util.List.of("LogicalTableScan(table=[[test-index]])"),
            "mockResultFieldNames",
            java.util.List.of("name"),
            "mockResultRows",
            java.util.List.of(java.util.List.of("Alice")),
            "planType",
            "HITS",
            "planShapeOnly",
            true
        );

        Path tempFile = createTempFile("golden_test_", ".json");
        Files.writeString(tempFile, MAPPER.writeValueAsString(fixture));

        // Should not throw
        GoldenTestCase tc = GoldenFileLoader.load(tempFile);
        assertEquals("plan_shape_only_valid", tc.getTestName());
        assertTrue(tc.isPlanShapeOnly());
    }
}
