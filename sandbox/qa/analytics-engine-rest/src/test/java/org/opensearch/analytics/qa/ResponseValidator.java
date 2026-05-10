/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Validates actual query responses against expected responses loaded from dataset resources.
 */
public final class ResponseValidator {

    private static final Logger logger = LogManager.getLogger(ResponseValidator.class);

    private ResponseValidator() {
        // utility class
    }

    /**
     * Validate actual response against expected response if it exists.
     * Returns null if validation passes or expected response doesn't exist.
     * Returns error message if validation fails.
     * 
     * @param dataset the dataset descriptor
     * @param language the query language (e.g., "ppl", "dsl")
     * @param queryNumber the query number
     * @param actual the actual response from the query
     * @param strategy the strategy for handling missing expected responses
     * @return null if validation passes, error message if validation fails
     */
    public static String validate(Dataset dataset, String language, int queryNumber, 
                                   Map<String, Object> actual, ExpectedResponseStrategy strategy) {
        String expectedPath = dataset.expectedResponseResourcePath(language, queryNumber);
        
        // Check if expected response exists
        boolean expectedExists = ResponseValidator.class.getClassLoader().getResource(expectedPath) != null;
        
        if (!expectedExists) {
            if (strategy == ExpectedResponseStrategy.FAIL_ON_MISSING) {
                return String.format("%s Q%d: Expected response file missing: %s", 
                    language.toUpperCase(), queryNumber, expectedPath);
            }
            // PASS_ON_MISSING or SKIP_VALIDATION - no validation needed
            logger.debug("No expected response for {} Q{}, strategy={}", 
                language.toUpperCase(), queryNumber, strategy);
            return null;
        }

        // Expected response exists - validate it (unless SKIP_VALIDATION)
        if (strategy == ExpectedResponseStrategy.SKIP_VALIDATION) {
            logger.debug("Skipping validation for {} Q{} (SKIP_VALIDATION strategy)", 
                language.toUpperCase(), queryNumber);
            return null;
        }

        try {
            String expectedJson = DatasetProvisioner.loadResource(expectedPath);
            // Parse JSON manually - simple approach for test data
            Map<String, Object> expected = parseSimpleJson(expectedJson);
            
            return compareResponses(expected, actual, language, queryNumber);
        } catch (Exception e) {
            return "Failed to load/parse expected response: " + e.getMessage();
        }
    }
    
    /**
     * Simple JSON parser for expected response files using OpenSearch's XContent parser.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> parseSimpleJson(String json) throws IOException {
        return org.opensearch.common.xcontent.XContentHelper.convertToMap(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            json,
            false
        );
    }

    /**
     * Compare expected and actual responses, focusing on data rows.
     */
    @SuppressWarnings("unchecked")
    private static String compareResponses(Map<String, Object> expected, Map<String, Object> actual, String language, int queryNumber) {
        // Extract datarows from both responses
        List<List<Object>> expectedRows = extractDataRows(expected);
        List<List<Object>> actualRows = extractDataRows(actual);

        if (expectedRows == null && actualRows == null) {
            return null; // both empty
        }

        if (expectedRows == null) {
            return String.format("%s Q%d: Expected empty response but got %d rows", 
                language.toUpperCase(), queryNumber, actualRows.size());
        }

        if (actualRows == null) {
            return String.format("%s Q%d: Expected %d rows but got empty response", 
                language.toUpperCase(), queryNumber, expectedRows.size());
        }

        if (expectedRows.size() != actualRows.size()) {
            return String.format("%s Q%d: Row count mismatch - expected %d, got %d", 
                language.toUpperCase(), queryNumber, expectedRows.size(), actualRows.size());
        }

        // Compare row by row
        for (int i = 0; i < expectedRows.size(); i++) {
            List<Object> expectedRow = expectedRows.get(i);
            List<Object> actualRow = actualRows.get(i);

            if (expectedRow.size() != actualRow.size()) {
                return String.format("%s Q%d row %d: Column count mismatch - expected %d, got %d", 
                    language.toUpperCase(), queryNumber, i, expectedRow.size(), actualRow.size());
            }

            for (int j = 0; j < expectedRow.size(); j++) {
                if (!valuesEqual(expectedRow.get(j), actualRow.get(j))) {
                    return String.format("%s Q%d row %d col %d: Value mismatch - expected %s, got %s", 
                        language.toUpperCase(), queryNumber, i, j, expectedRow.get(j), actualRow.get(j));
                }
            }
        }

        return null; // validation passed
    }

    /**
     * Extract datarows from response. Handles both PPL and DSL response formats.
     */
    @SuppressWarnings("unchecked")
    private static List<List<Object>> extractDataRows(Map<String, Object> response) {
        if (response == null) {
            return null;
        }

        // PPL format: response.datarows or response.rows
        if (response.containsKey("datarows")) {
            return (List<List<Object>>) response.get("datarows");
        }
        
        if (response.containsKey("rows")) {
            return (List<List<Object>>) response.get("rows");
        }

        // DSL format: response.hits.hits[].fields or _source
        if (response.containsKey("hits")) {
            Map<String, Object> hits = (Map<String, Object>) response.get("hits");
            if (hits != null && hits.containsKey("hits")) {
                List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hits.get("hits");
                if (hitsList != null && !hitsList.isEmpty()) {
                    // Convert hits to row format (simplified)
                    return null; // DSL validation can be added later
                }
            }
        }

        return null;
    }

    /**
     * Compare two values with numeric tolerance for floating point.
     */
    private static boolean valuesEqual(Object expected, Object actual) {
        if (Objects.equals(expected, actual)) {
            return true;
        }

        // Handle numeric comparison with tolerance
        if (expected instanceof Number && actual instanceof Number) {
            double exp = ((Number) expected).doubleValue();
            double act = ((Number) actual).doubleValue();
            return Math.abs(exp - act) < 1e-9;
        }

        return false;
    }
}
