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
                return String.format(java.util.Locale.ROOT, "%s Q%d: Expected response file missing: %s",
                    language.toUpperCase(java.util.Locale.ROOT), queryNumber, expectedPath);
            }
            // PASS_ON_MISSING or SKIP_VALIDATION - pass when file doesn't exist
            logger.debug("No expected response for {} Q{}, strategy={}",
                language.toUpperCase(java.util.Locale.ROOT), queryNumber, strategy);
            return null;
        }

        // Expected response exists - validate it (unless SKIP_VALIDATION)
        if (strategy == ExpectedResponseStrategy.SKIP_VALIDATION) {
            logger.debug("Skipping validation for {} Q{} (SKIP_VALIDATION strategy)",
                language.toUpperCase(java.util.Locale.ROOT), queryNumber);
            return null;
        }

        // For PASS_ON_MISSING and FAIL_ON_MISSING: validate since file exists

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
     * Rows are compared in an unordered fashion - both sets are sorted before comparison.
     */
    @SuppressWarnings("unchecked")
    private static String compareResponses(Map<String, Object> expected, Map<String, Object> actual, String language, int queryNumber) {
        // Extract datarows from both responses
        List<List<Object>> expectedRows = extractDataRows(expected);
        List<List<Object>> actualRows = extractDataRows(actual);
        // Optional per-query numeric tolerance (default 0 = strict). Use for approximate aggregates
        // like dc()/percentile_approx that vary slightly across shard configurations.
        long tolerance = expected.get("tolerance") instanceof Number ? ((Number) expected.get("tolerance")).longValue() : 0L;

        if (expectedRows == null && actualRows == null) {
            return null; // both empty
        }

        if (expectedRows == null) {
            return String.format(java.util.Locale.ROOT, "%s Q%d: Expected empty response but got %d rows",
                language.toUpperCase(java.util.Locale.ROOT), queryNumber, actualRows.size());
        }

        if (actualRows == null) {
            return String.format(java.util.Locale.ROOT, "%s Q%d: Expected %d rows but got empty response",
                language.toUpperCase(java.util.Locale.ROOT), queryNumber, expectedRows.size());
        }

        if (expectedRows.size() != actualRows.size()) {
            return String.format(java.util.Locale.ROOT, "%s Q%d: Row count mismatch - expected %d, got %d",
                language.toUpperCase(java.util.Locale.ROOT), queryNumber, expectedRows.size(), actualRows.size());
        }

        // With a tolerance, sort-then-compare-by-index is unsafe because a number differing by ±1
        // can shift the row's rank. Switch to order-invariant matching: for each expected row, find
        // a remaining actual row whose non-numeric cells equal and numeric cells fall within tolerance.
        if (tolerance > 0) {
            return compareRowsWithTolerance(expectedRows, actualRows, tolerance, language, queryNumber);
        }

        // Sort both row sets for unordered comparison
        expectedRows.sort(new RowComparator());
        actualRows.sort(new RowComparator());

        // Compare row by row after sorting
        for (int i = 0; i < expectedRows.size(); i++) {
            List<Object> expectedRow = expectedRows.get(i);
            List<Object> actualRow = actualRows.get(i);

            if (expectedRow.size() != actualRow.size()) {
                return String.format(java.util.Locale.ROOT, "%s Q%d row %d: Column count mismatch - expected %d, got %d",
                    language.toUpperCase(java.util.Locale.ROOT), queryNumber, i, expectedRow.size(), actualRow.size());
            }

            for (int j = 0; j < expectedRow.size(); j++) {
                if (!valuesEqual(expectedRow.get(j), actualRow.get(j))) {
                    return String.format(java.util.Locale.ROOT, "%s Q%d row %d col %d: Value mismatch - expected %s, got %s",
                        language.toUpperCase(java.util.Locale.ROOT), queryNumber, i, j, expectedRow.get(j), actualRow.get(j));
                }
            }
        }

        return null; // validation passed
    }

    /**
     * Order-invariant row comparison with per-cell numeric tolerance. Used when the expected file
     * declares {@code "tolerance": N}: two numbers count as equal if they differ by at most N
     * (typically 1 for HLL/dc() across shard configurations). Non-numeric cells still must match
     * exactly — they act as the row-identity key.
     */
    private static String compareRowsWithTolerance(
        List<List<Object>> expectedRows, List<List<Object>> actualRows,
        long tolerance, String language, int queryNumber
    ) {
        List<List<Object>> remaining = new java.util.ArrayList<>(actualRows);
        for (List<Object> exp : expectedRows) {
            int matchIdx = -1;
            for (int i = 0; i < remaining.size(); i++) {
                if (rowMatchesWithTolerance(exp, remaining.get(i), tolerance)) {
                    matchIdx = i;
                    break;
                }
            }
            if (matchIdx < 0) {
                return String.format(java.util.Locale.ROOT,
                    "%s Q%d: no actual row matches expected %s within tolerance %d (remaining unmatched actual rows: %s)",
                    language.toUpperCase(java.util.Locale.ROOT), queryNumber, exp, tolerance, remaining);
            }
            remaining.remove(matchIdx);
        }
        return null;
    }

    private static boolean rowMatchesWithTolerance(List<Object> a, List<Object> b, long tolerance) {
        if (a.size() != b.size()) return false;
        for (int i = 0; i < a.size(); i++) {
            Object x = a.get(i), y = b.get(i);
            if (x instanceof Number && y instanceof Number) {
                if (Math.abs(((Number) x).doubleValue() - ((Number) y).doubleValue()) > tolerance) return false;
            } else if (!Objects.equals(x, y)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Comparator for sorting rows lexicographically.
     */
    private static class RowComparator implements java.util.Comparator<List<Object>> {
        @Override
        public int compare(List<Object> row1, List<Object> row2) {
            int minSize = Math.min(row1.size(), row2.size());
            for (int i = 0; i < minSize; i++) {
                int cmp = compareValues(row1.get(i), row2.get(i));
                if (cmp != 0) {
                    return cmp;
                }
            }
            return Integer.compare(row1.size(), row2.size());
        }

        @SuppressWarnings("unchecked")
        private int compareValues(Object v1, Object v2) {
            if (v1 == null && v2 == null) return 0;
            if (v1 == null) return -1;
            if (v2 == null) return 1;

            // Compare numbers
            if (v1 instanceof Number && v2 instanceof Number) {
                return Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
            }

            // Compare strings
            if (v1 instanceof String && v2 instanceof String) {
                return ((String) v1).compareTo((String) v2);
            }

            // Compare booleans
            if (v1 instanceof Boolean && v2 instanceof Boolean) {
                return ((Boolean) v1).compareTo((Boolean) v2);
            }

            // Fallback to string comparison
            return v1.toString().compareTo(v2.toString());
        }
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
