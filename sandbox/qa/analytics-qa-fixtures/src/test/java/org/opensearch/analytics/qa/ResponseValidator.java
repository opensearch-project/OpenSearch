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
    private static String compareResponses(Map<String, Object> expected, Map<String, Object> actual, String language, int queryNumber) {
        String label = language.toUpperCase(java.util.Locale.ROOT) + " Q" + queryNumber;
        return compareData(expected, actual, label);
    }

    /**
     * Compare two response maps by their data rows, unordered and numeric-tolerant.
     * Returns {@code null} when equal, otherwise a human-readable diff prefixed with
     * {@code label}.
     *
     * <p>Exposed for name-keyed suites (e.g. the 2-shard reduce suite) that don't use the
     * numeric {@code q{N}} scheme and for differential checks (comparing a 1-shard result
     * against a 2-shard result rather than against a golden file).
     */
    @SuppressWarnings("unchecked")
    public static String compareData(Map<String, Object> expected, Map<String, Object> actual, String label) {
        // Extract datarows from both responses
        List<List<Object>> expectedRows = extractDataRows(expected);
        List<List<Object>> actualRows = extractDataRows(actual);

        if (expectedRows == null && actualRows == null) {
            return null; // both empty
        }

        if (expectedRows == null) {
            return String.format(java.util.Locale.ROOT, "%s: Expected empty response but got %d rows", label, actualRows.size());
        }

        if (actualRows == null) {
            return String.format(java.util.Locale.ROOT, "%s: Expected %d rows but got empty response", label, expectedRows.size());
        }

        if (expectedRows.size() != actualRows.size()) {
            return String.format(java.util.Locale.ROOT, "%s: Row count mismatch - expected %d, got %d",
                label, expectedRows.size(), actualRows.size());
        }

        // Copy before sorting so we never reorder the caller's lists.
        expectedRows = new java.util.ArrayList<>(expectedRows);
        actualRows = new java.util.ArrayList<>(actualRows);
        expectedRows.sort(new RowComparator());
        actualRows.sort(new RowComparator());

        // Compare row by row after sorting
        for (int i = 0; i < expectedRows.size(); i++) {
            List<Object> expectedRow = expectedRows.get(i);
            List<Object> actualRow = actualRows.get(i);

            if (expectedRow.size() != actualRow.size()) {
                return String.format(java.util.Locale.ROOT, "%s row %d: Column count mismatch - expected %d, got %d",
                    label, i, expectedRow.size(), actualRow.size());
            }

            for (int j = 0; j < expectedRow.size(); j++) {
                if (!valuesEqual(expectedRow.get(j), actualRow.get(j))) {
                    return String.format(java.util.Locale.ROOT, "%s row %d col %d: Value mismatch - expected %s, got %s",
                        label, i, j, expectedRow.get(j), actualRow.get(j));
                }
            }
        }

        return null; // validation passed
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
     *
     * <p>Property-match sentinels: a query whose value is non-deterministic (wall-clock or random)
     * can't assert an exact golden. The golden may instead use a sentinel that matches by <em>shape</em>:
     * <ul>
     *   <li>{@code "%%DATE%%"} — actual matches {@code yyyy-MM-dd}</li>
     *   <li>{@code "%%TIME%%"} — actual matches {@code HH:mm:ss} (optional fractional seconds)</li>
     *   <li>{@code "%%DATETIME%%"} — actual matches {@code yyyy-MM-dd HH:mm:ss} (optional fraction)</li>
     *   <li>{@code "%%RAND%%"} — actual is a number in [0, 1)</li>
     *   <li>{@code "%%ANY%%"} — actual is non-null (existence check)</li>
     * </ul>
     */
    private static boolean valuesEqual(Object expected, Object actual) {
        if (expected instanceof String) {
            String sentinel = matchSentinel((String) expected, actual);
            if (sentinel != null) {
                return sentinel.equals("true");
            }
        }

        // Multi-value cells (collection aggs like values()/list()) are an unordered multiset; the
        // gather decides element order, so compare them order-insensitively.
        if (expected instanceof List && actual instanceof List) {
            List<?> e = new java.util.ArrayList<>((List<?>) expected);
            List<?> a = new java.util.ArrayList<>((List<?>) actual);
            if (e.size() != a.size()) {
                return false;
            }
            java.util.Comparator<Object> byStr = java.util.Comparator.comparing(o -> o == null ? "" : o.toString());
            e.sort(byStr);
            a.sort(byStr);
            return e.equals(a);
        }

        if (Objects.equals(expected, actual)) {
            return true;
        }

        // Handle numeric comparison with tolerance
        if (expected instanceof Number && actual instanceof Number) {
            double exp = ((Number) expected).doubleValue();
            double act = ((Number) actual).doubleValue();
            return numbersClose(exp, act);
        }

        return false;
    }

    /**
     * Compares two doubles with a hybrid absolute + relative tolerance. A purely absolute {@code
     * 1e-9} is too strict for large aggregate sums (e.g. TPC-H q1's {@code sum_base_price ~ 3.7e7}):
     * floating-point summation reorders across shards and runs, so the last ULP of a ~1e7 magnitude
     * value drifts by ~1e-8 — larger than 1e-9 yet semantically identical. Accept when the values
     * are within 1e-9 absolutely OR within 1e-9 relative to the larger magnitude.
     */
    private static boolean numbersClose(double exp, double act) {
        double diff = Math.abs(exp - act);
        if (diff < 1e-9) {
            return true;
        }
        double scale = Math.max(Math.abs(exp), Math.abs(act));
        return diff <= 1e-9 * scale;
    }

    /** Returns "true"/"false" if {@code expected} is a known property sentinel, else null. */
    private static String matchSentinel(String expected, Object actual) {
        switch (expected) {
            case "%%DATE%%":
                return String.valueOf(actual).matches("\\d{4}-\\d{2}-\\d{2}") ? "true" : "false";
            case "%%TIME%%":
                return String.valueOf(actual).matches("\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?") ? "true" : "false";
            case "%%DATETIME%%":
                return String.valueOf(actual).matches("\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?") ? "true" : "false";
            case "%%RAND%%":
                if (actual instanceof Number) {
                    double d = ((Number) actual).doubleValue();
                    return (d >= 0.0 && d < 1.0) ? "true" : "false";
                }
                return "false";
            case "%%ANY%%":
                return actual != null ? "true" : "false";
            default:
                return null;
        }
    }
}
