/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

/**
 * Util class to extract trace id from traceparent header
 */
public class TraceUtil {

    /**
     * Extract trace id from traceparent header
     * @param traceparent traceparent header value
     * @return trace id
     */
    public static String extractTraceId(String traceparent) {
        validateTraceparent(traceparent);
        return traceparent.split("-")[1];
    }

    /**
     * Validates traceparent header format according to W3C Trace Context specification
     * Format: version-trace_id-parent_id-trace_flags
     * @param traceparent traceparent header value
     * @throws IllegalArgumentException if traceparent format is invalid
     */
    private static void validateTraceparent(String traceparent) {
        if (traceparent == null) {
            throw new IllegalArgumentException("traceparent cannot be null");
        }

        String[] parts = traceparent.split("-");

        if (traceparent.length() != 55) {
            throw new IllegalArgumentException("traceparent must be exactly 55 characters long");
        }

        if (parts.length != 4) {
            throw new IllegalArgumentException("traceparent must have exactly 4 parts separated by '-'");
        }

        // Validate version (2 hex digits)
        if (parts[0].length() != 2 || !isValidHex(parts[0])) {
            throw new IllegalArgumentException("version must be 2 hex digits");
        }

        if (parts[0].equals("ff")) {
            throw new IllegalArgumentException("version cannot be set to ff");
        }

        // Validate trace_id (32 hex digits, cannot be all zeros)
        if (parts[1].length() != 32 || !isValidHex(parts[1])) {
            throw new IllegalArgumentException("trace_id must be 32 hex digits");
        }
        if (parts[1].equals("00000000000000000000000000000000")) {
            throw new IllegalArgumentException("trace_id cannot be all zeros");
        }

        // Validate parent_id (16 hex digits, cannot be all zeros)
        if (parts[2].length() != 16 || !isValidHex(parts[2])) {
            throw new IllegalArgumentException("parent_id must be 16 hex digits");
        }
        if (parts[2].equals("0000000000000000")) {
            throw new IllegalArgumentException("parent_id cannot be all zeros");
        }

        // Validate trace_flags (2 hex digits)
        if (parts[3].length() != 2 || !isValidHex(parts[3])) {
            throw new IllegalArgumentException("trace_flags must be 2 hex digits");
        }
    }

    /**
     * Checks if a string contains only valid hexadecimal characters (0-9, a-f, A-F)
     * @param str string to validate
     * @return true if string contains only hex characters, false otherwise
     */
    private static boolean isValidHex(String str) {
        return str.matches("[0-9a-f]+");
    }
}
