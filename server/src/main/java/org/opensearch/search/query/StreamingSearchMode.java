/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Streaming search mode selector.
 *
 * <p>Only {@link #NO_SCORING} is active in the current streaming implementation.
 * Other values are retained for request parsing/backward compatibility.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public enum StreamingSearchMode {

    /**
     * No scoring and no sorting.
     */
    NO_SCORING("no_scoring"),

    /**
     * Retained for backward compatibility.
     */
    SCORED_SORTED("scored_sorted"),

    /**
     * Retained for backward compatibility.
     */
    SCORED_UNSORTED("scored_unsorted");

    private final String value;

    StreamingSearchMode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    /**
     * Parse mode from string representation.
     *
     * @param mode The string representation of the mode
     * @return The corresponding StreamingSearchMode
     * @throws IllegalArgumentException if mode is unknown
     */
    public static StreamingSearchMode fromString(String mode) {
        if (mode == null) {
            return NO_SCORING; // Default
        }

        // Backward compatibility: coerce older scored modes into NO_SCORING
        if ("SCORED_UNSORTED".equalsIgnoreCase(mode) || "SCORED_SORTED".equalsIgnoreCase(mode) || "NO_SCORING".equalsIgnoreCase(mode)) {
            return NO_SCORING;
        }

        for (StreamingSearchMode m : StreamingSearchMode.values()) {
            if (m.name().equalsIgnoreCase(mode) || m.value.equalsIgnoreCase(mode)) {
                return m;
            }
        }
        
        throw new IllegalArgumentException("Unknown StreamingSearchMode: " + mode);
    }

    /**
     * Helper method to check if this mode requires scoring.
     * @return true if scoring is required, false otherwise
     */
    public boolean requiresScoring() {
        return this != NO_SCORING;
    }

    /**
     * Helper method to check if this mode requires sorting.
     * @return true if sorting is required, false otherwise
     */
    public boolean requiresSorting() {
        return false; // No sorting is supported in the current mode
    }

    @Override
    public String toString() {
        return value;
    }
}
