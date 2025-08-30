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
 * Defines the different streaming search strategies based on the design-by-case approach.
 * Each mode optimizes for different use cases and performance characteristics.
 *
 * @opensearch.internal
 */
@ExperimentalApi
public enum StreamingSearchMode {

    /**
     * Case 1: No scoring, no sorting - fastest TTFB
     * - Shard collector: StreamingUnsortedCollector
     * - Ring buffer with batch emission
     * - Round-robin merge at coordinator
     * - Best for: simple filtering, counting, exists queries
     */
    NO_SCORING("no_scoring"),

    /**
     * Case 2: Full scoring + explicit sort - production ready
     * - Shard collector: StreamingSortedCollector
     * - WAND/Block-Max WAND with windowed top-K heap
     * - K-way streaming merge at coordinator
     * - Best for: scored searches with sorting
     */
    SCORED_SORTED("scored_sorted"),

    /**
     * Case 3: Full scoring, no sorting - moderate performance
     * - Shard collector: StreamingScoredUnsortedCollector
     * - Ring buffer with scoring
     * - No merge needed at coordinator
     * - Best for: scored searches without sorting
     */
    SCORED_UNSORTED("scored_unsorted"),

    /**
     * Case 4: Partial scoring + confidence preview - advanced
     * - Shard collector: StreamingConfidenceCollector
     * - Deterministic bounds + optional confidence metadata
     * - Global stability coordination
     * - Best for: progressive results with confidence guarantees
     */
    CONFIDENCE_BASED("confidence_based");

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
            return SCORED_UNSORTED; // Default
        }

        for (StreamingSearchMode m : values()) {
            if (m.name().equalsIgnoreCase(mode) || m.value.equalsIgnoreCase(mode)) {
                return m;
            }
        }

        throw new IllegalArgumentException("Unknown streaming search mode: " + mode);
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
        return this == SCORED_SORTED;
    }

    /**
     * Helper method to check if this mode uses confidence bounds.
     * @return true if confidence bounds are used, false otherwise
     */
    public boolean usesConfidence() {
        return this == CONFIDENCE_BASED;
    }

    @Override
    public String toString() {
        return value;
    }
}
