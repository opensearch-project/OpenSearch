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
    CONFIDENCE("confidence");
    
    private final String value;
    
    StreamingSearchMode(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    public static StreamingSearchMode fromString(String value) {
        if (value == null) {
            return SCORED_SORTED; // Default
        }
        
        for (StreamingSearchMode mode : values()) {
            if (mode.value.equalsIgnoreCase(value)) {
                return mode;
            }
        }
        
        throw new IllegalArgumentException("Unknown streaming search mode: " + value);
    }
    
    @Override
    public String toString() {
        return value;
    }
}
