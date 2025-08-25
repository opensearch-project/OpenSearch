/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

/**
 * Scoring modes for streaming search operations.
 * 
 * @opensearch.internal
 */
public enum StreamingScoringMode {
    
    /**
     * No scoring - stream results as they arrive without scoring.
     * Fastest but may have lower quality results.
     */
    NO_SCORING,
    
    /**
     * Confidence-based scoring using Hoeffding bounds.
     * Balances speed and accuracy by emitting when statistically confident.
     */
    CONFIDENCE_BASED,
    
    /**
     * Full scoring - complete all scoring before streaming results.
     * Most accurate but slower initial response.
     */
    FULL_SCORING;
    
    /**
     * Default mode when not specified.
     */
    public static final StreamingScoringMode DEFAULT = CONFIDENCE_BASED;
    
    /**
     * Parse mode from string representation.
     */
    public static StreamingScoringMode fromString(String mode) {
        if (mode == null) {
            return DEFAULT;
        }
        switch (mode.toLowerCase()) {
            case "none":
            case "no_scoring":
                return NO_SCORING;
            case "confidence":
            case "confidence_based":
                return CONFIDENCE_BASED;
            case "full":
            case "full_scoring":
                return FULL_SCORING;
            default:
                throw new IllegalArgumentException("Unknown streaming scoring mode: " + mode);
        }
    }
}