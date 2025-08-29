/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

/**
 * Configuration for streaming search with scoring.
 * Controls the behavior of streaming search with different scoring modes.
 * 
 * @opensearch.internal
 */
public class StreamingScoringConfig {
    
    private final boolean enabled;
    private final float confidence;
    private final int checkInterval;
    private final int minDocsBeforeEmission;
    
    /**
     * Creates a streaming scoring configuration.
     * 
     * @param enabled Whether streaming is enabled
     * @param confidence The confidence threshold for emission
     * @param checkInterval How often to check for emission (in documents)
     * @param minDocsBeforeEmission Minimum documents before first emission
     */
    public StreamingScoringConfig(boolean enabled, float confidence, int checkInterval, int minDocsBeforeEmission) {
        this.enabled = enabled;
        this.confidence = confidence;
        this.checkInterval = checkInterval;
        this.minDocsBeforeEmission = minDocsBeforeEmission;
    }
    
    /**
     * Returns a disabled configuration.
     * 
     * @return A disabled streaming scoring configuration
     */
    public static StreamingScoringConfig disabled() {
        return new StreamingScoringConfig(false, 0.95f, 100, 1000);
    }
    
    /**
     * Returns the default configuration.
     * 
     * @return The default streaming scoring configuration
     */
    public static StreamingScoringConfig defaultConfig() {
        return new StreamingScoringConfig(true, 0.95f, 100, 1000);
    }
    
    /**
     * Creates a configuration for a specific scoring mode.
     * 
     * @param mode The scoring mode
     * @return Configuration optimized for the given mode
     */
    public static StreamingScoringConfig forMode(StreamingScoringMode mode) {
        switch (mode) {
            case NO_SCORING:
                return new StreamingScoringConfig(true, 0.0f, 10, 10);
            case CONFIDENCE_BASED:
                return defaultConfig();
            case FULL_SCORING:
                return new StreamingScoringConfig(true, 1.0f, Integer.MAX_VALUE, Integer.MAX_VALUE);
            default:
                return defaultConfig();
        }
    }
    
    /**
     * Check if streaming is enabled.
     * 
     * @return true if streaming is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Get the confidence threshold.
     * 
     * @return The confidence level (0-1)
     */
    public float getConfidence() {
        return confidence;
    }
    
    /**
     * Get the check interval.
     * 
     * @return Number of documents between emission checks
     */
    public int getCheckInterval() {
        return checkInterval;
    }
    
    /**
     * Get the minimum documents before emission.
     * 
     * @return Minimum documents required before first emission
     */
    public int getMinDocsBeforeEmission() {
        return minDocsBeforeEmission;
    }
}