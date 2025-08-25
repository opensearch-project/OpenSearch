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
 * 
 * @opensearch.internal
 */
public class StreamingScoringConfig {
    
    private final boolean enabled;
    private final float confidence;
    private final int checkInterval;
    private final int minDocsBeforeEmission;
    
    public StreamingScoringConfig(boolean enabled, float confidence, int checkInterval, int minDocsBeforeEmission) {
        this.enabled = enabled;
        this.confidence = confidence;
        this.checkInterval = checkInterval;
        this.minDocsBeforeEmission = minDocsBeforeEmission;
    }
    
    public static StreamingScoringConfig disabled() {
        return new StreamingScoringConfig(false, 0.95f, 100, 1000);
    }
    
    public static StreamingScoringConfig defaultConfig() {
        return new StreamingScoringConfig(true, 0.95f, 100, 1000);
    }
    
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
    
    public boolean isEnabled() {
        return enabled;
    }
    
    public float getConfidence() {
        return confidence;
    }
    
    public int getCheckInterval() {
        return checkInterval;
    }
    
    public int getMinDocsBeforeEmission() {
        return minDocsBeforeEmission;
    }
}