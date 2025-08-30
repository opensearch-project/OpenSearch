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
 * Configuration for streaming search with scoring.
 * Controls the behavior of streaming search with different scoring modes.
 * 
 * @opensearch.internal
 */
@ExperimentalApi
public class StreamingScoringConfig {
    
    private final boolean enabled;
    private final StreamingScoringMode mode;
    private final int minDocsBeforeEmission;
    private final int emissionIntervalMillis;
    private final int rerankCountShard;
    private final int rerankCountGlobal;
    private final boolean enablePhaseMarkers;
    
    /**
     * Creates a streaming scoring configuration.
     */
    public StreamingScoringConfig(
        boolean enabled, 
        StreamingScoringMode mode,
        int minDocsBeforeEmission,
        int emissionIntervalMillis,
        int rerankCountShard,
        int rerankCountGlobal,
        boolean enablePhaseMarkers
    ) {
        this.enabled = enabled;
        this.mode = mode;
        this.minDocsBeforeEmission = minDocsBeforeEmission;
        this.emissionIntervalMillis = emissionIntervalMillis;
        this.rerankCountShard = rerankCountShard;
        this.rerankCountGlobal = rerankCountGlobal;
        this.enablePhaseMarkers = enablePhaseMarkers;
    }
    
    /**
     * Returns a disabled configuration.
     */
    public static StreamingScoringConfig disabled() {
        return new StreamingScoringConfig(
            false, 
            StreamingScoringMode.FULL_SCORING,
            1000, 100, 10, 100, false
        );
    }
    
    /**
     * Returns the default configuration.
     */
    public static StreamingScoringConfig defaultConfig() {
        return new StreamingScoringConfig(
            true, 
            StreamingScoringMode.CONFIDENCE_BASED,
            100, 50, 10, 100, true
        );
    }
    
    /**
     * Creates a configuration for a specific scoring mode.
     */
    public static StreamingScoringConfig forMode(StreamingScoringMode mode) {
        switch (mode) {
            case NO_SCORING:
                return new StreamingScoringConfig(
                    true, mode, 10, 10, 5, 50, true
                );
            case CONFIDENCE_BASED:
                return defaultConfig();
            case FULL_SCORING:
                return new StreamingScoringConfig(
                    true, mode, Integer.MAX_VALUE, Integer.MAX_VALUE, 10, 100, false
                );
            default:
                return defaultConfig();
        }
    }
    
    // Getters
    public boolean isEnabled() { return enabled; }
    public StreamingScoringMode getMode() { return mode; }
    public int getMinDocsBeforeEmission() { return minDocsBeforeEmission; }
    public int getEmissionIntervalMillis() { return emissionIntervalMillis; }
    public int getRerankCountShard() { return rerankCountShard; }
    public int getRerankCountGlobal() { return rerankCountGlobal; }
    public boolean isEnablePhaseMarkers() { return enablePhaseMarkers; }
}