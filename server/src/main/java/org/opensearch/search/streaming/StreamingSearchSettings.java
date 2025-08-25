/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Production-ready settings for streaming search with comprehensive configuration options.
 * All settings are dynamically updateable for runtime tuning.
 */
public final class StreamingSearchSettings {

    // Feature flags
    public static final Setting<Boolean> STREAMING_SEARCH_ENABLED = Setting.boolSetting(
        "search.streaming.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> STREAMING_SEARCH_ENABLED_FOR_EXPENSIVE_QUERIES = Setting.boolSetting(
        "search.streaming.expensive_queries.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Performance settings
    public static final Setting<Integer> STREAMING_BLOCK_SIZE = Setting.intSetting(
        "search.streaming.block_size",
        128,
        16,
        1024,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> STREAMING_BATCH_SIZE = Setting.intSetting(
        "search.streaming.batch_size",
        10,
        1,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> STREAMING_EMISSION_INTERVAL = Setting.timeSetting(
        "search.streaming.emission_interval",
        TimeValue.timeValueMillis(50),
        TimeValue.timeValueMillis(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Confidence settings
    public static final Setting<Float> STREAMING_INITIAL_CONFIDENCE = Setting.floatSetting(
        "search.streaming.initial_confidence",
        0.99f,
        0.5f,
        1.0f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Float> STREAMING_CONFIDENCE_DECAY_RATE = Setting.floatSetting(
        "search.streaming.confidence_decay_rate",
        0.02f,
        0.001f,
        0.1f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Float> STREAMING_MIN_CONFIDENCE = Setting.floatSetting(
        "search.streaming.min_confidence",
        0.80f,
        0.5f,
        0.95f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Thresholds
    public static final Setting<Integer> STREAMING_MIN_DOCS_FOR_STREAMING = Setting.intSetting(
        "search.streaming.min_docs_for_streaming",
        10,
        1,
        1000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Float> STREAMING_MIN_SHARD_RESPONSE_RATIO = Setting.floatSetting(
        "search.streaming.min_shard_response_ratio",
        0.2f,
        0.1f,
        0.9f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Float> STREAMING_OUTLIER_THRESHOLD_SIGMA = Setting.floatSetting(
        "search.streaming.outlier_threshold_sigma",
        2.0f,
        1.0f,
        4.0f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Memory management
    public static final Setting<ByteSizeValue> STREAMING_MAX_BUFFER_SIZE = Setting.byteSizeSetting(
        "search.streaming.max_buffer_size",
        new ByteSizeValue(10, ByteSizeUnit.MB),
        new ByteSizeValue(1, ByteSizeUnit.MB),
        new ByteSizeValue(100, ByteSizeUnit.MB),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> STREAMING_MAX_CONCURRENT_STREAMS = Setting.intSetting(
        "search.streaming.max_concurrent_streams",
        100,
        1,
        10000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Network settings
    public static final Setting<TimeValue> STREAMING_CLIENT_TIMEOUT = Setting.timeSetting(
        "search.streaming.client_timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> STREAMING_COMPRESSION_ENABLED = Setting.boolSetting(
        "search.streaming.compression.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Circuit breaker settings
    public static final Setting<ByteSizeValue> STREAMING_CIRCUIT_BREAKER_LIMIT = Setting.memorySizeSetting(
        "indices.breaker.streaming.limit",
        "10%",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Float> STREAMING_CIRCUIT_BREAKER_OVERHEAD = Setting.floatSetting(
        "indices.breaker.streaming.overhead",
        1.0f,
        0.0f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Monitoring settings
    public static final Setting<Boolean> STREAMING_METRICS_ENABLED = Setting.boolSetting(
        "search.streaming.metrics.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> STREAMING_METRICS_INTERVAL = Setting.timeSetting(
        "search.streaming.metrics.interval",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMinutes(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Advanced tuning
    public static final Setting<Float> STREAMING_BLOCK_SKIP_THRESHOLD_RATIO = Setting.floatSetting(
        "search.streaming.block_skip_threshold_ratio",
        0.3f,
        0.1f,
        0.9f,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Integer> STREAMING_MIN_COMPETITIVE_DOCS = Setting.intSetting(
        "search.streaming.min_competitive_docs",
        100,
        10,
        10000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<String> STREAMING_SCORE_MODE = Setting.simpleString(
        "search.streaming.score_mode",
        "COMPLETE",
        value -> {
            if (!Arrays.asList("COMPLETE", "TOP_SCORES", "MAX_SCORE").contains(value.toUpperCase())) {
                throw new IllegalArgumentException("Invalid score mode: " + value);
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Experimental features
    public static final Setting<Boolean> STREAMING_ADAPTIVE_BATCHING = Setting.boolSetting(
        "search.streaming.adaptive_batching.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> STREAMING_PREDICTIVE_SCORING = Setting.boolSetting(
        "search.streaming.predictive_scoring.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Returns all streaming search settings
     */
    public static List<Setting<?>> getAllSettings() {
        return Arrays.asList(
            STREAMING_SEARCH_ENABLED,
            STREAMING_SEARCH_ENABLED_FOR_EXPENSIVE_QUERIES,
            STREAMING_BLOCK_SIZE,
            STREAMING_BATCH_SIZE,
            STREAMING_EMISSION_INTERVAL,
            STREAMING_INITIAL_CONFIDENCE,
            STREAMING_CONFIDENCE_DECAY_RATE,
            STREAMING_MIN_CONFIDENCE,
            STREAMING_MIN_DOCS_FOR_STREAMING,
            STREAMING_MIN_SHARD_RESPONSE_RATIO,
            STREAMING_OUTLIER_THRESHOLD_SIGMA,
            STREAMING_MAX_BUFFER_SIZE,
            STREAMING_MAX_CONCURRENT_STREAMS,
            STREAMING_CLIENT_TIMEOUT,
            STREAMING_COMPRESSION_ENABLED,
            STREAMING_CIRCUIT_BREAKER_LIMIT,
            STREAMING_CIRCUIT_BREAKER_OVERHEAD,
            STREAMING_METRICS_ENABLED,
            STREAMING_METRICS_INTERVAL,
            STREAMING_BLOCK_SKIP_THRESHOLD_RATIO,
            STREAMING_MIN_COMPETITIVE_DOCS,
            STREAMING_SCORE_MODE,
            STREAMING_ADAPTIVE_BATCHING,
            STREAMING_PREDICTIVE_SCORING
        );
    }

    /**
     * Configuration holder for streaming search
     */
    public static class StreamingSearchConfig {
        private final Settings settings;
        private final ClusterSettings clusterSettings;

        // Cached values for performance
        private volatile boolean enabled;
        private volatile int blockSize;
        private volatile int batchSize;
        private volatile long emissionIntervalMillis;
        private volatile float initialConfidence;
        private volatile float confidenceDecayRate;
        private volatile float minConfidence;

        public StreamingSearchConfig(Settings settings, ClusterSettings clusterSettings) {
            this.settings = settings;
            this.clusterSettings = clusterSettings;
            
            // Initialize cached values
            updateCachedValues();
            
            // Register update listeners
            clusterSettings.addSettingsUpdateConsumer(STREAMING_SEARCH_ENABLED, this::setEnabled);
            clusterSettings.addSettingsUpdateConsumer(STREAMING_BLOCK_SIZE, this::setBlockSize);
            clusterSettings.addSettingsUpdateConsumer(STREAMING_BATCH_SIZE, this::setBatchSize);
            clusterSettings.addSettingsUpdateConsumer(STREAMING_EMISSION_INTERVAL, 
                interval -> this.emissionIntervalMillis = interval.millis());
            clusterSettings.addSettingsUpdateConsumer(STREAMING_INITIAL_CONFIDENCE, this::setInitialConfidence);
            clusterSettings.addSettingsUpdateConsumer(STREAMING_CONFIDENCE_DECAY_RATE, this::setConfidenceDecayRate);
            clusterSettings.addSettingsUpdateConsumer(STREAMING_MIN_CONFIDENCE, this::setMinConfidence);
        }

        private void updateCachedValues() {
            this.enabled = STREAMING_SEARCH_ENABLED.get(settings);
            this.blockSize = STREAMING_BLOCK_SIZE.get(settings);
            this.batchSize = STREAMING_BATCH_SIZE.get(settings);
            this.emissionIntervalMillis = STREAMING_EMISSION_INTERVAL.get(settings).millis();
            this.initialConfidence = STREAMING_INITIAL_CONFIDENCE.get(settings);
            this.confidenceDecayRate = STREAMING_CONFIDENCE_DECAY_RATE.get(settings);
            this.minConfidence = STREAMING_MIN_CONFIDENCE.get(settings);
        }

        // Fast getters for hot path
        public boolean isEnabled() { return enabled; }
        public int getBlockSize() { return blockSize; }
        public int getBatchSize() { return batchSize; }
        public long getEmissionIntervalMillis() { return emissionIntervalMillis; }
        public float getInitialConfidence() { return initialConfidence; }
        public float getConfidenceDecayRate() { return confidenceDecayRate; }
        public float getMinConfidence() { return minConfidence; }

        // Setters for dynamic updates
        private void setEnabled(boolean enabled) { this.enabled = enabled; }
        private void setBlockSize(int blockSize) { this.blockSize = blockSize; }
        private void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        private void setInitialConfidence(float confidence) { this.initialConfidence = confidence; }
        private void setConfidenceDecayRate(float rate) { this.confidenceDecayRate = rate; }
        private void setMinConfidence(float confidence) { this.minConfidence = confidence; }

        // Get non-cached values
        public int getMinDocsForStreaming() {
            return STREAMING_MIN_DOCS_FOR_STREAMING.get(settings);
        }

        public float getMinShardResponseRatio() {
            return STREAMING_MIN_SHARD_RESPONSE_RATIO.get(settings);
        }

        public float getOutlierThresholdSigma() {
            return STREAMING_OUTLIER_THRESHOLD_SIGMA.get(settings);
        }

        public ByteSizeValue getMaxBufferSize() {
            return STREAMING_MAX_BUFFER_SIZE.get(settings);
        }

        public int getMaxConcurrentStreams() {
            return STREAMING_MAX_CONCURRENT_STREAMS.get(settings);
        }

        public boolean isAdaptiveBatchingEnabled() {
            return STREAMING_ADAPTIVE_BATCHING.get(settings);
        }

        public boolean isPredictiveScoringEnabled() {
            return STREAMING_PREDICTIVE_SCORING.get(settings);
        }

        public boolean isMetricsEnabled() {
            return STREAMING_METRICS_ENABLED.get(settings);
        }
    }
}