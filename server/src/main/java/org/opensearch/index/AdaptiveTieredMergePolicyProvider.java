/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.TieredMergePolicy;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.store.Store;

/**
 * An adaptive merge policy provider that adjusts merge settings based on shard size
 * to optimize segment topology and reduce benchmark variance.
 *
 * This addresses the issue described in https://github.com/opensearch-project/OpenSearch/issues/11163
 * by providing more intelligent default merge settings that adapt to the actual shard size.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class AdaptiveTieredMergePolicyProvider implements MergePolicyProvider {

    private final Logger logger;
    private final OpenSearchTieredMergePolicy tieredMergePolicy;
    private final IndexSettings indexSettings;
    private Store store;
    private boolean mergesEnabled;

    // Adaptive settings based on shard size
    private static final ByteSizeValue SMALL_SHARD_THRESHOLD = new ByteSizeValue(100, ByteSizeUnit.MB);
    private static final ByteSizeValue MEDIUM_SHARD_THRESHOLD = new ByteSizeValue(1, ByteSizeUnit.GB);
    private static final ByteSizeValue LARGE_SHARD_THRESHOLD = new ByteSizeValue(10, ByteSizeUnit.GB);

    // Adaptive max segment sizes
    private static final ByteSizeValue SMALL_SHARD_MAX_SEGMENT = new ByteSizeValue(50, ByteSizeUnit.MB);
    private static final ByteSizeValue MEDIUM_SHARD_MAX_SEGMENT = new ByteSizeValue(200, ByteSizeUnit.MB);
    private static final ByteSizeValue LARGE_SHARD_MAX_SEGMENT = new ByteSizeValue(1, ByteSizeUnit.GB);
    private static final ByteSizeValue VERY_LARGE_SHARD_MAX_SEGMENT = new ByteSizeValue(2, ByteSizeUnit.GB);

    // Adaptive floor segment sizes
    private static final ByteSizeValue SMALL_SHARD_FLOOR = new ByteSizeValue(10, ByteSizeUnit.MB);
    private static final ByteSizeValue MEDIUM_SHARD_FLOOR = new ByteSizeValue(25, ByteSizeUnit.MB);
    private static final ByteSizeValue LARGE_SHARD_FLOOR = new ByteSizeValue(50, ByteSizeUnit.MB);
    private static final ByteSizeValue VERY_LARGE_SHARD_FLOOR = new ByteSizeValue(100, ByteSizeUnit.MB);

    // Adaptive segments per tier
    private static final double SMALL_SHARD_SEGMENTS_PER_TIER = 5.0;
    private static final double MEDIUM_SHARD_SEGMENTS_PER_TIER = 8.0;
    private static final double LARGE_SHARD_SEGMENTS_PER_TIER = 10.0;
    private static final double VERY_LARGE_SHARD_SEGMENTS_PER_TIER = 12.0;

    public AdaptiveTieredMergePolicyProvider(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        this.indexSettings = indexSettings;
        this.store = null; // Will be set later via setStore()
        this.tieredMergePolicy = new OpenSearchTieredMergePolicy();
        this.mergesEnabled = indexSettings.getSettings().getAsBoolean("index.merge.enabled", true);

        if (mergesEnabled == false) {
            logger.warn(
                "[index.merge.enabled] is set to false, this should only be used in tests and can cause serious problems in production environments"
            );
        }

        // Initialize with default settings first, will be updated when store is available
        applyDefaultSettings();
    }

    public AdaptiveTieredMergePolicyProvider(Logger logger, IndexSettings indexSettings, Store store) {
        this.logger = logger;
        this.indexSettings = indexSettings;
        this.store = store;
        this.tieredMergePolicy = new OpenSearchTieredMergePolicy();
        this.mergesEnabled = indexSettings.getSettings().getAsBoolean("index.merge.enabled", true);

        if (mergesEnabled == false) {
            logger.warn(
                "[index.merge.enabled] is set to false, this should only be used in tests and can cause serious problems in production environments"
            );
        }

        // Initialize with adaptive settings
        initializeAdaptiveSettings();
    }

    private void initializeAdaptiveSettings() {
        try {
            // Estimate shard size from store
            long estimatedShardSize = estimateShardSize();
            ShardSizeCategory category = categorizeShardSize(estimatedShardSize);

            // Apply adaptive settings based on shard size category
            applyAdaptiveSettings(category);

            logger.debug(
                "Initialized adaptive merge policy for shard size category: {} (estimated size: {})",
                category,
                new ByteSizeValue(estimatedShardSize)
            );

        } catch (Exception e) {
            logger.warn("Failed to initialize adaptive settings, falling back to defaults: {}", e.getMessage());
            applyDefaultSettings();
        }
    }

    private long estimateShardSize() {
        if (store == null) {
            // Fallback to a reasonable default when store is not available
            return MEDIUM_SHARD_THRESHOLD.getBytes();
        }
        try {
            // Try to get a rough estimate of shard size from the store
            // This is a best-effort estimation - using directory size as proxy
            return store.directory().listAll().length * 1024 * 1024; // Rough estimate
        } catch (Exception e) {
            // Fallback to a reasonable default
            return MEDIUM_SHARD_THRESHOLD.getBytes();
        }
    }

    private ShardSizeCategory categorizeShardSize(long sizeBytes) {
        if (sizeBytes < SMALL_SHARD_THRESHOLD.getBytes()) {
            return ShardSizeCategory.SMALL;
        } else if (sizeBytes < MEDIUM_SHARD_THRESHOLD.getBytes()) {
            return ShardSizeCategory.MEDIUM;
        } else if (sizeBytes < LARGE_SHARD_THRESHOLD.getBytes()) {
            return ShardSizeCategory.LARGE;
        } else {
            return ShardSizeCategory.VERY_LARGE;
        }
    }

    private void applyAdaptiveSettings(ShardSizeCategory category) {
        ByteSizeValue maxSegmentSize;
        ByteSizeValue floorSegmentSize;
        double segmentsPerTier;

        switch (category) {
            case SMALL:
                maxSegmentSize = SMALL_SHARD_MAX_SEGMENT;
                floorSegmentSize = SMALL_SHARD_FLOOR;
                segmentsPerTier = SMALL_SHARD_SEGMENTS_PER_TIER;
                break;
            case MEDIUM:
                maxSegmentSize = MEDIUM_SHARD_MAX_SEGMENT;
                floorSegmentSize = MEDIUM_SHARD_FLOOR;
                segmentsPerTier = MEDIUM_SHARD_SEGMENTS_PER_TIER;
                break;
            case LARGE:
                maxSegmentSize = LARGE_SHARD_MAX_SEGMENT;
                floorSegmentSize = LARGE_SHARD_FLOOR;
                segmentsPerTier = LARGE_SHARD_SEGMENTS_PER_TIER;
                break;
            case VERY_LARGE:
                maxSegmentSize = VERY_LARGE_SHARD_MAX_SEGMENT;
                floorSegmentSize = VERY_LARGE_SHARD_FLOOR;
                segmentsPerTier = VERY_LARGE_SHARD_SEGMENTS_PER_TIER;
                break;
            default:
                maxSegmentSize = MEDIUM_SHARD_MAX_SEGMENT;
                floorSegmentSize = MEDIUM_SHARD_FLOOR;
                segmentsPerTier = MEDIUM_SHARD_SEGMENTS_PER_TIER;
        }

        // Apply the adaptive settings
        tieredMergePolicy.setMaxMergedSegmentMB(maxSegmentSize.getMbFrac());
        tieredMergePolicy.setFloorSegmentMB(floorSegmentSize.getMbFrac());
        tieredMergePolicy.setSegmentsPerTier(segmentsPerTier);

        // Keep other settings at reasonable defaults
        tieredMergePolicy.setMaxMergeAtOnce(10);
        tieredMergePolicy.setForceMergeDeletesPctAllowed(10.0);
        tieredMergePolicy.setDeletesPctAllowed(20.0);
        tieredMergePolicy.setNoCFSRatio(TieredMergePolicy.DEFAULT_NO_CFS_RATIO);

        logger.info(
            "Applied adaptive merge settings - max_segment: {}, floor_segment: {}, segments_per_tier: {}",
            maxSegmentSize,
            floorSegmentSize,
            segmentsPerTier
        );
    }

    private void applyDefaultSettings() {
        // Fallback to the original default settings
        tieredMergePolicy.setMaxMergedSegmentMB(5 * 1024); // 5GB
        tieredMergePolicy.setFloorSegmentMB(16); // 16MB
        tieredMergePolicy.setSegmentsPerTier(10.0);
        tieredMergePolicy.setMaxMergeAtOnce(10);
        tieredMergePolicy.setForceMergeDeletesPctAllowed(10.0);
        tieredMergePolicy.setDeletesPctAllowed(20.0);
        tieredMergePolicy.setNoCFSRatio(TieredMergePolicy.DEFAULT_NO_CFS_RATIO);
    }

    /**
     * Sets the store instance and reinitializes adaptive settings
     */
    public void setStore(Store store) {
        this.store = store;
        if (store != null) {
            initializeAdaptiveSettings();
        }
    }

    /**
     * Updates merge settings based on runtime analysis of segment topology
     */
    public void updateSettingsBasedOnAnalysis(
        org.opensearch.index.analysis.SegmentTopologyAnalyzer.MergePolicyRecommendations recommendations
    ) {
        if (recommendations.hasVarianceIssue || recommendations.hasSkewIssue) {
            logger.info("Updating merge settings based on segment topology analysis");

            // Apply recommended settings
            tieredMergePolicy.setMaxMergedSegmentMB(recommendations.recommendedMaxSegmentSize / (1024 * 1024));
            tieredMergePolicy.setFloorSegmentMB(recommendations.recommendedFloorSegmentSize / (1024 * 1024));

            // Adjust segments per tier based on optimal count
            double newSegmentsPerTier = Math.max(5.0, Math.min(20.0, recommendations.optimalSegmentCount * 0.8));
            tieredMergePolicy.setSegmentsPerTier(newSegmentsPerTier);

            logger.info(
                "Updated merge settings - max_segment: {}MB, floor_segment: {}MB, segments_per_tier: {}",
                recommendations.recommendedMaxSegmentSize / (1024 * 1024),
                recommendations.recommendedFloorSegmentSize / (1024 * 1024),
                newSegmentsPerTier
            );
        }
    }

    @Override
    public org.apache.lucene.index.MergePolicy getMergePolicy() {
        return mergesEnabled ? tieredMergePolicy : org.apache.lucene.index.NoMergePolicy.INSTANCE;
    }

    // Getters for testing
    public double getMaxMergedSegmentMB() {
        return tieredMergePolicy.getMaxMergedSegmentMB();
    }

    public double getFloorSegmentMB() {
        return tieredMergePolicy.getFloorSegmentMB();
    }

    public double getSegmentsPerTier() {
        return tieredMergePolicy.getSegmentsPerTier();
    }

    private enum ShardSizeCategory {
        SMALL,
        MEDIUM,
        LARGE,
        VERY_LARGE
    }
}
