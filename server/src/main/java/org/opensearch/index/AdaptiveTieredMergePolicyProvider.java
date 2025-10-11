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
@PublicApi(since = "3.3.0")
public class AdaptiveTieredMergePolicyProvider implements MergePolicyProvider {

    private final Logger logger;
    private final OpenSearchTieredMergePolicy tieredMergePolicy;
    private Store store;
    private boolean mergesEnabled;

    // Base settings for small shards (100MB)
    private static final ByteSizeValue BASE_SHARD_SIZE = new ByteSizeValue(100, ByteSizeUnit.MB);
    private static final ByteSizeValue BASE_MAX_SEGMENT = new ByteSizeValue(50, ByteSizeUnit.MB);
    private static final ByteSizeValue BASE_FLOOR_SEGMENT = new ByteSizeValue(10, ByteSizeUnit.MB);
    private static final double BASE_SEGMENTS_PER_TIER = 5.0;

    public AdaptiveTieredMergePolicyProvider(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
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

            // Apply adaptive settings based on continuous scaling
            applyAdaptiveSettings(estimatedShardSize);

            logger.debug("Initialized adaptive merge policy for shard size: {}", new ByteSizeValue(estimatedShardSize));

        } catch (Exception e) {
            logger.warn("Failed to initialize adaptive settings, falling back to defaults: {}", e.getMessage());
            applyDefaultSettings();
        }
    }

    private long estimateShardSize() {
        if (store == null) {
            // Fallback to a reasonable default when store is not available
            return BASE_SHARD_SIZE.getBytes();
        }
        try {
            // Try to get a more accurate estimate by summing actual file sizes
            long totalSize = 0;
            String[] files = store.directory().listAll();
            for (String file : files) {
                try {
                    totalSize += store.directory().fileLength(file);
                } catch (Exception e) {
                    // Skip files we can't read, continue with others
                }
            }
            return totalSize;
        } catch (Exception e) {
            // Fallback to a reasonable default
            return BASE_SHARD_SIZE.getBytes();
        }
    }

    private void applyAdaptiveSettings(long shardSizeBytes) {
        // Calculate adaptive settings using continuous scaling
        // Scale from base values to maximum values based on shard size

        // Max segment size: 50MB to 5GB
        long maxSegmentSize = calculateMaxSegmentSize(shardSizeBytes);

        // Floor segment size: 10MB to 100MB
        long floorSegmentSize = calculateFloorSegmentSize(shardSizeBytes);

        // Segments per tier: 5.0 to 12.0
        double segmentsPerTier = calculateSegmentsPerTier(shardSizeBytes);

        // Apply the adaptive settings
        tieredMergePolicy.setMaxMergedSegmentMB(maxSegmentSize / (1024.0 * 1024.0));
        tieredMergePolicy.setFloorSegmentMB(floorSegmentSize / (1024.0 * 1024.0));
        tieredMergePolicy.setSegmentsPerTier(segmentsPerTier);

        // Keep other settings at reasonable defaults
        tieredMergePolicy.setMaxMergeAtOnce(10);
        tieredMergePolicy.setForceMergeDeletesPctAllowed(10.0);
        tieredMergePolicy.setDeletesPctAllowed(20.0);
        tieredMergePolicy.setNoCFSRatio(TieredMergePolicy.DEFAULT_NO_CFS_RATIO);

        logger.info(
            "Applied adaptive merge settings - max_segment: {}MB, floor_segment: {}MB, segments_per_tier: {}",
            maxSegmentSize / (1024 * 1024),
            floorSegmentSize / (1024 * 1024),
            segmentsPerTier
        );
    }

    private long calculateMaxSegmentSize(long shardSizeBytes) {
        double baseSize = BASE_MAX_SEGMENT.getBytes(); // 50MB
        double maxSize = 5L * 1024 * 1024 * 1024; // 5GB
        double baseShardSize = BASE_SHARD_SIZE.getBytes(); // 100MB

        // Use logarithmic scaling for smooth transitions
        double scaleFactor = Math.log10((double) shardSizeBytes / baseShardSize + 1.0);
        double maxScaleFactor = Math.log10(1000.0); // Scale up to 100GB shards

        double ratio = Math.min(scaleFactor / maxScaleFactor, 1.0);
        return (long) (baseSize + (maxSize - baseSize) * ratio);
    }

    private long calculateFloorSegmentSize(long shardSizeBytes) {
        double baseSize = BASE_FLOOR_SEGMENT.getBytes(); // 10MB
        double maxSize = 100L * 1024 * 1024; // 100MB
        double baseShardSize = BASE_SHARD_SIZE.getBytes(); // 100MB

        // Use logarithmic scaling for smooth transitions
        double scaleFactor = Math.log10((double) shardSizeBytes / baseShardSize + 1.0);
        double maxScaleFactor = Math.log10(1000.0); // Scale up to 100GB shards

        double ratio = Math.min(scaleFactor / maxScaleFactor, 1.0);
        return (long) (baseSize + (maxSize - baseSize) * ratio);
    }

    private double calculateSegmentsPerTier(long shardSizeBytes) {
        double baseValue = BASE_SEGMENTS_PER_TIER; // 5.0
        double maxValue = 12.0;
        double baseShardSize = BASE_SHARD_SIZE.getBytes(); // 100MB

        // Use logarithmic scaling for smooth transitions
        double scaleFactor = Math.log10((double) shardSizeBytes / baseShardSize + 1.0);
        double maxScaleFactor = Math.log10(1000.0); // Scale up to 100GB shards

        double ratio = Math.min(scaleFactor / maxScaleFactor, 1.0);
        return baseValue + (maxValue - baseValue) * ratio;
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

}
