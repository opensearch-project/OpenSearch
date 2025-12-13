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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An adaptive merge policy provider that adjusts merge settings based on shard size
 * to optimize segment topology and reduce benchmark variance.
 *
 * This addresses the issue described in https://github.com/opensearch-project/OpenSearch/issues/11163
 * by providing more intelligent default merge settings that adapt to the actual shard size.
 *
 * Implementation notes:
 * - Uses smooth interpolation (log/linear across size decades) instead of hard categories
 *   to avoid abrupt parameter jumps as shards grow.
 * - Caps the max merged segment size at 5GB to align with Lucene defaults.
 *
 * @opensearch.internal
 */
public class AdaptiveTieredMergePolicyProvider implements MergePolicyProvider {

    private final Logger logger;
    private final AdaptiveOpenSearchTieredMergePolicy tieredMergePolicy;
    private final boolean mergesEnabled;
    // Lock to protect concurrent modifications to merge policy settings
    private final ReentrantReadWriteLock settingsLock = new ReentrantReadWriteLock();

    public AdaptiveTieredMergePolicyProvider(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        this.tieredMergePolicy = new AdaptiveOpenSearchTieredMergePolicy(settingsLock);
        this.mergesEnabled = indexSettings.getSettings().getAsBoolean(INDEX_MERGE_ENABLED, true);

        if (mergesEnabled == false) {
            logger.warn(
                "[{}] is set to false, this should only be used in tests and can cause serious problems in production environments",
                INDEX_MERGE_ENABLED
            );
        }

        // Initialize with default settings. The adaptive policy updates itself dynamically
        // on each findMerges() call based on current SegmentInfos.
        applyDefaultSettings();
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

    @Override
    public org.apache.lucene.index.MergePolicy getMergePolicy() {
        return mergesEnabled ? tieredMergePolicy : org.apache.lucene.index.NoMergePolicy.INSTANCE;
    }
}
