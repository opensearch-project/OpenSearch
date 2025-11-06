/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexSettings;

import static org.opensearch.index.MergePolicyProvider.INDEX_MERGE_ENABLED;
import static org.opensearch.index.TieredMergePolicyProvider.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING;
import static org.opensearch.index.TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING;
import static org.opensearch.index.TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING;
import static org.opensearch.index.TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING;

public class ParquetTieredMergePolicyProvider {
    private final ParquetTieredMergePolicy mergePolicy = new ParquetTieredMergePolicy();

    private final Logger logger;
    private final boolean mergesEnabled;
    private int defaultMaxMergeAtOnce = 30;

    public ParquetTieredMergePolicyProvider(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        ByteSizeValue floorSegment = indexSettings.getValue(INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING);
        int maxMergeAtOnce = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING);
        ByteSizeValue maxMergedSegment = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING);
        double segmentsPerTier = indexSettings.getValue(INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING);

        this.mergesEnabled = indexSettings.getSettings().getAsBoolean(INDEX_MERGE_ENABLED, true);
        if (mergesEnabled == false) {
            this.logger.warn(
                "[{}] is set to false, this should only be used in tests and can cause serious problems in production" + " environments",
                INDEX_MERGE_ENABLED
            );
        }

        mergePolicy.setFloorSegmentMB(floorSegment.getMbFrac());
        mergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        mergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
        mergePolicy.setSegmentsPerTier(segmentsPerTier);
    }

    public ParquetTieredMergePolicy getMergePolicy() {
        return mergePolicy;
    }

    public void setSegmentsPerTier(Double segmentsPerTier) {
        mergePolicy.setSegmentsPerTier(segmentsPerTier);
    }

    public void setMaxMergedSegment(ByteSizeValue maxMergedSegment) {
        mergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
    }

    public void setFloorSegmentSetting(ByteSizeValue floorSegementSetting) {
        mergePolicy.setFloorSegmentMB(floorSegementSetting.getMbFrac());
    }

    public void setMaxMergesAtOnce(Integer maxMergeAtOnce) {
        mergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
    }

    public void setMaxMergesAtOnceToDefault() {
        mergePolicy.setMaxMergeAtOnce(defaultMaxMergeAtOnce);
    }
}
