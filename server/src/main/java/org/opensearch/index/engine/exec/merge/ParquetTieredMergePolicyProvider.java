/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexSettings;

import static org.opensearch.index.MergePolicyProvider.INDEX_MERGE_ENABLED;

public class ParquetTieredMergePolicyProvider {
    private final ParquetTieredMergePolicy mergePolicy = new ParquetTieredMergePolicy();

    private final Logger logger;
    private final boolean mergesEnabled;

    /**
     *  Use 16MB floor size to match Lucene default.
     *  See <a href="https://github.com/apache/lucene/pull/14189">...</a>
     */
    public static final ByteSizeValue DEFAULT_FLOOR_SEGMENT = new ByteSizeValue(16, ByteSizeUnit.MB);

    public static final int MIN_DEFAULT_MAX_MERGE_AT_ONCE = 2;
    public static final int DEFAULT_MAX_MERGE_AT_ONCE = 30;

    public static final ByteSizeValue DEFAULT_MAX_MERGED_SEGMENT = new ByteSizeValue(5, ByteSizeUnit.GB);
    public static final double DEFAULT_SEGMENTS_PER_TIER = 10.0d;

    public static final Setting<ByteSizeValue> INDEX_MERGE_PARQUET_POLICY_FLOOR_SEGMENT_SETTING = Setting.byteSizeSetting(
        "index.merge.parquet.policy.floor_segment",
        DEFAULT_FLOOR_SEGMENT,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static final Setting<Integer> INDEX_MERGE_PARQUET_POLICY_MAX_MERGE_AT_ONCE_SETTING = Setting.intSetting(
        "index.merge.parquet.policy.max_merge_at_once",
        DEFAULT_MAX_MERGE_AT_ONCE,
        MIN_DEFAULT_MAX_MERGE_AT_ONCE,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDEX_MERGE_PARQUET_POLICY_MAX_MERGED_SEGMENT_SETTING = Setting.byteSizeSetting(
        "index.merge.parquet.policy.max_merged_segment",
        DEFAULT_MAX_MERGED_SEGMENT,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );
    public static final Setting<Double> INDEX_MERGE_PARQUET_POLICY_SEGMENTS_PER_TIER_SETTING = Setting.doubleSetting(
        "index.merge.parquet.policy.segments_per_tier",
        DEFAULT_SEGMENTS_PER_TIER,
        2.0d,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public ParquetTieredMergePolicyProvider(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        ByteSizeValue floorSegment = indexSettings.getValue(INDEX_MERGE_PARQUET_POLICY_FLOOR_SEGMENT_SETTING);
        int maxMergeAtOnce = indexSettings.getValue(INDEX_MERGE_PARQUET_POLICY_MAX_MERGE_AT_ONCE_SETTING);
        ByteSizeValue maxMergedSegment = indexSettings.getValue(INDEX_MERGE_PARQUET_POLICY_MAX_MERGED_SEGMENT_SETTING);
        double segmentsPerTier = indexSettings.getValue(INDEX_MERGE_PARQUET_POLICY_SEGMENTS_PER_TIER_SETTING);

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
}
