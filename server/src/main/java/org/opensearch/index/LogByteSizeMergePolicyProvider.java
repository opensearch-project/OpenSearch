/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.apache.lucene.index.LogMergePolicy.DEFAULT_MAX_MERGE_DOCS;
import static org.apache.lucene.index.LogMergePolicy.DEFAULT_NO_CFS_RATIO;

/**
 * <p>
 * The LogByteSizeMergePolicy is an alternative merge policy primarily used here to optimize the merging of segments in scenarios
 * with index with timestamps.
 * While the TieredMergePolicy is the default choice, the LogByteSizeMergePolicy can be configured
 * as the default merge policy for time-index data using the <code>index.datastream_merge.policy</code> setting.
 *
 * <p>
 * Unlike the TieredMergePolicy, which prioritizes merging segments of equal sizes, the LogByteSizeMergePolicy
 * specializes in merging adjacent segments efficiently.
 * This characteristic makes it particularly well-suited for range queries on time-index data.
 * Typically, adjacent segments in time-index data often contain documents with similar timestamps.
 * When these segments are merged, the resulting segment covers a range of timestamps with reduced overlap compared
 * to the adjacent segments. This reduced overlap remains even as segments grow older and larger,
 * which can significantly benefit range queries on timestamps.
 *
 * <p>
 * In contrast, the TieredMergePolicy does not honor this timestamp range optimization. It focuses on merging segments
 * of equal sizes and does not consider adjacency. Consequently, as segments grow older and larger,
 * the overlap of timestamp ranges among adjacent segments managed by TieredMergePolicy can increase.
 * This can lead to inefficiencies in range queries on timestamps, as the number of segments to be scanned
 * within a given timestamp range could become high.
 *
 * @opensearch.internal
 */
public class LogByteSizeMergePolicyProvider implements MergePolicyProvider {
    private final LogByteSizeMergePolicy logByteSizeMergePolicy = new LogByteSizeMergePolicy();

    private final Logger logger;
    private final boolean mergesEnabled;

    public static final ByteSizeValue DEFAULT_MIN_MERGE = new ByteSizeValue(2, ByteSizeUnit.MB);
    public static final int DEFAULT_MERGE_FACTOR = 10;

    public static final ByteSizeValue DEFAULT_MAX_MERGED_SEGMENT = new ByteSizeValue(5, ByteSizeUnit.GB);

    public static final ByteSizeValue DEFAULT_MAX_MERGE_SEGMENT_FORCE_MERGE = new ByteSizeValue(Long.MAX_VALUE);

    public static final Setting<Integer> INDEX_LBS_MERGE_POLICY_MERGE_FACTOR_SETTING = Setting.intSetting(
        "index.merge.log_byte_size_policy.merge_factor",
        DEFAULT_MERGE_FACTOR, // keeping it same as default max merge at once for tiered merge policy
        2,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<ByteSizeValue> INDEX_LBS_MERGE_POLICY_MIN_MERGE_SETTING = Setting.byteSizeSetting(
        "index.merge.log_byte_size_policy.min_merge",
        DEFAULT_MIN_MERGE, // keeping it same as default floor segment for tiered merge policy
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<ByteSizeValue> INDEX_LBS_MAX_MERGE_SEGMENT_SETTING = Setting.byteSizeSetting(
        "index.merge.log_byte_size_policy.max_merge_segment",
        DEFAULT_MAX_MERGED_SEGMENT, // keeping default same as tiered merge policy
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<ByteSizeValue> INDEX_LBS_MAX_MERGE_SEGMENT_FOR_FORCED_MERGE_SETTING = Setting.byteSizeSetting(
        "index.merge.log_byte_size_policy.max_merge_segment_forced_merge",
        DEFAULT_MAX_MERGE_SEGMENT_FORCE_MERGE,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<Integer> INDEX_LBS_MAX_MERGED_DOCS_SETTING = Setting.intSetting(
        "index.merge.log_byte_size_policy.max_merged_docs",
        DEFAULT_MAX_MERGE_DOCS,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<Double> INDEX_LBS_NO_CFS_RATIO_SETTING = new Setting<>(
        "index.merge.log_byte_size_policy.no_cfs_ratio",
        Double.toString(DEFAULT_NO_CFS_RATIO),
        TieredMergePolicyProvider::parseNoCFSRatio,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    LogByteSizeMergePolicyProvider(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        this.mergesEnabled = indexSettings.getSettings().getAsBoolean(INDEX_MERGE_ENABLED, true);

        // Undocumented settings, works great with defaults
        logByteSizeMergePolicy.setMergeFactor(indexSettings.getValue(INDEX_LBS_MERGE_POLICY_MERGE_FACTOR_SETTING));
        logByteSizeMergePolicy.setMinMergeMB(indexSettings.getValue(INDEX_LBS_MERGE_POLICY_MIN_MERGE_SETTING).getMbFrac());
        logByteSizeMergePolicy.setMaxMergeMB(indexSettings.getValue(INDEX_LBS_MAX_MERGE_SEGMENT_SETTING).getMbFrac());
        logByteSizeMergePolicy.setMaxMergeMBForForcedMerge(
            indexSettings.getValue(INDEX_LBS_MAX_MERGE_SEGMENT_FOR_FORCED_MERGE_SETTING).getMbFrac()
        );
        logByteSizeMergePolicy.setMaxMergeDocs(indexSettings.getValue(INDEX_LBS_MAX_MERGED_DOCS_SETTING));
        logByteSizeMergePolicy.setNoCFSRatio(indexSettings.getValue(INDEX_LBS_NO_CFS_RATIO_SETTING));
    }

    @Override
    public MergePolicy getMergePolicy() {
        return mergesEnabled ? logByteSizeMergePolicy : NoMergePolicy.INSTANCE;
    }

    void setLBSMergeFactor(int mergeFactor) {
        logByteSizeMergePolicy.setMergeFactor(mergeFactor);
    }

    void setLBSMaxMergeSegment(ByteSizeValue maxMergeSegment) {
        logByteSizeMergePolicy.setMaxMergeMB(maxMergeSegment.getMbFrac());
    }

    void setLBSMinMergedMB(ByteSizeValue minMergedSize) {
        logByteSizeMergePolicy.setMinMergeMB(minMergedSize.getMbFrac());
    }

    void setLBSMaxMergeMBForForcedMerge(ByteSizeValue maxMergeForcedMerge) {
        logByteSizeMergePolicy.setMaxMergeMBForForcedMerge(maxMergeForcedMerge.getMbFrac());
    }

    void setLBSMaxMergeDocs(int maxMergeDocs) {
        logByteSizeMergePolicy.setMaxMergeDocs(maxMergeDocs);
    }

    void setLBSNoCFSRatio(Double noCFSRatio) {
        logByteSizeMergePolicy.setNoCFSRatio(noCFSRatio);
    }

    @Override
    public String toString() {
        return "LogByteSizeMergePolicyProvider{"
            + "mergeFactor="
            + logByteSizeMergePolicy.getMergeFactor()
            + ", minMergeMB="
            + logByteSizeMergePolicy.getMinMergeMB()
            + ", maxMergeMB="
            + logByteSizeMergePolicy.getMaxMergeMB()
            + ", maxMergeMBForForcedMerge="
            + logByteSizeMergePolicy.getMaxMergeMBForForcedMerge()
            + ", maxMergedDocs="
            + logByteSizeMergePolicy.getMaxMergeDocs()
            + ", noCFSRatio="
            + logByteSizeMergePolicy.getNoCFSRatio()
            + '}';
    }

}
