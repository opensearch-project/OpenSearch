/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * A shard in opensearch is a Lucene index, and a Lucene index is broken
 * down into segments. Segments are internal storage elements in the index
 * where the index data is stored, and are immutable up to delete markers.
 * Segments are, periodically, merged into larger segments to keep the
 * index size at bay and expunge deletes.
 * This class customizes and exposes 2 merge policies from lucene -
 * {@link LogByteSizeMergePolicy} and  {@link TieredMergePolicy}.
 *
 *
 * <p>
 * Tiered merge policy select segments of approximately equal size, subject to an allowed
 * number of segments per tier. The merge policy is able to merge
 * non-adjacent segments, and separates how many segments are merged at once from how many
 * segments are allowed per tier. It also does not over-merge (i.e., cascade merges).
 *
 * <p>
 * All merge policy settings are <b>dynamic</b> and can be updated on a live index.
 * The merge policy has the following settings:
 *
 * <ul>
 * <li><code>index.merge.policy.expunge_deletes_allowed</code>:
 *
 *     When expungeDeletes is called, we only merge away a segment if its delete
 *     percentage is over this threshold. Default is <code>10</code>.
 *
 * <li><code>index.merge.policy.floor_segment</code>:
 *
 *     Segments smaller than this are "rounded up" to this size, i.e. treated as
 *     equal (floor) size for merge selection. This is to prevent frequent
 *     flushing of tiny segments, thus preventing a long tail in the index. Default
 *     is <code>2mb</code>.
 *
 * <li><code>index.merge.policy.max_merge_at_once</code>:
 *
 *     Maximum number of segments to be merged at a time during "normal" merging.
 *     Default is <code>10</code>.
 *
 * <li><code>index.merge.policy.max_merged_segment</code>:
 *
 *     Maximum sized segment to produce during normal merging (not explicit
 *     force merge). This setting is approximate: the estimate of the merged
 *     segment size is made by summing sizes of to-be-merged segments
 *     (compensating for percent deleted docs). Default is <code>5gb</code>.
 *
 * <li><code>index.merge.policy.segments_per_tier</code>:
 *
 *     Sets the allowed number of segments per tier. Smaller values mean more
 *     merging but fewer segments. Default is <code>10</code>. Note, this value needs to be
 *     &gt;= than the <code>max_merge_at_once</code> otherwise you'll force too many merges to
 *     occur.
 *
 * <li><code>index.merge.policy.deletes_pct_allowed</code>:
 *
 *     Controls the maximum percentage of deleted documents that is tolerated in
 *     the index. Lower values make the index more space efficient at the
 *     expense of increased CPU and I/O activity. Values must be between <code>5</code> and
 *     <code>50</code>. Default value is <code>20</code>.
 * </ul>
 *
 * <p>
 * For normal merging, the policy first computes a "budget" of how many
 * segments are allowed to be in the index. If the index is over-budget,
 * then the policy sorts segments by decreasing size (proportionally considering percent
 * deletes), and then finds the least-cost merge. Merge cost is measured by
 * a combination of the "skew" of the merge (size of largest seg divided by
 * smallest seg), total merge size and pct deletes reclaimed, so that
 * merges with lower skew, smaller size and those reclaiming more deletes,
 * are favored.
 *
 * <p>
 * If a merge will produce a segment that's larger than
 * <code>max_merged_segment</code> then the policy will merge fewer segments (down to
 * 1 at once, if that one has deletions) to keep the segment size under
 * budget.
 *
 * <p>
 * Note, this can mean that for large shards that holds many gigabytes of
 * data, the default of <code>max_merged_segment</code> (<code>5gb</code>) can cause for many
 * segments to be in an index, and causing searches to be slower. Use the
 * indices segments API to see the segments that an index has, and
 * possibly either increase the <code>max_merged_segment</code> or issue an optimize
 * call for the index (try and aim to issue it on a low traffic time).
 *
 * @opensearch.internal
 */

public final class TieredMergePolicyProvider implements MergePolicyProvider {
    private final OpenSearchTieredMergePolicy tieredMergePolicy = new OpenSearchTieredMergePolicy();

    private final Logger logger;
    private final boolean mergesEnabled;
    private int defaultMaxMergeAtOnce = 30;

    public static final double DEFAULT_EXPUNGE_DELETES_ALLOWED = 10d;

    /**
     *  Use 16MB floor size to match Lucene default.
     *  See <a href="https://github.com/apache/lucene/pull/14189">...</a>
     */
    public static final ByteSizeValue DEFAULT_FLOOR_SEGMENT = new ByteSizeValue(16, ByteSizeUnit.MB);

    public static final int MIN_DEFAULT_MAX_MERGE_AT_ONCE = 2;
    public static final int DEFAULT_MAX_MERGE_AT_ONCE = 30;

    public static final ByteSizeValue DEFAULT_MAX_MERGED_SEGMENT = new ByteSizeValue(5, ByteSizeUnit.GB);
    public static final double DEFAULT_SEGMENTS_PER_TIER = 10.0d;
    public static final double DEFAULT_RECLAIM_DELETES_WEIGHT = 2.0d;
    public static final double DEFAULT_DELETES_PCT_ALLOWED = 20.0d;

    public static final Setting<Double> INDEX_COMPOUND_FORMAT_SETTING = new Setting<>(
        "index.compound_format",
        Double.toString(TieredMergePolicy.DEFAULT_NO_CFS_RATIO),
        TieredMergePolicyProvider::parseNoCFSRatio,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Double> INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING = Setting.doubleSetting(
        "index.merge.policy.expunge_deletes_allowed",
        DEFAULT_EXPUNGE_DELETES_ALLOWED,
        0.0d,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING = Setting.byteSizeSetting(
        "index.merge.policy.floor_segment",
        DEFAULT_FLOOR_SEGMENT,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Integer> INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING = Setting.intSetting(
        "index.merge.policy.max_merge_at_once",
        DEFAULT_MAX_MERGE_AT_ONCE,
        MIN_DEFAULT_MAX_MERGE_AT_ONCE,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING = Setting.byteSizeSetting(
        "index.merge.policy.max_merged_segment",
        DEFAULT_MAX_MERGED_SEGMENT,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Double> INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING = Setting.doubleSetting(
        "index.merge.policy.segments_per_tier",
        DEFAULT_SEGMENTS_PER_TIER,
        2.0d,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Double> INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING = Setting.doubleSetting(
        "index.merge.policy.reclaim_deletes_weight",
        DEFAULT_RECLAIM_DELETES_WEIGHT,
        0.0d,
        Property.Dynamic,
        Property.IndexScope,
        Property.Deprecated
    );
    public static final Setting<Double> INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING = Setting.doubleSetting(
        "index.merge.policy.deletes_pct_allowed",
        DEFAULT_DELETES_PCT_ALLOWED,
        5.0d,
        50.0d,
        Property.Dynamic,
        Property.IndexScope
    );

    TieredMergePolicyProvider(Logger logger, IndexSettings indexSettings) {
        this.logger = logger;
        double forceMergeDeletesPctAllowed = indexSettings.getValue(INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING); // percentage
        ByteSizeValue floorSegment = indexSettings.getValue(INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING);
        int maxMergeAtOnce = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING);
        // TODO is this really a good default number for max_merge_segment, what happens for large indices,
        // won't they end up with many segments?
        ByteSizeValue maxMergedSegment = indexSettings.getValue(INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING);
        double segmentsPerTier = indexSettings.getValue(INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING);
        double reclaimDeletesWeight = indexSettings.getValue(INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING);
        double deletesPctAllowed = indexSettings.getValue(INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING);
        this.mergesEnabled = indexSettings.getSettings().getAsBoolean(INDEX_MERGE_ENABLED, true);
        if (mergesEnabled == false) {
            logger.warn(
                "[{}] is set to false, this should only be used in tests and can cause serious problems in production" + " environments",
                INDEX_MERGE_ENABLED
            );
        }

        tieredMergePolicy.setNoCFSRatio(indexSettings.getValue(INDEX_COMPOUND_FORMAT_SETTING));
        tieredMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
        tieredMergePolicy.setFloorSegmentMB(floorSegment.getMbFrac());
        tieredMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        tieredMergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
        tieredMergePolicy.setSegmentsPerTier(segmentsPerTier);
        tieredMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
    }

    void setSegmentsPerTier(Double segmentsPerTier) {
        tieredMergePolicy.setSegmentsPerTier(segmentsPerTier);
    }

    void setMaxMergedSegment(ByteSizeValue maxMergedSegment) {
        tieredMergePolicy.setMaxMergedSegmentMB(maxMergedSegment.getMbFrac());
    }

    void setMaxMergesAtOnce(Integer maxMergeAtOnce) {
        tieredMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
    }

    /**
     * Update the value for maxMergesAtOnce in TieredMergePolicy used by engine to default value.
     * This would happen if index level override is being removed and we need to fallback to cluster level default
     */
    void setMaxMergesAtOnceToDefault() {
        tieredMergePolicy.setMaxMergeAtOnce(defaultMaxMergeAtOnce);
    }

    /**
     * Update the default value for maxMergesAtOnce. It is used when index level override is not present
     */
    void setDefaultMaxMergesAtOnce(Integer defaultMaxMergesAtOnce) {
        this.defaultMaxMergeAtOnce = defaultMaxMergesAtOnce;
    }

    void setFloorSegmentSetting(ByteSizeValue floorSegementSetting) {
        tieredMergePolicy.setFloorSegmentMB(floorSegementSetting.getMbFrac());
    }

    void setExpungeDeletesAllowed(Double value) {
        tieredMergePolicy.setForceMergeDeletesPctAllowed(value);
    }

    void setNoCFSRatio(Double noCFSRatio) {
        tieredMergePolicy.setNoCFSRatio(noCFSRatio);
    }

    void setDeletesPctAllowed(Double deletesPctAllowed) {
        tieredMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
    }

    public MergePolicy getMergePolicy() {
        return mergesEnabled ? tieredMergePolicy : NoMergePolicy.INSTANCE;
    }

    public static double parseNoCFSRatio(String noCFSRatio) {
        noCFSRatio = noCFSRatio.trim();
        if (noCFSRatio.equalsIgnoreCase("true")) {
            return 1.0d;
        } else if (noCFSRatio.equalsIgnoreCase("false")) {
            return 0.0;
        } else {
            try {
                double value = Double.parseDouble(noCFSRatio);
                if (value < 0.0 || value > 1.0) {
                    throw new IllegalArgumentException("NoCFSRatio must be in the interval [0..1] but was: [" + value + "]");
                }
                return value;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(
                    "Expected a boolean or a value in the interval [0..1] but was: " + "[" + noCFSRatio + "]",
                    ex
                );
            }
        }
    }

    @Override
    public String toString() {
        return "TieredMergePolicyProvider{"
            + "expungeDeletesAllowed="
            + tieredMergePolicy.getForceMergeDeletesPctAllowed()
            + ", floorSegment="
            + tieredMergePolicy.getFloorSegmentMB()
            + ", maxMergeAtOnce="
            + tieredMergePolicy.getMaxMergeAtOnce()
            + ", maxMergedSegment="
            + tieredMergePolicy.getMaxMergedSegmentMB()
            + ", segmentsPerTier="
            + tieredMergePolicy.getSegmentsPerTier()
            + ", deletesPctAllowed="
            + tieredMergePolicy.getDeletesPctAllowed()
            + '}';
    }

}
