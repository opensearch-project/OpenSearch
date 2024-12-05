/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * Cluster level settings for composite indices
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeIndexSettings {
    public static final Setting<Boolean> STAR_TREE_INDEX_ENABLED_SETTING = Setting.boolSetting(
        "indices.composite_index.star_tree.enabled",
        false,
        value -> {
            if (FeatureFlags.isEnabled(FeatureFlags.STAR_TREE_INDEX_SETTING) == false && value == true) {
                throw new IllegalArgumentException(
                    "star tree index is under an experimental feature and can be activated only by enabling "
                        + FeatureFlags.STAR_TREE_INDEX_SETTING.getKey()
                        + " feature flag in the JVM options"
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This sets the max flush threshold size for composite index
     */
    public static final Setting<ByteSizeValue> COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "indices.composite_index.translog.max_flush_threshold_size",
        new ByteSizeValue(512, ByteSizeUnit.MB),
        new ByteSizeValue(128, ByteSizeUnit.MB),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean starTreeIndexCreationEnabled;

    public CompositeIndexSettings(Settings settings, ClusterSettings clusterSettings) {
        this.starTreeIndexCreationEnabled = STAR_TREE_INDEX_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(STAR_TREE_INDEX_ENABLED_SETTING, this::starTreeIndexCreationEnabled);
    }

    private void starTreeIndexCreationEnabled(boolean value) {
        this.starTreeIndexCreationEnabled = value;
    }

    public boolean isStarTreeIndexCreationEnabled() {
        return starTreeIndexCreationEnabled;
    }
}
