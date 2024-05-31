/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.Rounding;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.FeatureFlags;

import java.util.Arrays;
import java.util.List;

/**
 * Cluster level settings which configures defaults for composite index
 */
@ExperimentalApi
public class CompositeIndexSettings {
    /**
     * This cluster level setting determines whether composite index is enabled or not
     */
    public static final Setting<Boolean> COMPOSITE_INDEX_ENABLED_SETTING = Setting.boolSetting(
        "indices.composite_index.enabled",
        false,
        value -> {
            if (FeatureFlags.isEnabled(FeatureFlags.COMPOSITE_INDEX_SETTING) == false && value == true) {
                throw new IllegalArgumentException(
                    "star tree index is under an experimental feature and can be activated only by enabling "
                        + FeatureFlags.COMPOSITE_INDEX_SETTING.getKey()
                        + " feature flag in the JVM options"
                );
            }
        },
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting determines the max number of composite fields that can be part of composite index config. For each
     * composite field, we will generate associated composite index. (eg : star tree index per field )
     */
    public static final Setting<Integer> COMPOSITE_INDEX_MAX_FIELDS_SETTING = Setting.intSetting(
        "indices.composite_index.max_fields",
        1,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting determines the max number of dimensions that can be part of composite index field. Number of
     * dimensions and associated cardinality has direct effect of composite index size and query performance.
     */
    public static final Setting<Integer> COMPOSITE_INDEX_MAX_DIMENSIONS_SETTING = Setting.intSetting(
        "indices.composite_index.field.max_dimensions",
        10,
        2,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * This setting configures the default "maxLeafDocs" setting of star tree. This affects both query performance and
     * star tree index size. Lesser the leaves, better the query latency but higher storage size and vice versa
     * <p>
     * We can remove this later or change it to an enum based constant setting.
     *
     * @opensearch.experimental
     */
    public static final Setting<Integer> STAR_TREE_DEFAULT_MAX_LEAF_DOCS = Setting.intSetting(
        "indices.composite_index.startree.default.max_leaf_docs",
        10000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default intervals for date dimension as part of composite fields
     */
    public static final Setting<List<Rounding.DateTimeUnit>> DEFAULT_DATE_INTERVALS = Setting.listSetting(
        "indices.composite_index.field.default.date_intervals",
        Arrays.asList(Rounding.DateTimeUnit.MINUTES_OF_HOUR.shortName(), Rounding.DateTimeUnit.HOUR_OF_DAY.shortName()),
        CompositeIndexConfig::getTimeUnit,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<List<MetricType>> DEFAULT_METRICS_LIST = Setting.listSetting(
        "indices.composite_index.field.default.metrics",
        Arrays.asList(
            MetricType.AVG.toString(),
            MetricType.COUNT.toString(),
            MetricType.SUM.toString(),
            MetricType.MAX.toString(),
            MetricType.MIN.toString()
        ),
        MetricType::fromTypeName,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile int maxLeafDocs;

    private volatile List<Rounding.DateTimeUnit> defaultDateIntervals;

    private volatile List<MetricType> defaultMetrics;
    private volatile int maxDimensions;
    private volatile int maxFields;
    private volatile boolean enabled;

    public CompositeIndexSettings(ClusterSettings clusterSettings) {
        this.setMaxLeafDocs(clusterSettings.get(STAR_TREE_DEFAULT_MAX_LEAF_DOCS));
        this.setDefaultDateIntervals(clusterSettings.get(DEFAULT_DATE_INTERVALS));
        this.setDefaultMetrics(clusterSettings.get(DEFAULT_METRICS_LIST));
        this.setMaxDimensions(clusterSettings.get(COMPOSITE_INDEX_MAX_DIMENSIONS_SETTING));
        this.setMaxFields(clusterSettings.get(COMPOSITE_INDEX_MAX_FIELDS_SETTING));
        this.setEnabled(clusterSettings.get(COMPOSITE_INDEX_ENABLED_SETTING));

        clusterSettings.addSettingsUpdateConsumer(STAR_TREE_DEFAULT_MAX_LEAF_DOCS, this::setMaxLeafDocs);
        clusterSettings.addSettingsUpdateConsumer(DEFAULT_DATE_INTERVALS, this::setDefaultDateIntervals);
        clusterSettings.addSettingsUpdateConsumer(DEFAULT_METRICS_LIST, this::setDefaultMetrics);
        clusterSettings.addSettingsUpdateConsumer(COMPOSITE_INDEX_MAX_DIMENSIONS_SETTING, this::setMaxDimensions);
        clusterSettings.addSettingsUpdateConsumer(COMPOSITE_INDEX_MAX_FIELDS_SETTING, this::setMaxFields);
        clusterSettings.addSettingsUpdateConsumer(COMPOSITE_INDEX_ENABLED_SETTING, this::setEnabled);
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setMaxLeafDocs(int maxLeafDocs) {
        this.maxLeafDocs = maxLeafDocs;
    }

    public void setDefaultDateIntervals(List<Rounding.DateTimeUnit> defaultDateIntervals) {
        this.defaultDateIntervals = defaultDateIntervals;
    }

    public void setDefaultMetrics(List<MetricType> defaultMetrics) {
        this.defaultMetrics = defaultMetrics;
    }

    public void setMaxDimensions(int maxDimensions) {
        this.maxDimensions = maxDimensions;
    }

    public void setMaxFields(int maxFields) {
        this.maxFields = maxFields;
    }

    public int getMaxDimensions() {
        return maxDimensions;
    }

    public int getMaxFields() {
        return maxFields;
    }

    public int getMaxLeafDocs() {
        return maxLeafDocs;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public List<Rounding.DateTimeUnit> getDefaultDateIntervals() {
        return defaultDateIntervals;
    }

    public List<MetricType> getDefaultMetrics() {
        return defaultMetrics;
    }
}
