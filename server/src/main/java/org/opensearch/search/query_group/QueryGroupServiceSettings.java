/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query_group;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Main class to declare the QueryGroup feature related settings
 */
public class QueryGroupServiceSettings {
    private static final Long DEFAULT_RUN_INTERVAL_MILLIS = 1000l;
    private static final Double DEFAULT_NODE_LEVEL_REJECTION_THRESHOLD = 0.8;
    private static final Double DEFAULT_NODE_LEVEL_CANCELLATION_THRESHOLD = 0.9;
    /**
     * default max queryGroup count on any node at any given point in time
     */
    public static final int DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE = 100;

    public static final String QUERY_GROUP_COUNT_SETTING_NAME = "node.query_group.max_count";
    public static final double NODE_LEVEL_CANCELLATION_THRESHOLD_MAX_VALUE = 0.95;
    public static final double NODE_LEVEL_REJECTION_THRESHOLD_MAX_VALUE = 0.90;

    private TimeValue runIntervalMillis;
    private Double nodeLevelJvmCancellationThreshold;
    private Double nodeLevelJvmRejectionThreshold;
    private volatile int maxQueryGroupCount;
    /**
     *  max QueryGroup count setting
     */
    public static final Setting<Integer> MAX_QUERY_GROUP_COUNT = Setting.intSetting(
        QUERY_GROUP_COUNT_SETTING_NAME,
        DEFAULT_MAX_QUERY_GROUP_COUNT_VALUE,
        0,
        (newVal) -> {
            if (newVal > 100 || newVal < 1) throw new IllegalArgumentException(
                QUERY_GROUP_COUNT_SETTING_NAME + " should be in range [1-100]"
            );
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for default QueryGroup count
     */
    public static final String SERVICE_RUN_INTERVAL_MILLIS_SETTING_NAME = "query_group.service.run_interval_millis";
    /**
     * Setting to control the run interval of QSB service
     */
    private static final Setting<Long> QUERY_GROUP_RUN_INTERVAL_SETTING = Setting.longSetting(
        SERVICE_RUN_INTERVAL_MILLIS_SETTING_NAME,
        DEFAULT_RUN_INTERVAL_MILLIS,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting name for node level rejection threshold for QSB
     */
    public static final String NODE_REJECTION_THRESHOLD_SETTING_NAME = "query_group.node.rejection_threshold";
    /**
     * Setting to control the rejection threshold
     */
    public static final Setting<Double> NODE_LEVEL_REJECTION_THRESHOLD = Setting.doubleSetting(
        NODE_REJECTION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_REJECTION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level cancellation threshold
     */
    public static final String NODE_CANCELLATION_THRESHOLD_SETTING_NAME = "query_group.node.cancellation_threshold";
    /**
     * Setting name for node level cancellation threshold
     */
    public static final Setting<Double> NODE_LEVEL_CANCELLATION_THRESHOLD = Setting.doubleSetting(
        NODE_CANCELLATION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_CANCELLATION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * QueryGroup service settings constructor
     * @param settings
     * @param clusterSettings
     */
    public QueryGroupServiceSettings(Settings settings, ClusterSettings clusterSettings) {
        runIntervalMillis = new TimeValue(QUERY_GROUP_RUN_INTERVAL_SETTING.get(settings));
        nodeLevelJvmCancellationThreshold = NODE_LEVEL_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelJvmRejectionThreshold = NODE_LEVEL_REJECTION_THRESHOLD.get(settings);
        maxQueryGroupCount = MAX_QUERY_GROUP_COUNT.get(settings);

        ensureRejectionThresholdIsLessThanCancellation(nodeLevelJvmRejectionThreshold, nodeLevelJvmCancellationThreshold);

        clusterSettings.addSettingsUpdateConsumer(MAX_QUERY_GROUP_COUNT, this::setMaxQueryGroupCount);
        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_CANCELLATION_THRESHOLD, this::setNodeLevelJvmCancellationThreshold);
        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_REJECTION_THRESHOLD, this::setNodeLevelJvmRejectionThreshold);
    }

    /**
     * Method to get runInterval for QSB
     * @return runInterval in milliseconds for QSB Service
     */
    public TimeValue getRunIntervalMillis() {
        return runIntervalMillis;
    }

    /**
     * Method to set the new QueryGroup count
     * @param newMaxQueryGroupCount is the new maxQueryGroupCount per node
     */
    public void setMaxQueryGroupCount(int newMaxQueryGroupCount) {
        if (newMaxQueryGroupCount < 0) {
            throw new IllegalArgumentException("node.node.query_group.max_count can't be negative");
        }
        this.maxQueryGroupCount = newMaxQueryGroupCount;
    }

    /**
     * Method to get the node level cancellation threshold
     * @return current node level cancellation threshold
     */
    public Double getNodeLevelJvmCancellationThreshold() {
        return nodeLevelJvmCancellationThreshold;
    }

    /**
     * Method to set the node level cancellation threshold
     * @param nodeLevelJvmCancellationThreshold sets the new node level cancellation threshold
     * @throws IllegalArgumentException if the value is &gt; 0.95 and cancellation &lt; rejection threshold
     */
    public void setNodeLevelJvmCancellationThreshold(Double nodeLevelJvmCancellationThreshold) {
        if (Double.compare(nodeLevelJvmCancellationThreshold, NODE_LEVEL_CANCELLATION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_CANCELLATION_THRESHOLD_SETTING_NAME + " value should not be greater than 0.95 as it pose a threat of node drop"
            );
        }

        ensureRejectionThresholdIsLessThanCancellation(nodeLevelJvmRejectionThreshold, nodeLevelJvmCancellationThreshold);

        this.nodeLevelJvmCancellationThreshold = nodeLevelJvmCancellationThreshold;
    }

    /**
     * Method to get the node level rejection threshold
     * @return the current node level rejection threshold
     */
    public Double getNodeLevelJvmRejectionThreshold() {
        return nodeLevelJvmRejectionThreshold;
    }

    /**
     * Method to set the node level rejection threshold
     * @param nodeLevelJvmRejectionThreshold sets the new rejection threshold
     * @throws IllegalArgumentException if rejection &gt; 0.90 and rejection &lt; cancellation threshold
     */
    public void setNodeLevelJvmRejectionThreshold(Double nodeLevelJvmRejectionThreshold) {
        if (Double.compare(nodeLevelJvmRejectionThreshold, NODE_LEVEL_REJECTION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_REJECTION_THRESHOLD_SETTING_NAME + " value not be greater than 0.90 as it pose a threat of node drop"
            );
        }

        ensureRejectionThresholdIsLessThanCancellation(nodeLevelJvmRejectionThreshold, nodeLevelJvmCancellationThreshold);

        this.nodeLevelJvmRejectionThreshold = nodeLevelJvmRejectionThreshold;
    }

    private void ensureRejectionThresholdIsLessThanCancellation(
        Double nodeLevelJvmRejectionThreshold,
        Double nodeLevelJvmCancellationThreshold
    ) {
        if (Double.compare(nodeLevelJvmCancellationThreshold, nodeLevelJvmRejectionThreshold) < 0) {
            throw new IllegalArgumentException(
                NODE_CANCELLATION_THRESHOLD_SETTING_NAME + " value should not be less than " + NODE_REJECTION_THRESHOLD_SETTING_NAME
            );
        }
    }

    /**
     * Method to get the current QueryGroup count
     * @return the current max QueryGroup count
     */
    public int getMaxQueryGroupCount() {
        return maxQueryGroupCount;
    }
}
