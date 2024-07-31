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
    private static final Double DEFAULT_NODE_LEVEL_MEMORY_REJECTION_THRESHOLD = 0.8;
    private static final Double DEFAULT_NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD = 0.9;
    private static final Double DEFAULT_NODE_LEVEL_CPU_REJECTION_THRESHOLD = 0.8;
    private static final Double DEFAULT_NODE_LEVEL_CPU_CANCELLATION_THRESHOLD = 0.9;
    public static final double NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD_MAX_VALUE = 0.95;
    public static final double NODE_LEVEL_MEMORY_REJECTION_THRESHOLD_MAX_VALUE = 0.9;
    public static final double NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE = 0.95;
    public static final double NODE_LEVEL_CPU_REJECTION_THRESHOLD_MAX_VALUE = 0.9;

    private TimeValue runIntervalMillis;
    private Double nodeLevelMemoryCancellationThreshold;
    private Double nodeLevelMemoryRejectionThreshold;
    private Double nodeLevelCpuCancellationThreshold;
    private Double nodeLevelCpuRejectionThreshold;
    /**
     * Setting name for the run interval of QueryGroup service
     */
    public static final String SERVICE_RUN_INTERVAL_MILLIS_SETTING_NAME = "query_group.service.run_interval_millis";
    /**
     * Setting to control the run interval of QueryGroup service
     */
    private static final Setting<Long> QUERY_GROUP_RUN_INTERVAL_SETTING = Setting.longSetting(
        SERVICE_RUN_INTERVAL_MILLIS_SETTING_NAME,
        DEFAULT_RUN_INTERVAL_MILLIS,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting name for node level memory rejection threshold for QueryGroup service
     */
    public static final String NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME = "query_group.node.memory_rejection_threshold";
    /**
     * Setting to control the memory rejection threshold
     */
    public static final Setting<Double> NODE_LEVEL_MEMORY_REJECTION_THRESHOLD = Setting.doubleSetting(
        NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_MEMORY_REJECTION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level cpu rejection threshold for QueryGroup service
     */
    public static final String NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME = "query_group.node.cpu_rejection_threshold";
    /**
     * Setting to control the cpu rejection threshold
     */
    public static final Setting<Double> NODE_LEVEL_CPU_REJECTION_THRESHOLD = Setting.doubleSetting(
        NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_CPU_REJECTION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level memory cancellation threshold for QueryGroup service
     */
    public static final String NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME = "query_group.node.memory_cancellation_threshold";
    /**
     * Setting to control the memory cancellation threshold
     */
    public static final Setting<Double> NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD = Setting.doubleSetting(
        NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level cpu cancellation threshold for QueryGroup service
     */
    public static final String NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME = "query_group.node.cpu_cancellation_threshold";
    /**
     * Setting to control the cpu cancellation threshold
     */
    public static final Setting<Double> NODE_LEVEL_CPU_CANCELLATION_THRESHOLD = Setting.doubleSetting(
        NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_CPU_CANCELLATION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * QueryGroup service settings constructor
     * @param settings - QueryGroup service settings
     * @param clusterSettings - QueryGroup cluster settings
     */
    public QueryGroupServiceSettings(Settings settings, ClusterSettings clusterSettings) {
        runIntervalMillis = new TimeValue(QUERY_GROUP_RUN_INTERVAL_SETTING.get(settings));
        nodeLevelMemoryCancellationThreshold = NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelMemoryRejectionThreshold = NODE_LEVEL_MEMORY_REJECTION_THRESHOLD.get(settings);
        nodeLevelCpuCancellationThreshold = NODE_LEVEL_CPU_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelCpuRejectionThreshold = NODE_LEVEL_CPU_REJECTION_THRESHOLD.get(settings);

        ensureRejectionThresholdIsLessThanCancellation(
            nodeLevelMemoryRejectionThreshold,
            nodeLevelMemoryCancellationThreshold,
            NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME
        );
        ensureRejectionThresholdIsLessThanCancellation(
            nodeLevelCpuRejectionThreshold,
            nodeLevelCpuCancellationThreshold,
            NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME
        );

        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD, this::setNodeLevelMemoryCancellationThreshold);
        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_MEMORY_REJECTION_THRESHOLD, this::setNodeLevelMemoryRejectionThreshold);
        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_CPU_CANCELLATION_THRESHOLD, this::setNodeLevelCpuCancellationThreshold);
        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_CPU_REJECTION_THRESHOLD, this::setNodeLevelCpuRejectionThreshold);
    }

    /**
     * Method to get runInterval for QueryGroup service
     * @return runInterval in milliseconds for QueryGroup service
     */
    public TimeValue getRunIntervalMillis() {
        return runIntervalMillis;
    }

    /**
     * Method to get the node level memory cancellation threshold
     * @return current node level memory cancellation threshold
     */
    public Double getNodeLevelMemoryCancellationThreshold() {
        return nodeLevelMemoryCancellationThreshold;
    }

    /**
     * Method to set the node level memory cancellation threshold
     * @param nodeLevelMemoryCancellationThreshold sets the new node level memory cancellation threshold
     * @throws IllegalArgumentException if the value is &gt; 0.95 and cancellation &lt; rejection threshold
     */
    public void setNodeLevelMemoryCancellationThreshold(Double nodeLevelMemoryCancellationThreshold) {
        if (Double.compare(nodeLevelMemoryCancellationThreshold, NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME + " value should not be greater than 0.95 as it pose a threat of node drop"
            );
        }

        ensureRejectionThresholdIsLessThanCancellation(
            nodeLevelMemoryRejectionThreshold,
            nodeLevelMemoryCancellationThreshold,
            NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME
        );

        this.nodeLevelMemoryCancellationThreshold = nodeLevelMemoryCancellationThreshold;
    }

    /**
     * Method to get the node level cpu cancellation threshold
     * @return current node level cpu cancellation threshold
     */
    public Double getNodeLevelCpuCancellationThreshold() {
        return nodeLevelCpuCancellationThreshold;
    }

    /**
     * Method to set the node level cpu cancellation threshold
     * @param nodeLevelCpuCancellationThreshold sets the new node level cpu cancellation threshold
     * @throws IllegalArgumentException if the value is &gt; 0.95 and cancellation &lt; rejection threshold
     */
    public void setNodeLevelCpuCancellationThreshold(Double nodeLevelCpuCancellationThreshold) {
        if (Double.compare(nodeLevelCpuCancellationThreshold, NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME + " value should not be greater than 0.95 as it pose a threat of node drop"
            );
        }

        ensureRejectionThresholdIsLessThanCancellation(
            nodeLevelCpuRejectionThreshold,
            nodeLevelCpuCancellationThreshold,
            NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME
        );

        this.nodeLevelCpuCancellationThreshold = nodeLevelCpuCancellationThreshold;
    }

    /**
     * Method to get the memory node level rejection threshold
     * @return the current memory node level rejection threshold
     */
    public Double getNodeLevelMemoryRejectionThreshold() {
        return nodeLevelMemoryRejectionThreshold;
    }

    /**
     * Method to set the node level memory rejection threshold
     * @param nodeLevelMemoryRejectionThreshold sets the new memory rejection threshold
     * @throws IllegalArgumentException if rejection &gt; 0.90 and rejection &lt; cancellation threshold
     */
    public void setNodeLevelMemoryRejectionThreshold(Double nodeLevelMemoryRejectionThreshold) {
        if (Double.compare(nodeLevelMemoryRejectionThreshold, NODE_LEVEL_MEMORY_REJECTION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME + " value not be greater than 0.90 as it pose a threat of node drop"
            );
        }

        ensureRejectionThresholdIsLessThanCancellation(
            nodeLevelMemoryRejectionThreshold,
            nodeLevelMemoryCancellationThreshold,
            NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME
        );

        this.nodeLevelMemoryRejectionThreshold = nodeLevelMemoryRejectionThreshold;
    }

    /**
     * Method to get the cpu node level rejection threshold
     * @return the current cpu node level rejection threshold
     */
    public Double getNodeLevelCpuRejectionThreshold() {
        return nodeLevelCpuRejectionThreshold;
    }

    /**
     * Method to set the node level cpu rejection threshold
     * @param nodeLevelCpuRejectionThreshold sets the new cpu rejection threshold
     * @throws IllegalArgumentException if rejection &gt; 0.90 and rejection &lt; cancellation threshold
     */
    public void setNodeLevelCpuRejectionThreshold(Double nodeLevelCpuRejectionThreshold) {
        if (Double.compare(nodeLevelCpuRejectionThreshold, NODE_LEVEL_CPU_REJECTION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME + " value not be greater than 0.90 as it pose a threat of node drop"
            );
        }

        ensureRejectionThresholdIsLessThanCancellation(
            nodeLevelCpuRejectionThreshold,
            nodeLevelCpuCancellationThreshold,
            NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME
        );

        this.nodeLevelCpuRejectionThreshold = nodeLevelCpuRejectionThreshold;
    }

    /**
     * Method to validate that the cancellation threshold is greater than or equal to rejection threshold
     * @param nodeLevelRejectionThreshold rejection threshold to be compared
     * @param nodeLevelCancellationThreshold cancellation threshold to be compared
     * @param rejectionThresholdSettingName name of the rejection threshold setting
     * @param cancellationThresholdSettingName name of the cancellation threshold setting
     * @throws IllegalArgumentException if cancellation threshold is less than rejection threshold
     */
    private void ensureRejectionThresholdIsLessThanCancellation(
        Double nodeLevelRejectionThreshold,
        Double nodeLevelCancellationThreshold,
        String rejectionThresholdSettingName,
        String cancellationThresholdSettingName
    ) {
        if (Double.compare(nodeLevelCancellationThreshold, nodeLevelRejectionThreshold) < 0) {
            throw new IllegalArgumentException(
                cancellationThresholdSettingName + " value should not be less than " + rejectionThresholdSettingName
            );
        }
    }
}
