/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Main class to declare Workload Management related settings
 */
@PublicApi(since = "2.18.0")
public class WorkloadManagementSettings {
    private static final Double DEFAULT_NODE_LEVEL_MEMORY_REJECTION_THRESHOLD = 0.8;
    private static final Double DEFAULT_NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD = 0.9;
    private static final Double DEFAULT_NODE_LEVEL_CPU_REJECTION_THRESHOLD = 0.8;
    private static final Double DEFAULT_NODE_LEVEL_CPU_CANCELLATION_THRESHOLD = 0.9;
    private static final Long DEFAULT_QUERYGROUP_SERVICE_RUN_INTERVAL_MILLIS = 1000L;
    public static final double NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD_MAX_VALUE = 0.95;
    public static final double NODE_LEVEL_MEMORY_REJECTION_THRESHOLD_MAX_VALUE = 0.9;
    public static final double NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE = 0.95;
    public static final double NODE_LEVEL_CPU_REJECTION_THRESHOLD_MAX_VALUE = 0.9;
    public static final String DEFAULT_WLM_MODE = "monitor_only";

    private Double nodeLevelMemoryCancellationThreshold;
    private Double nodeLevelMemoryRejectionThreshold;
    private Double nodeLevelCpuCancellationThreshold;
    private Double nodeLevelCpuRejectionThreshold;


    /**
     * Setting name for QueryGroupService node duress streak
     */
    public static final String QUERYGROUP_SERVICE_DURESS_STREAK_SETTING_NAME = "wlm.query_group.service.duress_streak";
    private int duressStreak;
    public static final Setting<Integer> QUERYGROUP_SERVICE_DURESS_STREAK_SETTING = Setting.intSetting(
        QUERYGROUP_SERVICE_DURESS_STREAK_SETTING_NAME,
        3,
        3,
        Setting.Property.NodeScope
    );

    /**
     * Setting name for Query Group Service run interval
     */
    public static final String QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING_NAME = "wlm.query_group.service.run_interval";

    private TimeValue queryGroupServiceRunInterval;
    /**
     * Setting to control the run interval of Query Group Service
     */
    public static final Setting<Long> QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING = Setting.longSetting(
        QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING_NAME,
        DEFAULT_QUERYGROUP_SERVICE_RUN_INTERVAL_MILLIS,
        1000,
        Setting.Property.NodeScope
    );

    /**
     * WLM mode setting name
     */
    public static final String WLM_MODE_SETTING_NAME = "wlm.query_group.mode";

    private volatile WlmMode wlmMode;

    /**
     * WLM mode setting, which determines which mode WLM is operating in
     */
    public static final Setting<WlmMode> WLM_MODE_SETTING = new Setting<WlmMode>(
        WLM_MODE_SETTING_NAME,
        DEFAULT_WLM_MODE,
        WlmMode::fromName,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting name for node level memory based rejection threshold for QueryGroup service
     */
    public static final String NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME = "wlm.query_group.node.memory_rejection_threshold";
    /**
     * Setting to control the memory based rejection threshold
     */
    public static final Setting<Double> NODE_LEVEL_MEMORY_REJECTION_THRESHOLD = Setting.doubleSetting(
        NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_MEMORY_REJECTION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level cpu based rejection threshold for QueryGroup service
     */
    public static final String NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME = "wlm.query_group.node.cpu_rejection_threshold";
    /**
     * Setting to control the cpu based rejection threshold
     */
    public static final Setting<Double> NODE_LEVEL_CPU_REJECTION_THRESHOLD = Setting.doubleSetting(
        NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_CPU_REJECTION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level memory based cancellation threshold for QueryGroup service
     */
    public static final String NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME = "wlm.query_group.node.memory_cancellation_threshold";
    /**
     * Setting to control the memory based cancellation threshold
     */
    public static final Setting<Double> NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD = Setting.doubleSetting(
        NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level cpu based cancellation threshold for QueryGroup service
     */
    public static final String NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME = "wlm.query_group.node.cpu_cancellation_threshold";
    /**
     * Setting to control the cpu based cancellation threshold
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
    public WorkloadManagementSettings(Settings settings, ClusterSettings clusterSettings) {
        this.wlmMode = WLM_MODE_SETTING.get(settings);
        nodeLevelMemoryCancellationThreshold = NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelMemoryRejectionThreshold = NODE_LEVEL_MEMORY_REJECTION_THRESHOLD.get(settings);
        nodeLevelCpuCancellationThreshold = NODE_LEVEL_CPU_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelCpuRejectionThreshold = NODE_LEVEL_CPU_REJECTION_THRESHOLD.get(settings);
        this.queryGroupServiceRunInterval = TimeValue.timeValueMillis(QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING.get(settings));
        duressStreak = QUERYGROUP_SERVICE_DURESS_STREAK_SETTING.get(settings);

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
        clusterSettings.addSettingsUpdateConsumer(WLM_MODE_SETTING, this::setWlmMode);
        clusterSettings.addSettingsUpdateConsumer(QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING, this::setQueryGroupServiceRunInterval);
        clusterSettings.addSettingsUpdateConsumer(QUERYGROUP_SERVICE_DURESS_STREAK_SETTING, this::setDuressStreak);
    }


    /**
     * node duress streak getter
     * @return current duressStreak value
     */
    public int getDuressStreak() {
        return duressStreak;
    }

    /**
     * node duress streak setter
     * @param duressStreak new value
     */
    public void setDuressStreak(int duressStreak) {
        this.duressStreak = duressStreak;
    }

    /**
     * queryGroupServiceRunInterval setter
     * @param newIntervalInMillis new value
     */
    public void setQueryGroupServiceRunInterval(long newIntervalInMillis) {
        this.queryGroupServiceRunInterval = TimeValue.timeValueMillis(newIntervalInMillis);
    }

    /**
     * queryGroupServiceRunInterval getter
     * @return current queryGroupServiceRunInterval value
     */
    public TimeValue getQueryGroupServiceRunInterval() {
        return this.queryGroupServiceRunInterval;
    }

    /**
     * WlmMode setter
     * @param mode new mode value
     */
    public void setWlmMode(final WlmMode mode) {
        this.wlmMode = mode;
    }

    /**
     * WlmMode getter
     * @return the current wlmMode
     */
    public WlmMode getWlmMode() {
        return this.wlmMode;
    }

    /**
     * Method to get the node level memory based cancellation threshold
     * @return current node level memory based cancellation threshold
     */
    public Double getNodeLevelMemoryCancellationThreshold() {
        return nodeLevelMemoryCancellationThreshold;
    }

    /**
     * Method to set the node level memory based cancellation threshold
     * @param nodeLevelMemoryCancellationThreshold sets the new node level memory based cancellation threshold
     * @throws IllegalArgumentException if the value is &gt; 0.95 and cancellation &lt; rejection threshold
     */
    public void setNodeLevelMemoryCancellationThreshold(Double nodeLevelMemoryCancellationThreshold) {
        if (Double.compare(nodeLevelMemoryCancellationThreshold, NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME + " value cannot be greater than 0.95 as it can result in a node drop"
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
     * Method to get the node level cpu based cancellation threshold
     * @return current node level cpu based cancellation threshold
     */
    public Double getNodeLevelCpuCancellationThreshold() {
        return nodeLevelCpuCancellationThreshold;
    }

    /**
     * Method to set the node level cpu based cancellation threshold
     * @param nodeLevelCpuCancellationThreshold sets the new node level cpu based cancellation threshold
     * @throws IllegalArgumentException if the value is &gt; 0.95 and cancellation &lt; rejection threshold
     */
    public void setNodeLevelCpuCancellationThreshold(Double nodeLevelCpuCancellationThreshold) {
        if (Double.compare(nodeLevelCpuCancellationThreshold, NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME + " value cannot be greater than 0.95 as it can result in a node drop"
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
     * Method to get the memory based node level rejection threshold
     * @return the current memory based node level rejection threshold
     */
    public Double getNodeLevelMemoryRejectionThreshold() {
        return nodeLevelMemoryRejectionThreshold;
    }

    /**
     * Method to set the node level memory based rejection threshold
     * @param nodeLevelMemoryRejectionThreshold sets the new memory based rejection threshold
     * @throws IllegalArgumentException if rejection &gt; 0.90 and rejection &lt; cancellation threshold
     */
    public void setNodeLevelMemoryRejectionThreshold(Double nodeLevelMemoryRejectionThreshold) {
        if (Double.compare(nodeLevelMemoryRejectionThreshold, NODE_LEVEL_MEMORY_REJECTION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME + " value cannot be greater than 0.90 as it can result in a node drop"
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
     * Method to get the cpu based node level rejection threshold
     * @return the current cpu based node level rejection threshold
     */
    public Double getNodeLevelCpuRejectionThreshold() {
        return nodeLevelCpuRejectionThreshold;
    }

    /**
     * Method to set the node level cpu based rejection threshold
     * @param nodeLevelCpuRejectionThreshold sets the new cpu based rejection threshold
     * @throws IllegalArgumentException if rejection &gt; 0.90 and rejection &lt; cancellation threshold
     */
    public void setNodeLevelCpuRejectionThreshold(Double nodeLevelCpuRejectionThreshold) {
        if (Double.compare(nodeLevelCpuRejectionThreshold, NODE_LEVEL_CPU_REJECTION_THRESHOLD_MAX_VALUE) > 0) {
            throw new IllegalArgumentException(
                NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME + " value cannot be greater than 0.90 as it can result in a node drop"
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
