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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
     * Setting name for WorkloadGroupService node duress streak
     */
    public static final String QUERYGROUP_DURESS_STREAK_SETTING_NAME = "wlm.workload_group.duress_streak";
    private int duressStreak;
    public static final Setting<Integer> QUERYGROUP_SERVICE_DURESS_STREAK_SETTING = Setting.intSetting(
        QUERYGROUP_DURESS_STREAK_SETTING_NAME,
        3,
        3,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting name for Workload Group Service run interval
     */
    public static final String QUERYGROUP_ENFORCEMENT_INTERVAL_SETTING_NAME = "wlm.workload_group.enforcement_interval";

    private TimeValue workloadGroupServiceRunInterval;
    /**
     * Setting to control the run interval of Workload Group Service
     */
    public static final Setting<Long> QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING = Setting.longSetting(
        QUERYGROUP_ENFORCEMENT_INTERVAL_SETTING_NAME,
        DEFAULT_QUERYGROUP_SERVICE_RUN_INTERVAL_MILLIS,
        1000,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * WLM mode setting name
     */
    public static final String WLM_MODE_SETTING_NAME = "wlm.workload_group.mode";

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
     * Setting name for node level memory based rejection threshold for WorkloadGroup service
     */
    public static final String NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME = "wlm.workload_group.node.memory_rejection_threshold";
    /**
     * Setting to control the memory based rejection threshold
     */
    public static final Setting<Double> NODE_LEVEL_MEMORY_REJECTION_THRESHOLD = Setting.doubleSetting(
        NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_MEMORY_REJECTION_THRESHOLD,
        NodeLevelThresholdValidator.forRejectionThreshold(
            NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_LEVEL_MEMORY_REJECTION_THRESHOLD_MAX_VALUE,
            () -> WorkloadManagementSettings.NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD
        ),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level cpu based rejection threshold for WorkloadGroup service
     */
    public static final String NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME = "wlm.workload_group.node.cpu_rejection_threshold";
    /**
     * Setting to control the cpu based rejection threshold
     */
    public static final Setting<Double> NODE_LEVEL_CPU_REJECTION_THRESHOLD = Setting.doubleSetting(
        NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_CPU_REJECTION_THRESHOLD,
        NodeLevelThresholdValidator.forRejectionThreshold(
            NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
            NODE_LEVEL_CPU_REJECTION_THRESHOLD_MAX_VALUE,
            () -> WorkloadManagementSettings.NODE_LEVEL_CPU_CANCELLATION_THRESHOLD
        ),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level memory based cancellation threshold for WorkloadGroup service
     */
    public static final String NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME = "wlm.workload_group.node.memory_cancellation_threshold";
    /**
     * Setting to control the memory based cancellation threshold
     */
    public static final Setting<Double> NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD = Setting.doubleSetting(
        NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD,
        NodeLevelThresholdValidator.forCancellationThreshold(
            NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME,
            NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD_MAX_VALUE,
            () -> WorkloadManagementSettings.NODE_LEVEL_MEMORY_REJECTION_THRESHOLD
        ),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /**
     * Setting name for node level cpu based cancellation threshold for WorkloadGroup service
     */
    public static final String NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME = "wlm.workload_group.node.cpu_cancellation_threshold";
    /**
     * Setting to control the cpu based cancellation threshold
     */
    public static final Setting<Double> NODE_LEVEL_CPU_CANCELLATION_THRESHOLD = Setting.doubleSetting(
        NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME,
        DEFAULT_NODE_LEVEL_CPU_CANCELLATION_THRESHOLD,
        NodeLevelThresholdValidator.forCancellationThreshold(
            NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME,
            NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE,
            () -> WorkloadManagementSettings.NODE_LEVEL_CPU_REJECTION_THRESHOLD
        ),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * WorkloadGroup service settings constructor
     * @param settings - WorkloadGroup service settings
     * @param clusterSettings - WorkloadGroup cluster settings
     */
    public WorkloadManagementSettings(Settings settings, ClusterSettings clusterSettings) {
        this.wlmMode = WLM_MODE_SETTING.get(settings);
        nodeLevelMemoryCancellationThreshold = NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelMemoryRejectionThreshold = NODE_LEVEL_MEMORY_REJECTION_THRESHOLD.get(settings);
        nodeLevelCpuCancellationThreshold = NODE_LEVEL_CPU_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelCpuRejectionThreshold = NODE_LEVEL_CPU_REJECTION_THRESHOLD.get(settings);
        this.workloadGroupServiceRunInterval = TimeValue.timeValueMillis(QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING.get(settings));
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
        clusterSettings.addSettingsUpdateConsumer(QUERYGROUP_SERVICE_RUN_INTERVAL_SETTING, this::setWorkloadGroupServiceRunInterval);
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
    private void setDuressStreak(int duressStreak) {
        this.duressStreak = duressStreak;
    }

    /**
     * workloadGroupServiceRunInterval setter
     * @param newIntervalInMillis new value
     */
    private void setWorkloadGroupServiceRunInterval(long newIntervalInMillis) {
        this.workloadGroupServiceRunInterval = TimeValue.timeValueMillis(newIntervalInMillis);
    }

    /**
     * workloadGroupServiceRunInterval getter
     * @return current workloadGroupServiceRunInterval value
     */
    public TimeValue getWorkloadGroupServiceRunInterval() {
        return this.workloadGroupServiceRunInterval;
    }

    /**
     * WlmMode setter
     * @param mode new mode value
     */
    private void setWlmMode(final WlmMode mode) {
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
     * Method to set the node level memory based cancellation threshold. This runs as the settings-update consumer at
     * apply time; the value has already been checked by {@link NodeLevelThresholdValidator} during settings-update
     * validation, so the setter only assigns.
     * @param nodeLevelMemoryCancellationThreshold sets the new node level memory based cancellation threshold
     */
    public void setNodeLevelMemoryCancellationThreshold(Double nodeLevelMemoryCancellationThreshold) {
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
     * Method to set the node level cpu based cancellation threshold. The value is validated by
     * {@link NodeLevelThresholdValidator} at settings-update validation time; the setter only assigns.
     * @param nodeLevelCpuCancellationThreshold sets the new node level cpu based cancellation threshold
     */
    public void setNodeLevelCpuCancellationThreshold(Double nodeLevelCpuCancellationThreshold) {
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
     * Method to set the node level memory based rejection threshold. The value is validated by
     * {@link NodeLevelThresholdValidator} at settings-update validation time; the setter only assigns.
     * @param nodeLevelMemoryRejectionThreshold sets the new memory based rejection threshold
     */
    public void setNodeLevelMemoryRejectionThreshold(Double nodeLevelMemoryRejectionThreshold) {
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
     * Method to set the node level cpu based rejection threshold. The value is validated by
     * {@link NodeLevelThresholdValidator} at settings-update validation time; the setter only assigns.
     * @param nodeLevelCpuRejectionThreshold sets the new cpu based rejection threshold
     */
    public void setNodeLevelCpuRejectionThreshold(Double nodeLevelCpuRejectionThreshold) {
        this.nodeLevelCpuRejectionThreshold = nodeLevelCpuRejectionThreshold;
    }

    /**
     * Method to validate that a threshold does not exceed its allowed maximum.
     * @param thresholdValue the threshold value being set
     * @param maxValue the maximum allowed value for this threshold
     * @param thresholdSettingName name of the threshold setting
     * @throws IllegalArgumentException if the value is greater than the allowed maximum
     */
    private static void ensureThresholdIsNotGreaterThanMax(Double thresholdValue, double maxValue, String thresholdSettingName) {
        if (Double.compare(thresholdValue, maxValue) > 0) {
            throw new IllegalArgumentException(
                thresholdSettingName + " value cannot be greater than " + maxValue + " as it can result in a node drop"
            );
        }
    }

    /**
     * Method to validate that a threshold is not negative.
     * @param thresholdValue the threshold value being set
     * @param thresholdSettingName name of the threshold setting
     * @throws IllegalArgumentException if the value is less than 0
     */
    private static void ensureThresholdIsNotNegative(Double thresholdValue, String thresholdSettingName) {
        if (Double.compare(thresholdValue, 0.0) < 0) {
            throw new IllegalArgumentException(thresholdSettingName + " value cannot be negative");
        }
    }

    /**
     * Method to validate that the cancellation threshold is greater than or equal to rejection threshold
     * @param nodeLevelRejectionThreshold rejection threshold to be compared
     * @param nodeLevelCancellationThreshold cancellation threshold to be compared
     * @param rejectionThresholdSettingName name of the rejection threshold setting
     * @param cancellationThresholdSettingName name of the cancellation threshold setting
     * @throws IllegalArgumentException if cancellation threshold is less than rejection threshold
     */
    private static void ensureRejectionThresholdIsLessThanCancellation(
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

    /**
     * Validator shared by all four node-level threshold settings. Enforced at settings-update validation time so that an
     * invalid value is rejected before the new cluster state is published, rather than throwing while the state is applied
     * (which would destabilize the cluster-manager).
     * <p>
     * It enforces two invariants: the value is within {@code [0, max]} (single-setting), and the rejection threshold does
     * not exceed the cancellation threshold (cross-setting). The paired setting is supplied lazily via a {@link Supplier}
     * because the four settings reference each other and are initialized in sequence; resolving it inside the method
     * bodies (which only run at validation time) avoids a forward-reference to a not-yet-initialized field.
     */
    static final class NodeLevelThresholdValidator implements Setting.Validator<Double> {
        private final String settingName;
        private final double maxValue;
        private final Supplier<Setting<Double>> pairedSetting;
        private final boolean isCancellation;

        private NodeLevelThresholdValidator(
            String settingName,
            double maxValue,
            Supplier<Setting<Double>> pairedSetting,
            boolean isCancellation
        ) {
            this.settingName = settingName;
            this.maxValue = maxValue;
            this.pairedSetting = pairedSetting;
            this.isCancellation = isCancellation;
        }

        /**
         * Builds a validator for a cancellation threshold (the upper bound of the pair).
         * @param settingName this cancellation setting's key
         * @param maxValue the maximum allowed value for this cancellation threshold
         * @param rejectionSetting supplier of the paired rejection setting
         */
        static NodeLevelThresholdValidator forCancellationThreshold(
            String settingName,
            double maxValue,
            Supplier<Setting<Double>> rejectionSetting
        ) {
            return new NodeLevelThresholdValidator(settingName, maxValue, rejectionSetting, true);
        }

        /**
         * Builds a validator for a rejection threshold (the lower bound of the pair).
         * @param settingName this rejection setting's key
         * @param maxValue the maximum allowed value for this rejection threshold
         * @param cancellationSetting supplier of the paired cancellation setting
         */
        static NodeLevelThresholdValidator forRejectionThreshold(
            String settingName,
            double maxValue,
            Supplier<Setting<Double>> cancellationSetting
        ) {
            return new NodeLevelThresholdValidator(settingName, maxValue, cancellationSetting, false);
        }

        @Override
        public void validate(Double value) {
            ensureThresholdIsNotNegative(value, settingName);
            ensureThresholdIsNotGreaterThanMax(value, maxValue, settingName);
        }

        @Override
        public void validate(Double value, Map<Setting<?>, Object> settings) {
            final String pairedName = pairedSetting.get().getKey();
            final Double pairedValue = (Double) settings.get(pairedSetting.get());
            // Substitute this setting's incoming value for its own side of the rejection <= cancellation comparison.
            if (isCancellation) {
                ensureRejectionThresholdIsLessThanCancellation(pairedValue, value, pairedName, settingName);
            } else {
                ensureRejectionThresholdIsLessThanCancellation(value, pairedValue, settingName, pairedName);
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(pairedSetting.get()).iterator();
        }
    }
}
