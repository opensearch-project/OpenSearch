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
        new NodeLevelMemoryRejectionThresholdValidator(),
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
        new NodeLevelCpuRejectionThresholdValidator(),
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
        new NodeLevelMemoryCancellationThresholdValidator(),
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
        new NodeLevelCpuCancellationThresholdValidator(),
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
     * Method to set the node level memory based cancellation threshold
     * @param nodeLevelMemoryCancellationThreshold sets the new node level memory based cancellation threshold
     * @throws IllegalArgumentException if the value is negative or &gt; 0.95
     */
    public void setNodeLevelMemoryCancellationThreshold(Double nodeLevelMemoryCancellationThreshold) {
        // NOTE: the rejection <= cancellation ordering invariant is intentionally NOT checked here. This setter runs as
        // the settings-update consumer at apply time, one setting at a time, so the sibling field may still hold its
        // previous value; checking ordering here throws when both thresholds are lowered together (a consistent final
        // state), destabilizing the cluster-manager. Ordering is enforced at validation time by the Setting.Validator
        // against the final, consistent settings, and at startup by the constructor.
        ensureThresholdIsWithinAllowedRange(
            nodeLevelMemoryCancellationThreshold,
            NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD_MAX_VALUE,
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
     * @throws IllegalArgumentException if the value is negative or &gt; 0.95
     */
    public void setNodeLevelCpuCancellationThreshold(Double nodeLevelCpuCancellationThreshold) {
        // See setNodeLevelMemoryCancellationThreshold: ordering is enforced by the Validator at validation time, not here.
        ensureThresholdIsWithinAllowedRange(
            nodeLevelCpuCancellationThreshold,
            NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE,
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
     * @throws IllegalArgumentException if the value is negative or &gt; 0.90
     */
    public void setNodeLevelMemoryRejectionThreshold(Double nodeLevelMemoryRejectionThreshold) {
        // See setNodeLevelMemoryCancellationThreshold: ordering is enforced by the Validator at validation time, not here.
        ensureThresholdIsWithinAllowedRange(
            nodeLevelMemoryRejectionThreshold,
            NODE_LEVEL_MEMORY_REJECTION_THRESHOLD_MAX_VALUE,
            NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME
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
     * @throws IllegalArgumentException if the value is negative or &gt; 0.90
     */
    public void setNodeLevelCpuRejectionThreshold(Double nodeLevelCpuRejectionThreshold) {
        // See setNodeLevelMemoryCancellationThreshold: ordering is enforced by the Validator at validation time, not here.
        ensureThresholdIsWithinAllowedRange(
            nodeLevelCpuRejectionThreshold,
            NODE_LEVEL_CPU_REJECTION_THRESHOLD_MAX_VALUE,
            NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME
        );
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
     * Runs every single-setting validation for a threshold (i.e. the checks that do not depend on a sibling setting):
     * the value must be non-negative and must not exceed its allowed maximum. This is the single place both enforcement
     * paths call — the settings-update {@link Setting.Validator} (validation time) and the setters (apply time) — so the
     * two cannot drift as new standalone bounds are added.
     * @param thresholdValue the threshold value being set
     * @param maxValue the maximum allowed value for this threshold
     * @param thresholdSettingName name of the threshold setting
     * @throws IllegalArgumentException if the value is negative or greater than the allowed maximum
     */
    private static void ensureThresholdIsWithinAllowedRange(Double thresholdValue, double maxValue, String thresholdSettingName) {
        ensureThresholdIsNotNegative(thresholdValue, thresholdSettingName);
        ensureThresholdIsNotGreaterThanMax(thresholdValue, maxValue, thresholdSettingName);
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
     * Validator for {@link #NODE_LEVEL_CPU_CANCELLATION_THRESHOLD}. Enforced at settings-update validation time so that an
     * invalid value is rejected before the new cluster state is published, rather than throwing while the state is applied
     * (which would destabilize the cluster-manager).
     */
    static final class NodeLevelCpuCancellationThresholdValidator implements Setting.Validator<Double> {
        @Override
        public void validate(Double value) {
            ensureThresholdIsWithinAllowedRange(
                value,
                NODE_LEVEL_CPU_CANCELLATION_THRESHOLD_MAX_VALUE,
                NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public void validate(Double value, Map<Setting<?>, Object> settings) {
            final Double rejectionThreshold = (Double) settings.get(NODE_LEVEL_CPU_REJECTION_THRESHOLD);
            ensureRejectionThresholdIsLessThanCancellation(
                rejectionThreshold,
                value,
                NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
                NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(NODE_LEVEL_CPU_REJECTION_THRESHOLD).iterator();
        }
    }

    /**
     * Validator for {@link #NODE_LEVEL_CPU_REJECTION_THRESHOLD}. See {@link NodeLevelCpuCancellationThresholdValidator}.
     */
    static final class NodeLevelCpuRejectionThresholdValidator implements Setting.Validator<Double> {
        @Override
        public void validate(Double value) {
            ensureThresholdIsWithinAllowedRange(
                value,
                NODE_LEVEL_CPU_REJECTION_THRESHOLD_MAX_VALUE,
                NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public void validate(Double value, Map<Setting<?>, Object> settings) {
            final Double cancellationThreshold = (Double) settings.get(NODE_LEVEL_CPU_CANCELLATION_THRESHOLD);
            ensureRejectionThresholdIsLessThanCancellation(
                value,
                cancellationThreshold,
                NODE_CPU_REJECTION_THRESHOLD_SETTING_NAME,
                NODE_CPU_CANCELLATION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(NODE_LEVEL_CPU_CANCELLATION_THRESHOLD).iterator();
        }
    }

    /**
     * Validator for {@link #NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD}. See {@link NodeLevelCpuCancellationThresholdValidator}.
     */
    static final class NodeLevelMemoryCancellationThresholdValidator implements Setting.Validator<Double> {
        @Override
        public void validate(Double value) {
            ensureThresholdIsWithinAllowedRange(
                value,
                NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD_MAX_VALUE,
                NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public void validate(Double value, Map<Setting<?>, Object> settings) {
            final Double rejectionThreshold = (Double) settings.get(NODE_LEVEL_MEMORY_REJECTION_THRESHOLD);
            ensureRejectionThresholdIsLessThanCancellation(
                rejectionThreshold,
                value,
                NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
                NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(NODE_LEVEL_MEMORY_REJECTION_THRESHOLD).iterator();
        }
    }

    /**
     * Validator for {@link #NODE_LEVEL_MEMORY_REJECTION_THRESHOLD}. See {@link NodeLevelCpuCancellationThresholdValidator}.
     */
    static final class NodeLevelMemoryRejectionThresholdValidator implements Setting.Validator<Double> {
        @Override
        public void validate(Double value) {
            ensureThresholdIsWithinAllowedRange(
                value,
                NODE_LEVEL_MEMORY_REJECTION_THRESHOLD_MAX_VALUE,
                NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public void validate(Double value, Map<Setting<?>, Object> settings) {
            final Double cancellationThreshold = (Double) settings.get(NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD);
            ensureRejectionThresholdIsLessThanCancellation(
                value,
                cancellationThreshold,
                NODE_MEMORY_REJECTION_THRESHOLD_SETTING_NAME,
                NODE_MEMORY_CANCELLATION_THRESHOLD_SETTING_NAME
            );
        }

        @Override
        public Iterator<Setting<?>> settings() {
            return List.<Setting<?>>of(NODE_LEVEL_MEMORY_CANCELLATION_THRESHOLD).iterator();
        }
    }
}
