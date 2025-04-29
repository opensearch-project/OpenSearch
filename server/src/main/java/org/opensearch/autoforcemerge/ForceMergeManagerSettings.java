/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autoforcemerge;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Settings class that manages configuration parameters for the Auto Force Merge functionality.
 * This class handles settings related to force merge operations, including thresholds,
 * timing, and operational parameters for the Force Merge Manager.
 */
public class ForceMergeManagerSettings {

    private Integer segmentCountThreshold;
    private TimeValue forcemergeDelay;
    private TimeValue schedulerInterval;
    private Double cpuThreshold;
    private Double jvmThreshold;
    private Integer forceMergeThreadCount;
    private Integer concurrencyMultiplier;
    private Boolean autoForceMergeFeatureEnabled;

    private final AutoForceMergeManager autoForceMergeManager;

    public static final String AUTO_FORCE_MERGE = "cluster.auto_force_merge_feature.enabled";

    /**
     * Setting to enable Auto Force Merge (default: false)
     */
    public static final Setting<Boolean> AUTO_FORCE_MERGE_SETTING = Setting.boolSetting(
        AUTO_FORCE_MERGE,
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for segment count threshold that triggers force merge (default: 20).
     */
    public static final Setting<Integer> SEGMENT_COUNT_THRESHOLD_FOR_AUTO_FORCE_MERGE = Setting.intSetting(
        "node.auto.force_merge.segment.count",
        1,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for wait time between force merge operations (default: 3s).
     */
    public static final Setting<TimeValue> MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE = Setting.timeSetting(
        "node.auto.force_merge.merge_delay",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueSeconds(15),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for scheduler interval (default: 30 minutes).
     */
    public static final Setting<TimeValue> AUTO_FORCE_MERGE_SCHEDULER_INTERVAL = Setting.timeSetting(
        "node.auto.force_merge.scheduler.interval",
        TimeValue.timeValueMinutes(30),
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueHours(24),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for cpu threshold. (default: 80)
     */
    public static final Setting<Double> CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE = Setting.doubleSetting(
        "node.auto.force_merge.cpu.threshold",
        80.0,
        10,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for jvm threshold. (default: 70)
     */
    public static final Setting<Double> JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE = Setting.doubleSetting(
        "node.auto.force_merge.jvm.threshold",
        70.0,
        10,
        100,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for force merge thread count concurrent threshold. (default: 1)
     */
    public static final Setting<Integer> FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE = Setting.intSetting(
        "node.auto.force_merge.threads.threshold",
        0,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Setting for thread pool multiplier to determine concurrent force merge operations (default: 3).
     */
    public static final Setting<Integer> CONCURRENCY_MULTIPLIER = Setting.intSetting(
        "node.auto.force_merge.threads.concurrency_multiplier",
        2,
        2,
        5,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Creates settings manager with cluster settings for dynamic updates.
     */
    public ForceMergeManagerSettings(Settings settings, ClusterSettings clusterSettings, AutoForceMergeManager autoForceMergeManager) {
        this.autoForceMergeManager = autoForceMergeManager;
        this.autoForceMergeFeatureEnabled = AUTO_FORCE_MERGE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AUTO_FORCE_MERGE_SETTING, this::setAutoForceMergeFeatureEnabled);
        this.segmentCountThreshold = SEGMENT_COUNT_THRESHOLD_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SEGMENT_COUNT_THRESHOLD_FOR_AUTO_FORCE_MERGE, this::setSegmentCountThreshold);
        this.forcemergeDelay = MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE, this::setForcemergeDelay);
        this.schedulerInterval = AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.get(settings);
        clusterSettings.addSettingsUpdateConsumer(AUTO_FORCE_MERGE_SCHEDULER_INTERVAL, this::setSchedulerInterval);
        this.cpuThreshold = CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE, this::setCpuThreshold);
        this.jvmThreshold = JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE, this::setJvmThreshold);
        this.forceMergeThreadCount = FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(FORCE_MERGE_THREADS_THRESHOLD_COUNT_FOR_AUTO_FORCE_MERGE, this::setForceMergeThreadCount);
        this.concurrencyMultiplier = CONCURRENCY_MULTIPLIER.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CONCURRENCY_MULTIPLIER, this::setConcurrencyMultiplier);
    }

    public void setAutoForceMergeFeatureEnabled(Boolean autoForceMergeFeatureEnabled) {
        this.autoForceMergeFeatureEnabled = autoForceMergeFeatureEnabled;
        if (this.autoForceMergeFeatureEnabled) {
            autoForceMergeManager.start();
        } else {
            autoForceMergeManager.stop();
        }
    }

    public Boolean getAutoForceMergeFeatureEnabled() {
        return this.autoForceMergeFeatureEnabled;
    }

    public void setSegmentCountThreshold(Integer segmentCountThreshold) {
        this.segmentCountThreshold = segmentCountThreshold;
    }

    public Integer getSegmentCountThreshold() {
        return this.segmentCountThreshold;
    }

    public void setForcemergeDelay(TimeValue forcemergeDelay) {
        this.forcemergeDelay = forcemergeDelay;
    }

    public TimeValue getForcemergeDelay() {
        return this.forcemergeDelay;
    }

    public void setSchedulerInterval(TimeValue schedulerInterval) {
        this.schedulerInterval = schedulerInterval;
    }

    public TimeValue getSchedulerInterval() {
        return this.schedulerInterval;
    }

    public void setCpuThreshold(Double cpuThreshold) {
        this.cpuThreshold = cpuThreshold;
    }

    public Double getCpuThreshold() {
        return this.cpuThreshold;
    }

    public void setJvmThreshold(Double jvmThreshold) {
        this.jvmThreshold = jvmThreshold;
    }

    public Double getJvmThreshold() {
        return this.jvmThreshold;
    }

    public void setForceMergeThreadCount(Integer forceMergeThreadCount) {
        this.forceMergeThreadCount = forceMergeThreadCount;
    }

    public Integer getForceMergeThreadCount() {
        return this.forceMergeThreadCount;
    }

    public void setConcurrencyMultiplier(Integer concurrencyMultiplier) {
        this.concurrencyMultiplier = concurrencyMultiplier;
    }

    public Integer getConcurrencyMultiplier() {
        return this.concurrencyMultiplier;
    }

}
