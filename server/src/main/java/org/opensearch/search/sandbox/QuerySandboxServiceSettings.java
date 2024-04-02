/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sandbox;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Main class to declare the query sandboxing feature related settings
 */
public class QuerySandboxServiceSettings {
    private static final Long DEFAULT_RUN_INTERVAL_MILLIS = 1000l;
    private static final Double DEFAULT_NODE_LEVEL_REJECTION_QUERY_SANDBOX_THRESHOLD = 0.8;
    private static final Double DEFAULT_NODE_LEVEL_CANCELLATION_QUERY_SANDBOX_THRESHOLD = 0.9;
    public static final int DEFAULT_MAX_SANDBOX_COUNT_VALUE = 100;
    static final String SANDBOX_COUNT_SETTING_NAME = "node.sandbox.max_count";

    private TimeValue runIntervalMillis;
    private Double nodeLevelJvmCancellationThreshold;
    private Double nodeLevelJvmRejectionThreshold;
    private volatile int maxSandboxCount;

    public static final Setting<Integer> MAX_SANDBOX_COUNT = Setting.intSetting(
        SANDBOX_COUNT_SETTING_NAME,
        DEFAULT_MAX_SANDBOX_COUNT_VALUE,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private static final Setting<Long> QSB_RUN_INTERVAL_SETTING = Setting.longSetting(
        "query_sandbox.service.run_interval_millis",
        DEFAULT_RUN_INTERVAL_MILLIS,
        1,
        Setting.Property.NodeScope
    );

    public static final Setting<Double> NODE_LEVEL_REJECTION_THRESHOLD = Setting.doubleSetting(
        "query_sandbox.node.rejection_threshold",
        DEFAULT_NODE_LEVEL_REJECTION_QUERY_SANDBOX_THRESHOLD,
        Setting.Property.NodeScope
    );
    public static final Setting<Double> NODE_LEVEL_CANCELLATION_THRESHOLD = Setting.doubleSetting(
        "query_sandbox.node.cancellation_threshold",
        DEFAULT_NODE_LEVEL_CANCELLATION_QUERY_SANDBOX_THRESHOLD,
        Setting.Property.NodeScope
    );

    public QuerySandboxServiceSettings(Settings settings, ClusterSettings clusterSettings) {
        runIntervalMillis = new TimeValue(QSB_RUN_INTERVAL_SETTING.get(settings));
        nodeLevelJvmCancellationThreshold = NODE_LEVEL_CANCELLATION_THRESHOLD.get(settings);
        nodeLevelJvmRejectionThreshold = NODE_LEVEL_REJECTION_THRESHOLD.get(settings);
        maxSandboxCount = MAX_SANDBOX_COUNT.get(settings);

        clusterSettings.addSettingsUpdateConsumer(MAX_SANDBOX_COUNT, this::setMaxSandboxCount);
        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_CANCELLATION_THRESHOLD, this::setNodeLevelJvmCancellationThreshold);
        clusterSettings.addSettingsUpdateConsumer(NODE_LEVEL_REJECTION_THRESHOLD, this::setNodeLevelJvmRejectionThreshold);
    }

    public TimeValue getRunIntervalMillis() {
        return runIntervalMillis;
    }


    public void setMaxSandboxCount(int newMaxSandboxCount) {
        if (newMaxSandboxCount < 0) {
            throw new IllegalArgumentException("node.sandbox.max_count can't be negative");
        }
        this.maxSandboxCount = newMaxSandboxCount;
    }

    public void setRunIntervalMillis(TimeValue runIntervalMillis) {
        this.runIntervalMillis = runIntervalMillis;
    }

    public Double getNodeLevelJvmCancellationThreshold() {
        return nodeLevelJvmCancellationThreshold;
    }

    public void setNodeLevelJvmCancellationThreshold(Double nodeLevelJvmCancellationThreshold) {
        this.nodeLevelJvmCancellationThreshold = nodeLevelJvmCancellationThreshold;
    }

    public Double getNodeLevelJvmRejectionThreshold() {
        return nodeLevelJvmRejectionThreshold;
    }

    public void setNodeLevelJvmRejectionThreshold(Double nodeLevelJvmRejectionThreshold) {
        this.nodeLevelJvmRejectionThreshold = nodeLevelJvmRejectionThreshold;
    }

    public int getMaxSandboxCount() {
        return maxSandboxCount;
    }
}
