/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.settings;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;

/**
 * Defines the settings for a node to be considered in duress.
 *
 * @opensearch.internal
 */
public class NodeDuressSettings {
    private static class Defaults {
        private static final int NUM_SUCCESSIVE_BREACHES = 3;
        private static final double CPU_THRESHOLD = 0.9;
        private static final double HEAP_THRESHOLD = 0.7;
    }

    /**
     * Defines the number of successive limit breaches after the node is marked "in duress".
     */
    private volatile int numSuccessiveBreaches;
    public static final Setting<Integer> SETTING_NUM_SUCCESSIVE_BREACHES = Setting.intSetting(
        "search_backpressure.node_duress.num_successive_breaches",
        Defaults.NUM_SUCCESSIVE_BREACHES,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the CPU usage threshold (in percentage) for a node to be considered "in duress".
     */
    private volatile double cpuThreshold;
    public static final Setting<Double> SETTING_CPU_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.cpu_threshold",
        Defaults.CPU_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the heap usage threshold (in percentage) for a node to be considered "in duress".
     */
    private volatile double heapThreshold;
    public static final Setting<Double> SETTING_HEAP_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.heap_threshold",
        Defaults.HEAP_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public NodeDuressSettings(Settings settings, ClusterSettings clusterSettings) {
        numSuccessiveBreaches = SETTING_NUM_SUCCESSIVE_BREACHES.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_NUM_SUCCESSIVE_BREACHES, this::setNumSuccessiveBreaches);

        cpuThreshold = SETTING_CPU_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_CPU_THRESHOLD, this::setCpuThreshold);

        heapThreshold = SETTING_HEAP_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_HEAP_THRESHOLD, this::setHeapThreshold);
    }

    public int getNumSuccessiveBreaches() {
        return numSuccessiveBreaches;
    }

    private void setNumSuccessiveBreaches(int numSuccessiveBreaches) {
        this.numSuccessiveBreaches = numSuccessiveBreaches;
    }

    public double getCpuThreshold() {
        return cpuThreshold;
    }

    private void setCpuThreshold(double cpuThreshold) {
        this.cpuThreshold = cpuThreshold;
    }

    public double getHeapThreshold() {
        return heapThreshold;
    }

    private void setHeapThreshold(double heapThreshold) {
        this.heapThreshold = heapThreshold;
    }
}
