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
import org.opensearch.core.common.unit.ByteSizeValue;

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
        // Trip native-memory duress when processNativeBytes / nodeNativeMemoryLimit
        // exceeds this fraction. Default 0.85 — tighter than HEAP_THRESHOLD because
        // native allocations bypass GC and hit the OS scheduler directly.
        private static final double NATIVE_MEMORY_THRESHOLD = 0.85;
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

    /**
     * Defines the native-memory usage threshold (as a fraction) above which a node is considered
     * "in duress" for native memory. The duress probe computes
     * {@code usedFraction = OsProbe.getProcessNativeMemoryBytes() / nodeNativeMemoryLimit} and
     * compares against this threshold; when the comparison holds for
     * {@link #numSuccessiveBreaches} consecutive observations the node is marked in duress.
     *
     * <p>This gate is independent of heap duress: backends that manage memory outside the JVM
     * heap (e.g. DataFusion's memory pool) can cancel search tasks when native memory is tight
     * even if the heap itself is nowhere near its threshold.
     */
    private volatile double nativeMemoryThreshold;
    public static final Setting<Double> SETTING_NATIVE_MEMORY_THRESHOLD = Setting.doubleSetting(
        "search_backpressure.node_duress.native_memory_threshold",
        Defaults.NATIVE_MEMORY_THRESHOLD,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Absolute native-memory budget for this node, in bytes. When the value is {@link ByteSizeValue#ZERO}
     * (default) the tracker treats the budget as unconfigured and reports {@code 0%}.
     */
    private volatile ByteSizeValue nodeNativeMemory;
    public static final Setting<ByteSizeValue> NODE_NATIVE_MEMORY_LIMIT_SETTING = Setting.byteSizeSetting(
        "node.native_memory.limit",
        ByteSizeValue.ZERO,
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

        nativeMemoryThreshold = SETTING_NATIVE_MEMORY_THRESHOLD.get(settings);
        clusterSettings.addSettingsUpdateConsumer(SETTING_NATIVE_MEMORY_THRESHOLD, this::setNativeMemoryThreshold);

        nodeNativeMemory = NODE_NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(NODE_NATIVE_MEMORY_LIMIT_SETTING, this::setNodeNativeMemory);
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

    public double getNativeMemoryThreshold() {
        return nativeMemoryThreshold;
    }

    private void setNativeMemoryThreshold(double nativeMemoryThreshold) {
        this.nativeMemoryThreshold = nativeMemoryThreshold;
    }

    public long getNodeNativeMemory() {
        return nodeNativeMemory.getBytes();
    }

    public void setNodeNativeMemory(ByteSizeValue nativeMemoryLimitBytes) {
        this.nodeNativeMemory = nativeMemoryLimitBytes;
    }

}
