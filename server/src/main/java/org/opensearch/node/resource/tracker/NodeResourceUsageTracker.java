/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.apache.lucene.util.Constants;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.node.IoUsageStats;
import org.opensearch.threadpool.ThreadPool;

/**
 * This tracks the usage of node resources such as CPU, IO and memory
 */
public class NodeResourceUsageTracker extends AbstractLifecycleComponent {
    private ThreadPool threadPool;
    private final ClusterSettings clusterSettings;
    private AverageCpuUsageTracker cpuUsageTracker;
    private AverageMemoryUsageTracker memoryUsageTracker;
    private AverageIoUsageTracker ioUsageTracker;
    private AverageNativeMemoryUsageTracker nativeMemoryUsageTracker;

    private ResourceTrackerSettings resourceTrackerSettings;

    private final FsService fsService;

    public NodeResourceUsageTracker(FsService fsService, ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings) {
        this.fsService = fsService;
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.resourceTrackerSettings = new ResourceTrackerSettings(settings);
        initialize();
    }

    /**
     * Return CPU utilization average if we have enough datapoints, otherwise return 0
     */
    public double getCpuUtilizationPercent() {
        if (cpuUsageTracker.isReady()) {
            return cpuUsageTracker.getAverage();
        }
        return 0.0;
    }

    /**
     * Return memory utilization average if we have enough datapoints, otherwise return 0
     */
    public double getMemoryUtilizationPercent() {
        if (memoryUsageTracker.isReady()) {
            return memoryUsageTracker.getAverage();
        }
        return 0.0;
    }

    /**
     * Return io stats average if we have enough datapoints, otherwise return 0
     */
    public IoUsageStats getIoUsageStats() {
        return ioUsageTracker.getIoUsageStats();
    }

    /**
     * Return native memory utilization average if we have enough datapoints, otherwise return 0
     */
    public double getNativeMemoryUtilizationPercent() {
        if (nativeMemoryUsageTracker.isReady()) {
            return nativeMemoryUsageTracker.getAverage();
        }
        return 0.0;
    }

    /**
     * Checks if all of the resource usage trackers are ready
     */
    public boolean isReady() {
        if (Constants.LINUX) {
            return memoryUsageTracker.isReady()
                && cpuUsageTracker.isReady()
                && ioUsageTracker.isReady()
                && nativeMemoryUsageTracker.isReady();
        }
        return memoryUsageTracker.isReady() && cpuUsageTracker.isReady();
    }

    void initialize() {
        cpuUsageTracker = new AverageCpuUsageTracker(
            threadPool,
            resourceTrackerSettings.getCpuPollingInterval(),
            resourceTrackerSettings.getCpuWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            ResourceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setCpuWindowDuration
        );

        memoryUsageTracker = new AverageMemoryUsageTracker(
            threadPool,
            resourceTrackerSettings.getMemoryPollingInterval(),
            resourceTrackerSettings.getMemoryWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            ResourceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setMemoryWindowDuration
        );

        ioUsageTracker = new AverageIoUsageTracker(
            fsService,
            threadPool,
            resourceTrackerSettings.getIoPollingInterval(),
            resourceTrackerSettings.getIoWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            ResourceTrackerSettings.GLOBAL_IO_USAGE_AC_WINDOW_DURATION_SETTING,
            this::setIoWindowDuration
        );

        nativeMemoryUsageTracker = new AverageNativeMemoryUsageTracker(
            threadPool,
            resourceTrackerSettings.getNativeMemoryPollingInterval(),
            resourceTrackerSettings.getNativeMemoryWindowDuration(),
            resourceTrackerSettings
        );
        if (Constants.LINUX) {
            clusterSettings.addSettingsUpdateConsumer(
                ResourceTrackerSettings.GLOBAL_NATIVE_MEMORY_USAGE_AC_WINDOW_DURATION_SETTING,
                this::setNativeMemoryWindowDuration
            );
            clusterSettings.addSettingsUpdateConsumer(ResourceTrackerSettings.NODE_NATIVE_MEMORY_LIMIT_SETTING, this::setNativeMemoryLimit);
            clusterSettings.addSettingsUpdateConsumer(
                ResourceTrackerSettings.NODE_NATIVE_MEMORY_BUFFER_PERCENT_SETTING,
                this::setNativeMemoryBufferPercent
            );
        }
    }

    private void setMemoryWindowDuration(TimeValue windowDuration) {
        memoryUsageTracker.setWindowSize(windowDuration);
        resourceTrackerSettings.setMemoryWindowDuration(windowDuration);
    }

    private void setCpuWindowDuration(TimeValue windowDuration) {
        cpuUsageTracker.setWindowSize(windowDuration);
        resourceTrackerSettings.setCpuWindowDuration(windowDuration);
    }

    private void setIoWindowDuration(TimeValue windowDuration) {
        ioUsageTracker.setWindowSize(windowDuration);
        resourceTrackerSettings.setIoWindowDuration(windowDuration);
    }

    private void setNativeMemoryWindowDuration(TimeValue windowDuration) {
        nativeMemoryUsageTracker.setWindowSize(windowDuration);
        resourceTrackerSettings.setNativeMemoryWindowDuration(windowDuration);
    }

    private void setNativeMemoryLimit(ByteSizeValue newLimit) {
        resourceTrackerSettings.setNativeMemoryLimitBytes(newLimit.getBytes());
    }

    private void setNativeMemoryBufferPercent(int newBufferPercent) {
        resourceTrackerSettings.setNativeMemoryBufferPercent(newBufferPercent);
    }

    /**
     * Visible for testing
     */
    ResourceTrackerSettings getResourceTrackerSettings() {
        return resourceTrackerSettings;
    }

    @Override
    protected void doStart() {
        cpuUsageTracker.doStart();
        memoryUsageTracker.doStart();
        ioUsageTracker.doStart();
        if (Constants.LINUX) {
            nativeMemoryUsageTracker.doStart();
        }
    }

    @Override
    protected void doStop() {
        cpuUsageTracker.doStop();
        memoryUsageTracker.doStop();
        ioUsageTracker.doStop();
        if (Constants.LINUX) {
            nativeMemoryUsageTracker.doStop();
        }
    }

    @Override
    protected void doClose() {
        cpuUsageTracker.doClose();
        memoryUsageTracker.doClose();
        ioUsageTracker.doClose();
        if (Constants.LINUX) {
            nativeMemoryUsageTracker.doClose();
        }
    }
}
