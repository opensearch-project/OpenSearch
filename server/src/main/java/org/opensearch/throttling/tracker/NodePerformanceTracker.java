/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.PerfStatsCollectorService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * This tracks the performance of node resources such as CPU, IO and memory
 */
public class NodePerformanceTracker extends AbstractLifecycleComponent {
    private double cpuUtilizationPercent;
    private double memoryUtilizationPercent;
    private ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ClusterSettings clusterSettings;
    private AverageCpuUsageTracker cpuUsageTracker;
    private AverageMemoryUsageTracker memoryUsageTracker;
    private PerfStatsCollectorService perfStatsCollectorService;

    private PerformanceTrackerSettings performanceTrackerSettings;
    private static final Logger logger = LogManager.getLogger(NodePerformanceTracker.class);
    private final TimeValue interval;

    public static final String LOCAL_NODE = "LOCAL";

    public NodePerformanceTracker(
        PerfStatsCollectorService perfStatsCollectorService,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this.perfStatsCollectorService = perfStatsCollectorService;
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.performanceTrackerSettings = new PerformanceTrackerSettings(settings, clusterSettings);
        interval = new TimeValue(performanceTrackerSettings.getRefreshInterval());
        initialize();
    }

    private double getAverageCpuPercentUsed() {
        return cpuUsageTracker.getAverage();
    }

    private double getAverageMemoryPercentUsed() {
        return memoryUsageTracker.getAverage();
    }

    private void setCpuUtilizationPercent(double cpuUtilizationPercent) {
        this.cpuUtilizationPercent = cpuUtilizationPercent;
    }

    private void setMemoryUtilizationPercent(double memoryUtilizationPercent) {
        this.memoryUtilizationPercent = memoryUtilizationPercent;
    }

    public double getCpuUtilizationPercent() {
        return cpuUtilizationPercent;
    }

    public double getMemoryUtilizationPercent() {
        return memoryUtilizationPercent;
    }

    void doRun() {
        setCpuUtilizationPercent(getAverageCpuPercentUsed());
        setMemoryUtilizationPercent(getAverageMemoryPercentUsed());
        perfStatsCollectorService.collectNodePerfStatistics(
            LOCAL_NODE,
            getCpuUtilizationPercent(),
            getMemoryUtilizationPercent(),
            System.currentTimeMillis()
        );
    }

    void initialize() {
        cpuUsageTracker = new AverageCpuUsageTracker(
            threadPool,
            performanceTrackerSettings.getCpuPollingInterval(),
            performanceTrackerSettings.getCpuWindowDuration()
        );

        clusterSettings.addSettingsUpdateConsumer(
            PerformanceTrackerSettings.GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
            cpuUsageTracker::setWindowSize
        );

        memoryUsageTracker = new AverageMemoryUsageTracker(
            threadPool,
            performanceTrackerSettings.getMemoryPollingInterval(),
            performanceTrackerSettings.getMemoryWindowDuration()
        );
        clusterSettings.addSettingsUpdateConsumer(
            PerformanceTrackerSettings.GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING,
            memoryUsageTracker::setWindowSize
        );
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {
                logger.debug("failure in node performance tracker", e);
            }
        }, interval, ThreadPool.Names.GENERIC);
        cpuUsageTracker.doStart();
        memoryUsageTracker.doStart();
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
        cpuUsageTracker.doStop();
        memoryUsageTracker.doStop();
    }

    @Override
    protected void doClose() throws IOException {
        cpuUsageTracker.doClose();
        memoryUsageTracker.doClose();
    }
}
