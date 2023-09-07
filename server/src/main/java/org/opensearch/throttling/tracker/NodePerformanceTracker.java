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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.node.PerformanceCollectorService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * This tracks the performance of node resources such as CPU, IO and memory
 */
public class NodePerformanceTracker extends AbstractLifecycleComponent {
    private double cpuPercentUsed;
    private double memoryPercentUsed;
    private double ioPercentUsed;
    private ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final Settings settings;
    private final ClusterSettings clusterSettings;
    private AverageCpuUsageTracker cpuUsageTracker;
    private AverageMemoryUsageTracker memoryUsageTracker;
    private PerformanceCollectorService performanceCollectorService;
    private static final Logger logger = LogManager.getLogger(NodePerformanceTracker.class);
    private final TimeValue interval;
    private static final long refreshIntervalInMills = 1000;
    public static final Setting<Long> REFRESH_INTERVAL_MILLIS = Setting.longSetting(
        "node.performance_tracker.interval_millis",
        refreshIntervalInMills,
        1,
        Setting.Property.NodeScope
    );

    public static final String LOCAL_NODE = "LOCAL";

    public static final Setting<TimeValue> GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.global_cpu_usage.polling_interval",
        TimeValue.timeValueMillis(500),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.global_cpu_usage.window_duration",
        TimeValue.timeValueSeconds(30),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_JVM_USAGE_AC_POLLING_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "node.global_jvmmp.polling_interval",
        TimeValue.timeValueMillis(500),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING = Setting.positiveTimeSetting(
        "node.global_jvmmp.window_duration",
        TimeValue.timeValueSeconds(30),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public NodePerformanceTracker(
        PerformanceCollectorService performanceCollectorService,
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        this.performanceCollectorService = performanceCollectorService;
        this.threadPool = threadPool;
        this.settings = settings;
        this.clusterSettings = clusterSettings;
        interval = new TimeValue(REFRESH_INTERVAL_MILLIS.get(settings));
        initialize();
    }

    private double getAverageCpuUsed() {
        return cpuUsageTracker.getAverage();
    }

    private double getAverageMemoryUsed() {
        return memoryUsageTracker.getAverage();
    }

    private double getAverageIoUsed() {
        // IO utilization percentage - this will be completed after we enhance FS stats
        return 0;
    }

    public TimeValue getCpuWindow() {
        return GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings);
    }

    private void setCpuPercentUsed(double cpuPercentUsed) {
        this.cpuPercentUsed = cpuPercentUsed;
    }

    private void setMemoryPercentUsed(double memoryPercentUsed) {
        this.memoryPercentUsed = memoryPercentUsed;
    }

    private void setIoPercentUsed(double ioPercentUsed) {
        this.ioPercentUsed = ioPercentUsed;
    }

    public double getCpuPercentUsed() {
        return cpuPercentUsed;
    }

    public double getIoPercentUsed() {
        return ioPercentUsed;
    }

    public double getMemoryPercentUsed() {
        return memoryPercentUsed;
    }

    void doRun() {
        setCpuPercentUsed(getAverageCpuUsed());
        setMemoryPercentUsed(getAverageMemoryUsed());
        setIoPercentUsed(getAverageIoUsed());
        performanceCollectorService.addNodePerfStatistics(
            LOCAL_NODE,
            getCpuPercentUsed(),
            getIoPercentUsed(),
            getMemoryPercentUsed(),
            System.currentTimeMillis()
        );
    }

    void initialize() {
        cpuUsageTracker = new AverageCpuUsageTracker(
            threadPool,
            GLOBAL_CPU_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings),
            GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING.get(settings)
        );

        clusterSettings.addSettingsUpdateConsumer(
            GLOBAL_CPU_USAGE_AC_WINDOW_DURATION_SETTING,
            duration -> cpuUsageTracker.setWindowDuration(duration)
        );

        memoryUsageTracker = new AverageMemoryUsageTracker(
            threadPool,
            GLOBAL_JVM_USAGE_AC_POLLING_INTERVAL_SETTING.get(settings),
            GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING.get(settings)
        );

        clusterSettings.addSettingsUpdateConsumer(
            GLOBAL_JVM_USAGE_AC_WINDOW_DURATION_SETTING,
            duration -> memoryUsageTracker.setWindowDuration(duration)
        );
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {
                logger.debug("failure in search search backpressure", e);
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
