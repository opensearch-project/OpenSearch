/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import org.opensearch.common.Nullable;
import org.opensearch.common.UUIDs;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.NodeHealthService;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.opensearch.monitor.StatusInfo.Status.HEALTHY;
import static org.opensearch.monitor.StatusInfo.Status.UNHEALTHY;

/**
 * Runs periodically and attempts to create a temp file to see if the filesystem is writable. If not then it marks the
 * path as unhealthy.
 *
 * @opensearch.internal
 */
public class FsHealthService extends AbstractLifecycleComponent implements NodeHealthService {

    private static final Logger logger = LogManager.getLogger(FsHealthService.class);
    private final ThreadPool threadPool;
    private volatile boolean enabled;
    private volatile boolean brokenLock;
    private final TimeValue refreshInterval;
    private volatile TimeValue slowPathLoggingThreshold;
    private final NodeEnvironment nodeEnv;
    private final LongSupplier currentTimeMillisSupplier;
    private volatile Scheduler.Cancellable scheduledFuture;
    private volatile TimeValue healthyTimeoutThreshold;
    private final AtomicLong lastRunStartTimeMillis = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean checkInProgress = new AtomicBoolean();

    @Nullable
    private volatile Set<Path> unhealthyPaths;

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "monitor.fs.health.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.fs.health.refresh_interval",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> SLOW_PATH_LOGGING_THRESHOLD_SETTING = Setting.timeSetting(
        "monitor.fs.health.slow_path_logging_threshold",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> HEALTHY_TIMEOUT_SETTING = Setting.timeSetting(
        "monitor.fs.health.healthy_timeout_threshold",
        TimeValue.timeValueSeconds(60),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public FsHealthService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, NodeEnvironment nodeEnv) {
        this.threadPool = threadPool;
        this.enabled = ENABLED_SETTING.get(settings);
        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.slowPathLoggingThreshold = SLOW_PATH_LOGGING_THRESHOLD_SETTING.get(settings);
        this.currentTimeMillisSupplier = threadPool::relativeTimeInMillis;
        this.healthyTimeoutThreshold = HEALTHY_TIMEOUT_SETTING.get(settings);
        this.nodeEnv = nodeEnv;
        clusterSettings.addSettingsUpdateConsumer(SLOW_PATH_LOGGING_THRESHOLD_SETTING, this::setSlowPathLoggingThreshold);
        clusterSettings.addSettingsUpdateConsumer(HEALTHY_TIMEOUT_SETTING, this::setHealthyTimeoutThreshold);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(new FsHealthMonitor(), refreshInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        scheduledFuture.cancel();
    }

    @Override
    protected void doClose() {}

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setSlowPathLoggingThreshold(TimeValue slowPathLoggingThreshold) {
        this.slowPathLoggingThreshold = slowPathLoggingThreshold;
    }

    public void setHealthyTimeoutThreshold(TimeValue healthyTimeoutThreshold) {
        this.healthyTimeoutThreshold = healthyTimeoutThreshold;
    }

    @Override
    public StatusInfo getHealth() {
        StatusInfo statusInfo;
        Set<Path> unhealthyPaths = this.unhealthyPaths;
        if (enabled == false) {
            statusInfo = new StatusInfo(HEALTHY, "health check disabled");
        } else if (brokenLock) {
            statusInfo = new StatusInfo(UNHEALTHY, "health check failed due to broken node lock");
        } else if (checkInProgress.get()
            && currentTimeMillisSupplier.getAsLong() - lastRunStartTimeMillis.get() > healthyTimeoutThreshold.millis()) {
                statusInfo = new StatusInfo(UNHEALTHY, "healthy threshold breached");
            } else if (unhealthyPaths == null) {
                statusInfo = new StatusInfo(HEALTHY, "health check passed");
            } else {
                String info = "health check failed on ["
                    + unhealthyPaths.stream().map(k -> k.toString()).collect(Collectors.joining(","))
                    + "]";
                statusInfo = new StatusInfo(UNHEALTHY, info);
            }
        return statusInfo;
    }

    class FsHealthMonitor implements Runnable {

        static final String TEMP_FILE_NAME = ".opensearch_temp_file";
        private byte[] byteToWrite;

        FsHealthMonitor() {
            this.byteToWrite = UUIDs.randomBase64UUID().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void run() {
            boolean checkEnabled = enabled;
            try {
                if (checkEnabled) {
                    setLastRunStartTimeMillis();
                    boolean started = checkInProgress.compareAndSet(false, true);
                    assert started;
                    monitorFSHealth();
                    logger.debug("health check succeeded");
                }
            } catch (Exception e) {
                logger.error("health check failed", e);
            } finally {
                if (checkEnabled) {
                    boolean completed = checkInProgress.compareAndSet(true, false);
                    assert completed;
                }
            }
        }

        private void monitorFSHealth() {
            Set<Path> currentUnhealthyPaths = null;
            Path[] paths = null;
            try {
                paths = nodeEnv.nodeDataPaths();
            } catch (IllegalStateException e) {
                logger.error("health check failed", e);
                brokenLock = true;
                return;
            }

            for (Path path : paths) {
                long executionStartTime = currentTimeMillisSupplier.getAsLong();
                try {
                    if (Files.exists(path)) {
                        Path tempDataPath = path.resolve(TEMP_FILE_NAME);
                        Files.deleteIfExists(tempDataPath);
                        try (OutputStream os = Files.newOutputStream(tempDataPath, StandardOpenOption.CREATE_NEW)) {
                            os.write(byteToWrite);
                            IOUtils.fsync(tempDataPath, false);
                        }
                        Files.delete(tempDataPath);
                        final long elapsedTime = currentTimeMillisSupplier.getAsLong() - executionStartTime;
                        if (elapsedTime > slowPathLoggingThreshold.millis()) {
                            logger.warn(
                                "health check of [{}] took [{}ms] which is above the warn threshold of [{}]",
                                path,
                                elapsedTime,
                                slowPathLoggingThreshold
                            );
                        }
                        if (elapsedTime > healthyTimeoutThreshold.millis()) {
                            logger.error(
                                "health check of [{}] failed, took [{}ms] which is above the healthy threshold of [{}]",
                                path,
                                elapsedTime,
                                healthyTimeoutThreshold
                            );
                            if (currentUnhealthyPaths == null) {
                                currentUnhealthyPaths = new HashSet<>(1);
                            }
                            currentUnhealthyPaths.add(path);
                        }
                    }
                } catch (Exception ex) {
                    logger.error(new ParameterizedMessage("health check of [{}] failed", path), ex);
                    if (currentUnhealthyPaths == null) {
                        currentUnhealthyPaths = new HashSet<>(1);
                    }
                    currentUnhealthyPaths.add(path);
                }
            }
            unhealthyPaths = currentUnhealthyPaths;
            brokenLock = false;
        }
    }

    private void setLastRunStartTimeMillis() {
        lastRunStartTimeMillis.getAndUpdate(l -> Math.max(l, currentTimeMillisSupplier.getAsLong()));
    }
}
