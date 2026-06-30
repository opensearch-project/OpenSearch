/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.resource.tracker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.opensearch.common.ValidationException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.monitor.fs.FsInfo.DeviceStats;
import org.opensearch.monitor.fs.FsService;
import org.opensearch.node.IoUsageStats;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Optional;

/**
 * AverageIoUsageTracker tracks the IO usage by polling the FS Stats for IO metrics every (pollingInterval)
 * and keeping track of the rolling average over a defined time window (windowDuration).
 */
public class AverageIoUsageTracker extends AbstractAverageUsageTracker {

    private static final Logger LOGGER = LogManager.getLogger(AverageIoUsageTracker.class);
    private final FsService fsService;
    private final HashMap<String, Long> prevIoTimeDeviceMap;
    private long prevTimeInMillis;
    private IoUsageStats ioUsageStats;

    public AverageIoUsageTracker(FsService fsService, ThreadPool threadPool, TimeValue pollingInterval, TimeValue windowDuration) {
        super(threadPool, pollingInterval, windowDuration);
        this.fsService = fsService;
        this.prevIoTimeDeviceMap = new HashMap<>();
        this.prevTimeInMillis = -1;
        this.ioUsageStats = null;
    }

    /**
     * Get current IO usage percentage calculated using fs stats
     */
    @Override
    public long getUsage() {
        long usage = 0;
        Optional<ValidationException> validationException = this.preValidateFsStats();
        if (validationException != null && validationException.isPresent()) {
            throw validationException.get();
        }
        // Currently even during the raid setup we have only one mount device and it is giving 0 io time from /proc/diskstats
        DeviceStats[] devicesStats = fsService.stats().getIoStats().getDevicesStats();
        long latestTimeInMillis = fsService.stats().getTimestamp();
        for (DeviceStats devicesStat : devicesStats) {
            long devicePreviousIoTime = prevIoTimeDeviceMap.getOrDefault(devicesStat.getDeviceName(), (long) -1);
            long deviceCurrentIoTime = devicesStat.ioTimeInMillis();
            if (prevTimeInMillis > 0 && (latestTimeInMillis - this.prevTimeInMillis > 0) && devicePreviousIoTime > 0) {
                long absIoTime = (deviceCurrentIoTime - devicePreviousIoTime);
                long deviceCurrentIoUsage = absIoTime * 100 / (latestTimeInMillis - this.prevTimeInMillis);
                // We are returning the maximum IO Usage for all the attached devices
                usage = Math.max(usage, deviceCurrentIoUsage);
            }
            prevIoTimeDeviceMap.put(devicesStat.getDeviceName(), devicesStat.ioTimeInMillis());
        }
        this.prevTimeInMillis = latestTimeInMillis;
        return usage;
    }

    @Override
    protected void doStart() {
        if (Constants.LINUX) {
            this.ioUsageStats = new IoUsageStats(-1);
            scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
                long usage = getUsage();
                recordUsage(usage);
                updateIoUsageStats();
            }, pollingInterval, ThreadPool.Names.GENERIC);
        }
    }

    public Optional<ValidationException> preValidateFsStats() {
        ValidationException validationException = new ValidationException();
        if (fsService == null
            || fsService.stats() == null
            || fsService.stats().getIoStats() == null
            || fsService.stats().getIoStats().getDevicesStats() == null) {
            validationException.addValidationError("FSService IoStats Or DeviceStats are Missing");
        }
        return validationException.validationErrors().isEmpty() ? Optional.empty() : Optional.of(validationException);
    }

    private void updateIoUsageStats() {
        this.ioUsageStats.setIoUtilisationPercent(this.isReady() ? this.getAverage() : -1);
    }

    public IoUsageStats getIoUsageStats() {
        return this.ioUsageStats;
    }
}
