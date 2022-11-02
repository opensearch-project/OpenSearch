/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.CPU_USAGE_TRACKER;

/**
 * CpuUsageTracker evaluates if the task has consumed too many CPU cycles than allowed.
 *
 * @opensearch.internal
 */
public class CpuUsageTracker extends TaskResourceUsageTracker {
    private static class Defaults {
        private static final long CPU_TIME_MILLIS_THRESHOLD = 15000;
    }

    /**
     * Defines the CPU usage threshold (in millis) for an individual task before it is considered for cancellation.
     */
    private volatile long cpuTimeMillisThreshold;
    public static final Setting<Long> SETTING_CPU_TIME_MILLIS_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_shard_task.cpu_time_millis_threshold",
        Defaults.CPU_TIME_MILLIS_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public CpuUsageTracker(SearchBackpressureSettings settings) {
        this.cpuTimeMillisThreshold = SETTING_CPU_TIME_MILLIS_THRESHOLD.get(settings.getSettings());
        settings.getClusterSettings().addSettingsUpdateConsumer(SETTING_CPU_TIME_MILLIS_THRESHOLD, this::setCpuTimeMillisThreshold);
    }

    @Override
    public String name() {
        return CPU_USAGE_TRACKER.getName();
    }

    @Override
    public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
        long usage = task.getTotalResourceStats().getCpuTimeInNanos();
        long threshold = getCpuTimeNanosThreshold();

        if (usage < threshold) {
            return Optional.empty();
        }

        return Optional.of(
            new TaskCancellation.Reason(
                "cpu usage exceeded ["
                    + new TimeValue(usage, TimeUnit.NANOSECONDS)
                    + " >= "
                    + new TimeValue(threshold, TimeUnit.NANOSECONDS)
                    + "]",
                1  // TODO: fine-tune the cancellation score/weight
            )
        );
    }

    public long getCpuTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(cpuTimeMillisThreshold);
    }

    public void setCpuTimeMillisThreshold(long cpuTimeMillisThreshold) {
        this.cpuTimeMillisThreshold = cpuTimeMillisThreshold;
    }
}
