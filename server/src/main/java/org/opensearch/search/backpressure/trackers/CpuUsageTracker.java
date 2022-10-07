/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * CpuUsageTracker evaluates if the task has consumed too many CPU cycles than allowed.
 *
 * @opensearch.internal
 */
public class CpuUsageTracker extends TaskResourceUsageTracker {
    public static final String NAME = "cpu_usage_tracker";

    private final LongSupplier cpuTimeNanosThresholdSupplier;

    public CpuUsageTracker(SearchBackpressureSettings settings) {
        this.cpuTimeNanosThresholdSupplier = () -> settings.getSearchShardTaskSettings().getCpuTimeNanosThreshold();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void update(Task task) {}

    @Override
    public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
        long usage = task.getTotalResourceStats().getCpuTimeInNanos();
        long threshold = cpuTimeNanosThresholdSupplier.getAsLong();

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
}
