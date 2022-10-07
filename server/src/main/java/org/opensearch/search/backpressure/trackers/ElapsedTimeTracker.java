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
 * ElapsedTimeTracker evaluates if the task has been running for more time than allowed.
 *
 * @opensearch.internal
 */
public class ElapsedTimeTracker extends TaskResourceUsageTracker {
    public static final String NAME = "elapsed_time_tracker";

    private final LongSupplier timeNanosSupplier;
    private final LongSupplier elapsedTimeNanosThresholdSupplier;

    public ElapsedTimeTracker(SearchBackpressureSettings settings, LongSupplier timeNanosSupplier) {
        this.timeNanosSupplier = timeNanosSupplier;
        this.elapsedTimeNanosThresholdSupplier = () -> settings.getSearchShardTaskSettings().getElapsedTimeNanosThreshold();
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void update(Task task) {}

    @Override
    public Optional<TaskCancellation.Reason> cancellationReason(Task task) {
        long usage = timeNanosSupplier.getAsLong() - task.getStartTimeNanos();
        long threshold = elapsedTimeNanosThresholdSupplier.getAsLong();

        if (usage < threshold) {
            return Optional.empty();
        }

        return Optional.of(
            new TaskCancellation.Reason(
                "elapsed time exceeded ["
                    + new TimeValue(usage, TimeUnit.NANOSECONDS)
                    + " >= "
                    + new TimeValue(threshold, TimeUnit.NANOSECONDS)
                    + "]",
                1  // TODO: fine-tune the cancellation score/weight
            )
        );
    }
}
