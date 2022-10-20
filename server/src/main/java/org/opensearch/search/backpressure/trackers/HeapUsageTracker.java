/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.util.MovingAverage;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.search.backpressure.settings.SearchShardTaskSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.HEAP_USAGE_TRACKER;

/**
 * HeapUsageTracker evaluates if the task has consumed too much heap than allowed.
 * It also compares the task's heap usage against a historical moving average of previously completed tasks.
 *
 * @opensearch.internal
 */
public class HeapUsageTracker extends TaskResourceUsageTracker implements SearchShardTaskSettings.Listener {
    private final LongSupplier heapBytesThresholdSupplier;
    private final DoubleSupplier heapVarianceThresholdSupplier;
    private final AtomicReference<MovingAverage> movingAverageReference;

    public HeapUsageTracker(SearchBackpressureSettings settings) {
        this.heapBytesThresholdSupplier = () -> settings.getSearchShardTaskSettings().getHeapBytesThreshold();
        this.heapVarianceThresholdSupplier = () -> settings.getSearchShardTaskSettings().getHeapVarianceThreshold();
        this.movingAverageReference = new AtomicReference<>(
            new MovingAverage(settings.getSearchShardTaskSettings().getHeapMovingAverageWindowSize())
        );
        settings.getSearchShardTaskSettings().addListener(this);
    }

    @Override
    public String name() {
        return HEAP_USAGE_TRACKER.getName();
    }

    @Override
    public void update(Task task) {
        movingAverageReference.get().record(task.getTotalResourceStats().getMemoryInBytes());
    }

    @Override
    public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
        MovingAverage movingAverage = movingAverageReference.get();

        // There haven't been enough measurements.
        if (movingAverage.isReady() == false) {
            return Optional.empty();
        }

        double currentUsage = task.getTotalResourceStats().getMemoryInBytes();
        double averageUsage = movingAverage.getAverage();
        double allowedUsage = averageUsage * heapVarianceThresholdSupplier.getAsDouble();

        if (currentUsage < heapBytesThresholdSupplier.getAsLong() || currentUsage < allowedUsage) {
            return Optional.empty();
        }

        return Optional.of(
            new TaskCancellation.Reason(
                "heap usage exceeded [" + new ByteSizeValue((long) currentUsage) + " >= " + new ByteSizeValue((long) allowedUsage) + "]",
                (int) (currentUsage / averageUsage)  // TODO: fine-tune the cancellation score/weight
            )
        );
    }

    @Override
    public void onHeapMovingAverageWindowSizeChanged(int newWindowSize) {
        movingAverageReference.set(new MovingAverage(newWindowSize));
    }
}
