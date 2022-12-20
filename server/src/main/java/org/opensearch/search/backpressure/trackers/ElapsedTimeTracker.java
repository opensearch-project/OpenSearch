/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.action.search.SearchTask;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.backpressure.settings.SearchBackpressureSettings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import static org.opensearch.search.backpressure.trackers.TaskResourceUsageTrackerType.ELAPSED_TIME_TRACKER;

/**
 * ElapsedTimeTracker evaluates if the task has been running for more time than allowed.
 *
 * @opensearch.internal
 */
public class ElapsedTimeTracker extends TaskResourceUsageTracker {
    private static class Defaults {
        private static final long ELAPSED_TIME_MILLIS_THRESHOLD_FOR_SEARCH_QUERY = 120000;
        private static final long ELAPSED_TIME_MILLIS_THRESHOLD = 30000;
    }

    /**
     * Defines the elapsed time threshold (in millis) for an individual search task before it is considered for cancellation.
     */
    private volatile long elapsedTimeMillisThresholdForSearchQuery;
    public static final Setting<Long> SETTING_ELAPSED_TIME_MILLIS_THRESHOLD_FOR_SEARCH_QUERY = Setting.longSetting(
        "search_backpressure.search_task.elapsed_time_millis_threshold_for_search_query",
        Defaults.ELAPSED_TIME_MILLIS_THRESHOLD_FOR_SEARCH_QUERY,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Defines the elapsed time threshold (in millis) for an individual search shard task before it is considered for cancellation.
     */
    private volatile long elapsedTimeMillisThreshold;
    public static final Setting<Long> SETTING_ELAPSED_TIME_MILLIS_THRESHOLD = Setting.longSetting(
        "search_backpressure.search_shard_task.elapsed_time_millis_threshold",
        Defaults.ELAPSED_TIME_MILLIS_THRESHOLD,
        0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final LongSupplier timeNanosSupplier;

    public ElapsedTimeTracker(SearchBackpressureSettings settings, LongSupplier timeNanosSupplier) {
        this.timeNanosSupplier = timeNanosSupplier;
        this.elapsedTimeMillisThresholdForSearchQuery = SETTING_ELAPSED_TIME_MILLIS_THRESHOLD_FOR_SEARCH_QUERY.get(settings.getSettings());
        this.elapsedTimeMillisThreshold = SETTING_ELAPSED_TIME_MILLIS_THRESHOLD.get(settings.getSettings());
        settings.getClusterSettings()
            .addSettingsUpdateConsumer(
                SETTING_ELAPSED_TIME_MILLIS_THRESHOLD_FOR_SEARCH_QUERY,
                this::setElapsedTimeMillisThresholdForSearchQuery
            );
        settings.getClusterSettings().addSettingsUpdateConsumer(SETTING_ELAPSED_TIME_MILLIS_THRESHOLD, this::setElapsedTimeMillisThreshold);
    }

    @Override
    public String name() {
        return ELAPSED_TIME_TRACKER.getName();
    }

    @Override
    public Optional<TaskCancellation.Reason> checkAndMaybeGetCancellationReason(Task task) {
        long usage = timeNanosSupplier.getAsLong() - task.getStartTimeNanos();
        long threshold = (task instanceof SearchTask) ? getElapsedTimeNanosThresholdForSearchQuery() : getElapsedTimeNanosThreshold();

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

    public long getElapsedTimeNanosThresholdForSearchQuery() {
        return TimeUnit.MILLISECONDS.toNanos(elapsedTimeMillisThresholdForSearchQuery);
    }

    public long getElapsedTimeNanosThreshold() {
        return TimeUnit.MILLISECONDS.toNanos(elapsedTimeMillisThreshold);
    }

    public void setElapsedTimeMillisThresholdForSearchQuery(long elapsedTimeMillisThresholdForSearchQuery) {
        this.elapsedTimeMillisThresholdForSearchQuery = elapsedTimeMillisThresholdForSearchQuery;
    }

    public void setElapsedTimeMillisThreshold(long elapsedTimeMillisThreshold) {
        this.elapsedTimeMillisThreshold = elapsedTimeMillisThreshold;
    }

    @Override
    public TaskResourceUsageTracker.Stats searchTaskStats(List<? extends Task> searchTasks) {
        long now = timeNanosSupplier.getAsLong();
        long currentMax = searchTasks.stream().mapToLong(t -> now - t.getStartTimeNanos()).max().orElse(0);
        long currentAvg = (long) searchTasks.stream().mapToLong(t -> now - t.getStartTimeNanos()).average().orElse(0);
        return new Stats(getSearchTaskCancellationCount(), currentMax, currentAvg);
    }

    @Override
    public TaskResourceUsageTracker.Stats searchShardTaskStats(List<? extends Task> searchShardTasks) {
        long now = timeNanosSupplier.getAsLong();
        long currentMax = searchShardTasks.stream().mapToLong(t -> now - t.getStartTimeNanos()).max().orElse(0);
        long currentAvg = (long) searchShardTasks.stream().mapToLong(t -> now - t.getStartTimeNanos()).average().orElse(0);
        return new Stats(getSearchShardTaskCancellationCount(), currentMax, currentAvg);
    }

    /**
     * Stats related to ElapsedTimeTracker.
     */
    public static class Stats implements TaskResourceUsageTracker.Stats {
        private final long cancellationCount;
        private final long currentMax;
        private final long currentAvg;

        public Stats(long cancellationCount, long currentMax, long currentAvg) {
            this.cancellationCount = cancellationCount;
            this.currentMax = currentMax;
            this.currentAvg = currentAvg;
        }

        public Stats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("cancellation_count", cancellationCount)
                .humanReadableField("current_max_millis", "current_max", new TimeValue(currentMax, TimeUnit.NANOSECONDS))
                .humanReadableField("current_avg_millis", "current_avg", new TimeValue(currentAvg, TimeUnit.NANOSECONDS))
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(cancellationCount);
            out.writeVLong(currentMax);
            out.writeVLong(currentAvg);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats stats = (Stats) o;
            return cancellationCount == stats.cancellationCount && currentMax == stats.currentMax && currentAvg == stats.currentAvg;
        }

        @Override
        public int hashCode() {
            return Objects.hash(cancellationCount, currentMax, currentAvg);
        }
    }
}
