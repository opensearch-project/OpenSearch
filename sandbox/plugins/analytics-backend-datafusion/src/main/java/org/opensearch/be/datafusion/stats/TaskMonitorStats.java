/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * 5 fields per operation type from {@code tokio_metrics::TaskMonitor::cumulative()}.
 * Three duration fields plus instrumented/dropped counts that enable computing alive task count client-side.
 */
public class TaskMonitorStats implements Writeable {
    /** Total time spent polling instrumented futures, in milliseconds. */
    public final long totalPollDurationMs;
    /** Total time tasks spent waiting in the scheduler queue, in milliseconds. */
    public final long totalScheduledDurationMs;
    /** Total time tasks spent idle between polls, in milliseconds. */
    public final long totalIdleDurationMs;
    /** Cumulative number of tasks instrumented by this monitor. */
    public final long instrumentedCount;
    /** Cumulative number of instrumented tasks that have been dropped (completed). */
    public final long droppedCount;

    /**
     * Construct from explicit field values.
     *
     * @param totalPollDurationMs      total poll duration in milliseconds
     * @param totalScheduledDurationMs total scheduled duration in milliseconds
     * @param totalIdleDurationMs      total idle duration in milliseconds
     * @param instrumentedCount        cumulative instrumented task count
     * @param droppedCount             cumulative dropped (completed) task count
     */
    public TaskMonitorStats(
        long totalPollDurationMs,
        long totalScheduledDurationMs,
        long totalIdleDurationMs,
        long instrumentedCount,
        long droppedCount
    ) {
        this.totalPollDurationMs = totalPollDurationMs;
        this.totalScheduledDurationMs = totalScheduledDurationMs;
        this.totalIdleDurationMs = totalIdleDurationMs;
        this.instrumentedCount = instrumentedCount;
        this.droppedCount = droppedCount;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public TaskMonitorStats(StreamInput in) throws IOException {
        this.totalPollDurationMs = in.readVLong();
        this.totalScheduledDurationMs = in.readVLong();
        this.totalIdleDurationMs = in.readVLong();
        this.instrumentedCount = in.readVLong();
        this.droppedCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalPollDurationMs);
        out.writeVLong(totalScheduledDurationMs);
        out.writeVLong(totalIdleDurationMs);
        out.writeVLong(instrumentedCount);
        out.writeVLong(droppedCount);
    }

    /**
     * Render all 5 fields as snake_case JSON fields.
     *
     * @param builder the XContent builder to write to
     * @throws IOException if writing fails
     */
    public void toXContent(XContentBuilder builder) throws IOException {
        builder.field("total_poll_duration_ms", totalPollDurationMs);
        builder.field("total_scheduled_duration_ms", totalScheduledDurationMs);
        builder.field("total_idle_duration_ms", totalIdleDurationMs);
        builder.field("instrumented_count", instrumentedCount);
        builder.field("dropped_count", droppedCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskMonitorStats that = (TaskMonitorStats) o;
        return totalPollDurationMs == that.totalPollDurationMs
            && totalScheduledDurationMs == that.totalScheduledDurationMs
            && totalIdleDurationMs == that.totalIdleDurationMs
            && instrumentedCount == that.instrumentedCount
            && droppedCount == that.droppedCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalPollDurationMs, totalScheduledDurationMs, totalIdleDurationMs, instrumentedCount, droppedCount);
    }
}
