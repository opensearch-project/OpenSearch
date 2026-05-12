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
 * 3 duration fields per operation type from {@code tokio_metrics::TaskMonitor::cumulative()}.
 */
public class TaskMonitorStats implements Writeable {
    /** Total time spent polling instrumented futures, in milliseconds. */
    public final long totalPollDurationMs;
    /** Total time tasks spent waiting in the scheduler queue, in milliseconds. */
    public final long totalScheduledDurationMs;
    /** Total time tasks spent idle between polls, in milliseconds. */
    public final long totalIdleDurationMs;

    /**
     * Construct from explicit field values.
     *
     * @param totalPollDurationMs      total poll duration in milliseconds
     * @param totalScheduledDurationMs total scheduled duration in milliseconds
     * @param totalIdleDurationMs      total idle duration in milliseconds
     */
    public TaskMonitorStats(long totalPollDurationMs, long totalScheduledDurationMs, long totalIdleDurationMs) {
        this.totalPollDurationMs = totalPollDurationMs;
        this.totalScheduledDurationMs = totalScheduledDurationMs;
        this.totalIdleDurationMs = totalIdleDurationMs;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalPollDurationMs);
        out.writeVLong(totalScheduledDurationMs);
        out.writeVLong(totalIdleDurationMs);
    }

    /**
     * Render all 3 fields as snake_case JSON fields.
     *
     * @param builder the XContent builder to write to
     * @throws IOException if writing fails
     */
    public void toXContent(XContentBuilder builder) throws IOException {
        builder.field("total_poll_duration_ms", totalPollDurationMs);
        builder.field("total_scheduled_duration_ms", totalScheduledDurationMs);
        builder.field("total_idle_duration_ms", totalIdleDurationMs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskMonitorStats that = (TaskMonitorStats) o;
        return totalPollDurationMs == that.totalPollDurationMs
            && totalScheduledDurationMs == that.totalScheduledDurationMs
            && totalIdleDurationMs == that.totalIdleDurationMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalPollDurationMs, totalScheduledDurationMs, totalIdleDurationMs);
    }
}
