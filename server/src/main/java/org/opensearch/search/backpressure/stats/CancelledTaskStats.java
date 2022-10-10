/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.tasks.CancellableTask;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Stats related to a task at the time of cancellation.
 */
public class CancelledTaskStats implements ToXContentObject, Writeable {
    private final long cpuUsageNanos;
    private final long heapUsageBytes;
    private final long elapsedTimeNanos;

    public CancelledTaskStats(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong());
    }

    public CancelledTaskStats(long cpuUsageNanos, long heapUsageBytes, long elapsedTimeNanos) {
        this.cpuUsageNanos = cpuUsageNanos;
        this.heapUsageBytes = heapUsageBytes;
        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    public static CancelledTaskStats from(CancellableTask task, LongSupplier timeNanosSupplier) {
        return new CancelledTaskStats(
            task.getTotalResourceStats().getCpuTimeInNanos(),
            task.getTotalResourceStats().getMemoryInBytes(),
            timeNanosSupplier.getAsLong() - task.getStartTimeNanos()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .humanReadableField("cpu_usage_millis", "cpu_usage", new TimeValue(cpuUsageNanos, TimeUnit.NANOSECONDS))
            .humanReadableField("heap_usage_bytes", "heap_usage", new ByteSizeValue(heapUsageBytes))
            .humanReadableField("elapsed_time_millis", "elapsed_time", new TimeValue(elapsedTimeNanos, TimeUnit.NANOSECONDS))
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(cpuUsageNanos);
        out.writeVLong(heapUsageBytes);
        out.writeVLong(elapsedTimeNanos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CancelledTaskStats that = (CancelledTaskStats) o;
        return cpuUsageNanos == that.cpuUsageNanos && heapUsageBytes == that.heapUsageBytes && elapsedTimeNanos == that.elapsedTimeNanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpuUsageNanos, heapUsageBytes, elapsedTimeNanos);
    }
}
