/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.metrics;

import org.opensearch.Version;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * An immutable representation of a {@link OperationMetrics}
 */
public class OperationStats implements Writeable, ToXContentFragment {
    private final long count;
    private final long totalTime;
    private final long current;
    private final long failedCount;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private final Map<TimeUnit, String> TIME_UNIT_TO_FIELD_NAME_MAP = Map.of(
        TimeUnit.NANOSECONDS,
        "time_in_nanos",
        TimeUnit.MICROSECONDS,
        "time_in_micros",
        TimeUnit.MILLISECONDS,
        "time_in_millis"
    );

    public OperationStats(long count, long totalTime, long current, long failedCount) {
        this.count = count;
        this.totalTime = totalTime;
        this.current = current;
        this.failedCount = failedCount;
    }

    public OperationStats(long count, long totalTime, long current, long failedCount, TimeUnit timeUnit) {
        this(count, totalTime, current, failedCount);
        this.timeUnit = timeUnit;
    }

    /**
     * Read from a stream.
     */
    public OperationStats(StreamInput in) throws IOException {
        count = in.readVLong();
        totalTime = in.readVLong();
        current = in.readVLong();
        failedCount = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_3_3_0)) {
            timeUnit = in.readEnum(TimeUnit.class);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(totalTime);
        out.writeVLong(current);
        out.writeVLong(failedCount);
        if (out.getVersion().onOrAfter(Version.V_3_3_0)) {
            out.writeEnum(timeUnit);
        }
    }

    /**
     * @return The total number of executed operations.
     */
    public long getCount() {
        return count;
    }

    /**
     * @return The total time spent of in target time unit. Default is millis.
     */
    public long getTotalTime() {
        return totalTime;
    }

    /**
     * @return The total number of operations currently executing.
     */
    public long getCurrent() {
        return current;
    }

    /**
     * @return The total number of operations that have failed.
     */
    public long getFailedCount() {
        return failedCount;
    }

    /**
     * @return The time unit of the stats
     */
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field("count", count)
            .humanReadableField(TIME_UNIT_TO_FIELD_NAME_MAP.get(timeUnit), "time", new TimeValue(totalTime, timeUnit))
            .field("current", current)
            .field("failed", failedCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationStats that = (OperationStats) o;
        return Objects.equals(count, that.count)
            && Objects.equals(totalTime, that.totalTime)
            && Objects.equals(failedCount, that.failedCount)
            && Objects.equals(current, that.current);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, totalTime, failedCount, current);
    }
}
