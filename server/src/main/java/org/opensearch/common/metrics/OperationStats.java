/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.metrics;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * An immutable representation of a {@link OperationMetrics}
 */
public class OperationStats implements Writeable, ToXContentFragment {
    private final long count;
    private final long totalTimeInMillis;
    private final long current;
    private final long failedCount;

    public OperationStats(long count, long totalTimeInMillis, long current, long failedCount) {
        this.count = count;
        this.totalTimeInMillis = totalTimeInMillis;
        this.current = current;
        this.failedCount = failedCount;
    }

    /**
     * Read from a stream.
     */
    public OperationStats(StreamInput in) throws IOException {
        count = in.readVLong();
        totalTimeInMillis = in.readVLong();
        current = in.readVLong();
        failedCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(current);
        out.writeVLong(failedCount);
    }

    /**
     * @return The total number of executed operations.
     */
    public long getCount() {
        return count;
    }

    /**
     * @return The total time spent of in millis.
     */
    public long getTotalTimeInMillis() {
        return totalTimeInMillis;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field("count", count)
            .humanReadableField("time_in_millis", "time", new TimeValue(totalTimeInMillis, TimeUnit.MILLISECONDS))
            .field("current", current)
            .field("failed", failedCount);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationStats that = (OperationStats) o;
        return Objects.equals(count, that.count)
            && Objects.equals(totalTimeInMillis, that.totalTimeInMillis)
            && Objects.equals(failedCount, that.failedCount)
            && Objects.equals(current, that.current);
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, totalTimeInMillis, failedCount, current);
    }
}
