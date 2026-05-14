/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Native task cancellation counters from the DataFusion Rust layer.
 *
 * <p>Implements {@link PluginStats} for SPI compatibility,
 * {@link Writeable} for transport serialization, and {@link ToXContentFragment}
 * for JSON rendering within the {@code task_cancellation} stats object.
 *
 * <p>Contains 4 counters tracking native search tasks and native search shard tasks
 * that continue executing after cancellation.
 */
public class DataFusionNativeNodeStats implements PluginStats, Writeable, ToXContentFragment {

    private final long nativeSearchTaskCurrent;
    private final long nativeSearchTaskTotal;
    private final long nativeSearchShardTaskCurrent;
    private final long nativeSearchShardTaskTotal;

    /**
     * Construct from individual counter values.
     *
     * @param nativeSearchTaskCurrent      current count of native search tasks executing post-cancellation
     * @param nativeSearchTaskTotal        total count of native search tasks that executed post-cancellation
     * @param nativeSearchShardTaskCurrent current count of native search shard tasks executing post-cancellation
     * @param nativeSearchShardTaskTotal   total count of native search shard tasks that executed post-cancellation
     */
    public DataFusionNativeNodeStats(
        long nativeSearchTaskCurrent,
        long nativeSearchTaskTotal,
        long nativeSearchShardTaskCurrent,
        long nativeSearchShardTaskTotal
    ) {
        this.nativeSearchTaskCurrent = nativeSearchTaskCurrent;
        this.nativeSearchTaskTotal = nativeSearchTaskTotal;
        this.nativeSearchShardTaskCurrent = nativeSearchShardTaskCurrent;
        this.nativeSearchShardTaskTotal = nativeSearchShardTaskTotal;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionNativeNodeStats(StreamInput in) throws IOException {
        this.nativeSearchTaskCurrent = in.readVLong();
        this.nativeSearchTaskTotal = in.readVLong();
        this.nativeSearchShardTaskCurrent = in.readVLong();
        this.nativeSearchShardTaskTotal = in.readVLong();
    }

    public long getNativeSearchTaskCurrent() {
        return nativeSearchTaskCurrent;
    }

    public long getNativeSearchTaskTotal() {
        return nativeSearchTaskTotal;
    }

    public long getNativeSearchShardTaskCurrent() {
        return nativeSearchShardTaskCurrent;
    }

    public long getNativeSearchShardTaskTotal() {
        return nativeSearchShardTaskTotal;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(nativeSearchTaskCurrent);
        out.writeVLong(nativeSearchTaskTotal);
        out.writeVLong(nativeSearchShardTaskCurrent);
        out.writeVLong(nativeSearchShardTaskTotal);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("native_search_task");
        builder.field("current_count_post_cancel", nativeSearchTaskCurrent);
        builder.field("total_count_post_cancel", nativeSearchTaskTotal);
        builder.endObject();
        builder.startObject("native_search_shard_task");
        builder.field("current_count_post_cancel", nativeSearchShardTaskCurrent);
        builder.field("total_count_post_cancel", nativeSearchShardTaskTotal);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionNativeNodeStats that = (DataFusionNativeNodeStats) o;
        return nativeSearchTaskCurrent == that.nativeSearchTaskCurrent
            && nativeSearchTaskTotal == that.nativeSearchTaskTotal
            && nativeSearchShardTaskCurrent == that.nativeSearchShardTaskCurrent
            && nativeSearchShardTaskTotal == that.nativeSearchShardTaskTotal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativeSearchTaskCurrent, nativeSearchTaskTotal, nativeSearchShardTaskCurrent, nativeSearchShardTaskTotal);
    }
}
