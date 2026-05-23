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
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats for a partition budget gate — a node-level semaphore that limits
 * concurrent {@code stream_next} batch fetches across all active queries.
 *
 * <p>Contains 6 metrics: the semaphore capacity, current utilization, cumulative
 * wait time, cumulative batch count, poison permits held, and target max permits.
 *
 * <p>Two instances exist at runtime: one for the datanode gate (shard-scan partitions)
 * and one for the coordinator gate (reduce partitions).
 */
public class PartitionGateStats implements Writeable, ToXContentFragment {

    /** JSON key used when serializing this gate's stats. */
    private final String name;

    /** Total semaphore capacity (immutable after initialization). */
    public final long maxPermits;

    /** Number of permits currently held (point-in-time). */
    public final long activePermits;

    /** Cumulative milliseconds partitions spent waiting for permits. */
    public final long totalWaitDurationMs;

    /** Cumulative count of batches started (permits granted) since startup. */
    public final long totalBatchesStarted;

    /** Number of poison permits currently held to reduce effective capacity. */
    public final long poisonPermits;

    /** Target max permits during an in-progress resize; equals maxPermits when idle. */
    public final long targetMaxPermits;

    /**
     * Construct from explicit field values.
     *
     * @param name                JSON key for this gate (e.g. "datanode_gate", "coordinator_gate")
     * @param maxPermits          total semaphore capacity
     * @param activePermits       currently held permits
     * @param totalWaitDurationMs cumulative wait time in milliseconds
     * @param totalBatchesStarted cumulative batches started
     * @param poisonPermits       number of poison permits held
     * @param targetMaxPermits    target max permits during resize
     */
    public PartitionGateStats(
        String name,
        long maxPermits,
        long activePermits,
        long totalWaitDurationMs,
        long totalBatchesStarted,
        long poisonPermits,
        long targetMaxPermits
    ) {
        this.name = name;
        this.maxPermits = maxPermits;
        this.activePermits = activePermits;
        this.totalWaitDurationMs = totalWaitDurationMs;
        this.totalBatchesStarted = totalBatchesStarted;
        this.poisonPermits = poisonPermits;
        this.targetMaxPermits = targetMaxPermits;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public PartitionGateStats(StreamInput in) throws IOException {
        this.name = in.readString();
        this.maxPermits = in.readVLong();
        this.activePermits = in.readVLong();
        this.totalWaitDurationMs = in.readVLong();
        this.totalBatchesStarted = in.readVLong();
        this.poisonPermits = in.readVLong();
        this.targetMaxPermits = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(maxPermits);
        out.writeVLong(activePermits);
        out.writeVLong(totalWaitDurationMs);
        out.writeVLong(totalBatchesStarted);
        out.writeVLong(poisonPermits);
        out.writeVLong(targetMaxPermits);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("max_permits", maxPermits);
        builder.field("active_permits", activePermits);
        builder.field("total_wait_duration_ms", totalWaitDurationMs);
        builder.field("total_batches_started", totalBatchesStarted);
        builder.field("poison_permits", poisonPermits);
        builder.field("target_max_permits", targetMaxPermits);
        builder.endObject();
        return builder;
    }

    /** Returns the JSON key for this gate. */
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionGateStats that = (PartitionGateStats) o;
        return maxPermits == that.maxPermits
            && activePermits == that.activePermits
            && totalWaitDurationMs == that.totalWaitDurationMs
            && totalBatchesStarted == that.totalBatchesStarted
            && poisonPermits == that.poisonPermits
            && targetMaxPermits == that.targetMaxPermits
            && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, maxPermits, activePermits, totalWaitDurationMs, totalBatchesStarted, poisonPermits, targetMaxPermits);
    }
}
