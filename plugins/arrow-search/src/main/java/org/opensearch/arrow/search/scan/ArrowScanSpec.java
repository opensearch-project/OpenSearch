/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.arrow.search.Column;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * What a shard scan reads and how it sizes its batches.
 *
 * @param columns              the columns, in schema order
 * @param batchBytes           the size batches grow to
 * @param firstBatchBytes      the size of the first batch; later batches double it up to {@code batchBytes}, so the first
 *                             rows arrive early
 * @param maxBatchRows         the maximum rows in a batch
 * @param maxRowBytes          the largest row a batch may hold; a larger row fails the scan
 * @param shardRowLimit        the maximum rows the shard streams across all its slices: the smaller of {@code size} and
 *                             {@code terminate_after}
 * @param producerReadyTimeout how long the shard waits for the coordinator to accept a batch
 */
public record ArrowScanSpec(List<Column> columns, long batchBytes, long firstBatchBytes, int maxBatchRows, long maxRowBytes,
    long shardRowLimit, TimeValue producerReadyTimeout) implements Writeable {

    /**
     * Creates a spec with an immutable copy of the columns.
     *
     * @param columns              the columns, in schema order
     * @param batchBytes           the size batches grow to
     * @param firstBatchBytes      the size of the first batch
     * @param maxBatchRows         the maximum rows in a batch
     * @param maxRowBytes          the largest row a batch may hold
     * @param shardRowLimit        the maximum rows the shard streams across all its slices
     * @param producerReadyTimeout how long the shard waits for the coordinator to accept a batch
     */
    public ArrowScanSpec {
        columns = List.copyOf(columns);
    }

    /**
     * Reads a spec written by {@link #writeTo}.
     *
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public ArrowScanSpec(StreamInput in) throws IOException {
        this(in.readList(Column::new), in.readVLong(), in.readVLong(), in.readVInt(), in.readVLong(), in.readVLong(), in.readTimeValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(columns);
        out.writeVLong(batchBytes);
        out.writeVLong(firstBatchBytes);
        out.writeVInt(maxBatchRows);
        out.writeVLong(maxRowBytes);
        out.writeVLong(shardRowLimit);
        out.writeTimeValue(producerReadyTimeout);
    }

    /**
     * Returns the byte budget of a batch: {@code firstBatchBytes} doubled for each batch before it, at most
     * {@code batchBytes}.
     *
     * @param batchIndex the zero-based index of the batch in its slice
     */
    public long batchBudget(int batchIndex) {
        final long ramped = batchIndex >= Long.numberOfLeadingZeros(firstBatchBytes) - 1 ? batchBytes : firstBatchBytes << batchIndex;
        return Math.max(1, Math.min(batchBytes, ramped));
    }

    /** Returns the columns, in schema order. */
    public List<Column> columns() {
        return columns;
    }

    /** Returns the size batches grow to. */
    public long batchBytes() {
        return batchBytes;
    }

    /** Returns the size of a slice's first batch. */
    public long firstBatchBytes() {
        return firstBatchBytes;
    }

    /** Returns the maximum rows in a batch. */
    public int maxBatchRows() {
        return maxBatchRows;
    }

    /** Returns the largest row a batch may hold. */
    public long maxRowBytes() {
        return maxRowBytes;
    }

    /** Returns the maximum rows the shard streams across all its slices. */
    public long shardRowLimit() {
        return shardRowLimit;
    }

    /** Returns how long the shard waits for the coordinator to accept a batch. */
    public TimeValue producerReadyTimeout() {
        return producerReadyTimeout;
    }
}
