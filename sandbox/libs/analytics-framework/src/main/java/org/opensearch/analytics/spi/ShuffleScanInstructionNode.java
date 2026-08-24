/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Instruction for a hash-shuffle worker: register a channel-backed input stream as the named input
 * for partition {@code shufflePartitionIndex}. The backend's handler wires a {@code PartitionStream}
 * (or equivalent consumer primitive) that reads from the per-node shuffle buffer, then registers
 * it as a {@code NamedScan} under {@code namedInputId}. The worker's Substrait plan references that
 * name, so the hash-join's input resolves to the partitioned stream.
 *
 * <p>{@code namedInputId} must match the canonical {@code "input-<producerStageId>"} the
 * fragment convertor emits when it rewrites the consumer fragment's {@code OpenSearchStageInputScan}
 * leaves. The convertor strips the {@code OpenSearchShuffleExchange} wrapper so the leaf
 * {@code StageInputScan} drives the NamedScan binding.
 *
 * <p>{@code side} ({@code "left"} or {@code "right"}) tells the consumer-side handler which half
 * of the join input this scan represents — drives the per-side buffer drain
 * ({@code buffer.getLeftData()} vs {@code getRightData()}). Producers stamp the same label on
 * outgoing shuffle requests, so the buffer routes payloads into the correct slice.
 *
 * <p>{@code queryId} and {@code targetStageId} key the worker-side shuffle buffer — paired with
 * {@code shufflePartitionIndex} they identify the {@code (queryId, stageId, partitionIndex)} bucket
 * the producers send into and the consumer drains here. Mirrors the same triple
 * {@link ShuffleProducerInstructionNode} carries.
 *
 * <p>{@code expectedSenders} tells the consumer how many upstream senders will mark {@code isLast}
 * before the stream is considered complete — i.e. the {@code awaitReady} count for this partition.
 *
 * @opensearch.internal
 */
public class ShuffleScanInstructionNode implements InstructionNode {

    private final String namedInputId;
    private final int shufflePartitionIndex;
    private final int expectedSenders;
    private final String queryId;
    private final int targetStageId;
    private final String side;
    private final byte[] producerPlanBytes;

    public ShuffleScanInstructionNode(
        String namedInputId,
        int shufflePartitionIndex,
        int expectedSenders,
        String queryId,
        int targetStageId,
        String side
    ) {
        this(namedInputId, shufflePartitionIndex, expectedSenders, queryId, targetStageId, side, null);
    }

    public ShuffleScanInstructionNode(
        String namedInputId,
        int shufflePartitionIndex,
        int expectedSenders,
        String queryId,
        int targetStageId,
        String side,
        byte[] producerPlanBytes
    ) {
        this.namedInputId = namedInputId;
        this.shufflePartitionIndex = shufflePartitionIndex;
        this.expectedSenders = expectedSenders;
        this.queryId = queryId;
        this.targetStageId = targetStageId;
        this.side = side;
        this.producerPlanBytes = producerPlanBytes;
    }

    public ShuffleScanInstructionNode(StreamInput in) throws IOException {
        this.namedInputId = in.readString();
        this.shufflePartitionIndex = in.readVInt();
        this.expectedSenders = in.readVInt();
        this.queryId = in.readString();
        this.targetStageId = in.readVInt();
        this.side = in.readString();
        this.producerPlanBytes = in.readBoolean() ? in.readByteArray() : null;
    }

    public String getNamedInputId() {
        return namedInputId;
    }

    public int getShufflePartitionIndex() {
        return shufflePartitionIndex;
    }

    public int getExpectedSenders() {
        return expectedSenders;
    }

    public String getQueryId() {
        return queryId;
    }

    public int getTargetStageId() {
        return targetStageId;
    }

    public String getSide() {
        return side;
    }

    /** The producer's converted PARTIAL-aggregate plan bytes, or {@code null}. When present, the
     *  worker handler derives the registered input schema by RE-LOWERING this plan (matching the
     *  coordinator-reduce path's {@code derive_schema_from_partial_plan}) instead of trusting the
     *  raw Arrow IPC schema, whose physical partial-state column names ({@code sum_qty[sum]}) don't
     *  match the FINAL Substrait's logical {@code base_schema} names ({@code sum_qty}) — the M3
     *  agg-shuffle worker-schema mismatch (TPC-H q1/q15). Only the agg-shuffle dispatch sets this;
     *  the join-shuffle path leaves it null (its producers ship raw rows, names already match). */
    public byte[] getProducerPlanBytes() {
        return producerPlanBytes;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SHUFFLE_SCAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(namedInputId);
        out.writeVInt(shufflePartitionIndex);
        out.writeVInt(expectedSenders);
        out.writeString(queryId);
        out.writeVInt(targetStageId);
        out.writeString(side);
        if (producerPlanBytes != null) {
            out.writeBoolean(true);
            out.writeByteArray(producerPlanBytes);
        } else {
            out.writeBoolean(false);
        }
    }
}
