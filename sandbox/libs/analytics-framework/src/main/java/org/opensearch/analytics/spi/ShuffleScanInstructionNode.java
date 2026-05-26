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

    public ShuffleScanInstructionNode(
        String namedInputId,
        int shufflePartitionIndex,
        int expectedSenders,
        String queryId,
        int targetStageId
    ) {
        this.namedInputId = namedInputId;
        this.shufflePartitionIndex = shufflePartitionIndex;
        this.expectedSenders = expectedSenders;
        this.queryId = queryId;
        this.targetStageId = targetStageId;
    }

    public ShuffleScanInstructionNode(StreamInput in) throws IOException {
        this.namedInputId = in.readString();
        this.shufflePartitionIndex = in.readVInt();
        this.expectedSenders = in.readVInt();
        this.queryId = in.readString();
        this.targetStageId = in.readVInt();
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
    }
}
