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
 * Instruction prepended to every hash-shuffle worker fragment's plan alternatives. The
 * backend's handler creates a worker-mode {@code SessionContext} (no shard view, no listing
 * table) and returns it as the {@link BackendExecutionContext}, so subsequent
 * {@link ShuffleScanInstructionNode} handlers can register named-input streams against it.
 *
 * <p>Workers are stateless — no extra fields needed beyond the type marker.
 *
 * @opensearch.internal
 */
public class ShuffleWorkerSetupInstructionNode implements InstructionNode {

    private final String queryId;
    private final int targetStageId;
    private final int partitionIndex;
    private final int leftExpectedSenders;
    private final int rightExpectedSenders;

    /**
     * @param queryId             worker buffer triple key
     * @param targetStageId       worker buffer triple key
     * @param partitionIndex      worker buffer triple key
     * @param leftExpectedSenders expected isLast count from left producers for this partition
     * @param rightExpectedSenders expected isLast count from right producers for this partition
     */
    public ShuffleWorkerSetupInstructionNode(
        String queryId,
        int targetStageId,
        int partitionIndex,
        int leftExpectedSenders,
        int rightExpectedSenders
    ) {
        this.queryId = queryId;
        this.targetStageId = targetStageId;
        this.partitionIndex = partitionIndex;
        this.leftExpectedSenders = leftExpectedSenders;
        this.rightExpectedSenders = rightExpectedSenders;
    }

    public ShuffleWorkerSetupInstructionNode(StreamInput in) throws IOException {
        this.queryId = in.readString();
        this.targetStageId = in.readVInt();
        this.partitionIndex = in.readVInt();
        this.leftExpectedSenders = in.readVInt();
        this.rightExpectedSenders = in.readVInt();
    }

    public String getQueryId() {
        return queryId;
    }

    public int getTargetStageId() {
        return targetStageId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public int getLeftExpectedSenders() {
        return leftExpectedSenders;
    }

    public int getRightExpectedSenders() {
        return rightExpectedSenders;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHUFFLE_WORKER;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryId);
        out.writeVInt(targetStageId);
        out.writeVInt(partitionIndex);
        out.writeVInt(leftExpectedSenders);
        out.writeVInt(rightExpectedSenders);
    }
}
