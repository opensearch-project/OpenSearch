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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Instruction for a hash-shuffle producer: hash-partition this data-node's local scan output by
 * {@code hashKeyChannels} into {@code partitionCount} buckets, then ship bucket {@code i} to the
 * worker at {@code targetWorkerNodeIds[i]} via {@code AnalyticsShuffleDataAction}. The {@code side}
 * tag ({@code "left"} / {@code "right"}) distinguishes which join input this producer feeds; the
 * worker side's {@code ShuffleBuffer} tracks per-side completion.
 *
 * <p>{@code queryId} and {@code targetStageId} key the worker-side shuffle buffer so senders and
 * the consumer meet at the same buffer.
 *
 * @opensearch.internal
 */
public class ShuffleProducerInstructionNode implements InstructionNode {

    private final List<Integer> hashKeyChannels;
    private final int partitionCount;
    private final List<String> targetWorkerNodeIds;
    private final String queryId;
    private final int targetStageId;
    private final String side;

    public ShuffleProducerInstructionNode(
        List<Integer> hashKeyChannels,
        int partitionCount,
        List<String> targetWorkerNodeIds,
        String queryId,
        int targetStageId,
        String side
    ) {
        this.hashKeyChannels = List.copyOf(hashKeyChannels);
        this.partitionCount = partitionCount;
        this.targetWorkerNodeIds = List.copyOf(targetWorkerNodeIds);
        this.queryId = queryId;
        this.targetStageId = targetStageId;
        this.side = side;
    }

    public ShuffleProducerInstructionNode(StreamInput in) throws IOException {
        int keyCount = in.readVInt();
        List<Integer> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add(in.readVInt());
        }
        this.hashKeyChannels = Collections.unmodifiableList(keys);
        this.partitionCount = in.readVInt();
        this.targetWorkerNodeIds = in.readStringList();
        this.queryId = in.readString();
        this.targetStageId = in.readVInt();
        this.side = in.readString();
    }

    public List<Integer> getHashKeyChannels() {
        return hashKeyChannels;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public List<String> getTargetWorkerNodeIds() {
        return targetWorkerNodeIds;
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

    @Override
    public InstructionType type() {
        return InstructionType.SHUFFLE_PRODUCER;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(hashKeyChannels.size());
        for (int key : hashKeyChannels) {
            out.writeVInt(key);
        }
        out.writeVInt(partitionCount);
        out.writeStringCollection(targetWorkerNodeIds);
        out.writeString(queryId);
        out.writeVInt(targetStageId);
        out.writeString(side);
    }
}
