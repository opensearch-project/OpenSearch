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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Instruction prepended to every hash-shuffle worker fragment's plan alternatives. The
 * backend's handler creates a worker-mode {@code SessionContext} (no shard view, no listing
 * table) and returns it as the {@link BackendExecutionContext}, so subsequent
 * {@link ShuffleScanInstructionNode} handlers can register named-input streams against it.
 *
 * <p>Beyond the buffer-triple key + expected-sender counts, the node carries two per-worker-stage
 * decisions the coordinator makes because that is where the stats live:
 * <ul>
 *   <li>{@code preferHashJoin} — {@code false} when this worker join's build side is estimated too
 *       large for an in-memory hash table, so the backend builds a spillable sort-merge join instead
 *       of the non-spillable hash-join build. Defaults {@code true} (hash-join, the historical
 *       behavior).</li>
 *   <li>{@code pipelined} — {@code true} to drain CONCURRENTLY with the producers (low latency,
 *       residency bounded by arrival rate), {@code false} for the MATERIALIZED shape: the producers
 *       all finish first and the consumer's buffer spills, so residency is bounded by a shallow
 *       window and backpressure cannot fail the query. Per stage rather than per node because the
 *       right answer is a property of the shuffle's size, not of the cluster.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class ShuffleWorkerSetupInstructionNode implements InstructionNode {

    private final String queryId;
    private final int targetStageId;
    private final int partitionIndex;
    private final Map<String, Integer> expectedSendersBySlot;
    private final boolean preferHashJoin;
    private final boolean pipelined;

    /**
     * @param queryId               worker buffer triple key
     * @param targetStageId         worker buffer triple key
     * @param partitionIndex        worker buffer triple key
     * @param expectedSendersBySlot expected isLast count per slot label (see {@link ShuffleSlots}) for
     *                              this partition. Must name EVERY slot the consumer will read — the
     *                              handler declares them all in one call so the buffer's
     *                              {@code awaitReady} waits on the complete set.
     * @param preferHashJoin        false → the backend builds a spillable sort-merge join for this worker
     * @param pipelined             false → this worker uses the materialized shape (producers finish
     *                              first, consumer buffer spills) instead of draining concurrently
     */
    public ShuffleWorkerSetupInstructionNode(
        String queryId,
        int targetStageId,
        int partitionIndex,
        Map<String, Integer> expectedSendersBySlot,
        boolean preferHashJoin,
        boolean pipelined
    ) {
        this.queryId = queryId;
        this.targetStageId = targetStageId;
        this.partitionIndex = partitionIndex;
        // LinkedHashMap: slot order is the consumer's input order, which keeps log output and the
        // handler's declaration order deterministic (the buffer itself is order-insensitive).
        this.expectedSendersBySlot = Collections.unmodifiableMap(new LinkedHashMap<>(expectedSendersBySlot));
        this.preferHashJoin = preferHashJoin;
        this.pipelined = pipelined;
    }

    /**
     * Binary convenience form for a two-slot (hash-join) consumer. A negative count omits that slot,
     * which is how the single-slot aggregate-shuffle path declares an unused right side.
     */
    public ShuffleWorkerSetupInstructionNode(
        String queryId,
        int targetStageId,
        int partitionIndex,
        int leftExpectedSenders,
        int rightExpectedSenders,
        boolean preferHashJoin,
        boolean pipelined
    ) {
        this(queryId, targetStageId, partitionIndex, binarySlots(leftExpectedSenders, rightExpectedSenders), preferHashJoin, pipelined);
    }

    private static Map<String, Integer> binarySlots(int leftExpectedSenders, int rightExpectedSenders) {
        Map<String, Integer> bySlot = new LinkedHashMap<>();
        if (leftExpectedSenders >= 0) {
            bySlot.put(ShuffleSlots.LEFT, leftExpectedSenders);
        }
        if (rightExpectedSenders >= 0) {
            bySlot.put(ShuffleSlots.RIGHT, rightExpectedSenders);
        }
        return bySlot;
    }

    public ShuffleWorkerSetupInstructionNode(StreamInput in) throws IOException {
        this.queryId = in.readString();
        this.targetStageId = in.readVInt();
        this.partitionIndex = in.readVInt();
        int slotCount = in.readVInt();
        Map<String, Integer> bySlot = new LinkedHashMap<>();
        for (int i = 0; i < slotCount; i++) {
            bySlot.put(in.readString(), in.readVInt());
        }
        this.expectedSendersBySlot = Collections.unmodifiableMap(bySlot);
        this.preferHashJoin = in.readBoolean();
        this.pipelined = in.readBoolean();
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

    /** Expected isLast count per slot label; never null, possibly empty. */
    public Map<String, Integer> getExpectedSendersBySlot() {
        return expectedSendersBySlot;
    }

    /** Expected isLast count for {@code slot}, or -1 when this node does not declare that slot. */
    public int getExpectedSenders(String slot) {
        return expectedSendersBySlot.getOrDefault(slot, -1);
    }

    public int getLeftExpectedSenders() {
        return getExpectedSenders(ShuffleSlots.LEFT);
    }

    public int getRightExpectedSenders() {
        return getExpectedSenders(ShuffleSlots.RIGHT);
    }

    public boolean getPreferHashJoin() {
        return preferHashJoin;
    }

    /** True when this worker drains concurrently with its producers; false for the materialized shape. */
    public boolean isPipelined() {
        return pipelined;
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
        out.writeVInt(expectedSendersBySlot.size());
        for (Map.Entry<String, Integer> e : expectedSendersBySlot.entrySet()) {
            out.writeString(e.getKey());
            out.writeVInt(e.getValue());
        }
        out.writeBoolean(preferHashJoin);
        out.writeBoolean(pipelined);
    }
}
