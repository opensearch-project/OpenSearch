/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.analytics.spi.DelegationDescriptor;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.InstructionType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport request carrying plan fragment alternatives to a data node for shard-level execution.
 *
 * <p>Each {@link PlanAlternative} represents a backend-specific serialized plan produced by
 * {@code FragmentConversionDriver}. The data node selects the best alternative based on
 * available backend capabilities.
 *
 * @opensearch.internal
 */
public class FragmentExecutionRequest extends ActionRequest {

    private final String queryId;
    private final int stageId;
    private final ShardId shardId;
    private final List<PlanAlternative> planAlternatives;

    // QTF fetch mode fields (null = normal query, non-null = fetch-by-row-id)
    private final long[] fetchRowIds;
    private final String[] fetchColumns;

    public FragmentExecutionRequest(String queryId, int stageId, ShardId shardId, List<PlanAlternative> planAlternatives) {
        this(queryId, stageId, shardId, planAlternatives, null, null);
    }

    public FragmentExecutionRequest(
        String queryId, int stageId, ShardId shardId,
        List<PlanAlternative> planAlternatives,
        long[] fetchRowIds, String[] fetchColumns
    ) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.shardId = shardId;
        this.planAlternatives = planAlternatives;
        this.fetchRowIds = fetchRowIds;
        this.fetchColumns = fetchColumns;
    }

    /** Factory for QTF fetch-mode requests. */
    public static FragmentExecutionRequest fetchMode(String queryId, ShardId shardId, long[] rowIds, String[] columns) {
        return new FragmentExecutionRequest(queryId, 0, shardId, List.of(), rowIds, columns);
    }

    public FragmentExecutionRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.stageId = in.readInt();
        this.shardId = new ShardId(in);
        int numAlternatives = in.readVInt();
        this.planAlternatives = new ArrayList<>(numAlternatives);
        for (int i = 0; i < numAlternatives; i++) {
            planAlternatives.add(new PlanAlternative(in));
        }
        // QTF fetch fields
        if (in.readBoolean()) {
            int len = in.readVInt();
            this.fetchRowIds = new long[len];
            for (int i = 0; i < len; i++) fetchRowIds[i] = in.readLong();
            this.fetchColumns = in.readStringArray();
        } else {
            this.fetchRowIds = null;
            this.fetchColumns = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeInt(stageId);
        shardId.writeTo(out);
        out.writeVInt(planAlternatives.size());
        for (PlanAlternative alt : planAlternatives) {
            alt.writeTo(out);
        }
        // QTF fetch fields
        out.writeBoolean(fetchRowIds != null);
        if (fetchRowIds != null) {
            out.writeVInt(fetchRowIds.length);
            for (long id : fetchRowIds) out.writeLong(id);
            out.writeStringArray(fetchColumns);
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public int getStageId() {
        return stageId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public List<PlanAlternative> getPlanAlternatives() {
        return planAlternatives;
    }

    public boolean isFetchMode() {
        return fetchRowIds != null;
    }

    public long[] getFetchRowIds() {
        return fetchRowIds;
    }

    public String[] getFetchColumns() {
        return fetchColumns;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        String desc = "queryId[" + queryId + "] stageId[" + stageId + "] shardId[" + shardId + "]";
        return new AnalyticsShardTask(id, type, action, desc, parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * A single plan alternative: a backend ID paired with its serialized fragment bytes
     * and ordered instruction nodes for data-node execution.
     * Produced by {@code FragmentConversionDriver.convertAll()} using the backend's
     * {@code FragmentConvertor}.
     */
    public static class PlanAlternative {
        private final String backendId;
        private final byte[] fragmentBytes;
        private final List<InstructionNode> instructions;
        private final DelegationDescriptor delegationDescriptor;

        public PlanAlternative(String backendId, byte[] fragmentBytes, List<InstructionNode> instructions) {
            this(backendId, fragmentBytes, instructions, null);
        }

        public PlanAlternative(
            String backendId,
            byte[] fragmentBytes,
            List<InstructionNode> instructions,
            DelegationDescriptor delegationDescriptor
        ) {
            this.backendId = backendId;
            this.fragmentBytes = fragmentBytes;
            this.instructions = instructions;
            this.delegationDescriptor = delegationDescriptor;
        }

        public PlanAlternative(StreamInput in) throws IOException {
            this.backendId = in.readString();
            byte[] bytes = in.readByteArray();
            this.fragmentBytes = (bytes.length == 0) ? null : bytes;
            int instructionCount = in.readVInt();
            List<InstructionNode> nodes = new ArrayList<>(instructionCount);
            for (int i = 0; i < instructionCount; i++) {
                InstructionType type = in.readEnum(InstructionType.class);
                nodes.add(type.readNode(in));
            }
            this.instructions = nodes;
            this.delegationDescriptor = in.readBoolean() ? new DelegationDescriptor(in) : null;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(backendId);
            out.writeByteArray(fragmentBytes != null ? fragmentBytes : new byte[0]);
            out.writeVInt(instructions.size());
            for (InstructionNode node : instructions) {
                out.writeEnum(node.type());
                node.writeTo(out);
            }
            if (delegationDescriptor != null) {
                out.writeBoolean(true);
                delegationDescriptor.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        public String getBackendId() {
            return backendId;
        }

        public byte[] getFragmentBytes() {
            return fragmentBytes;
        }

        public List<InstructionNode> getInstructions() {
            return instructions;
        }

        public DelegationDescriptor getDelegationDescriptor() {
            return delegationDescriptor;
        }
    }
}
