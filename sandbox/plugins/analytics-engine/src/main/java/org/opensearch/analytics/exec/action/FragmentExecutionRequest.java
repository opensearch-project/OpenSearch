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

    public FragmentExecutionRequest(String queryId, int stageId, ShardId shardId, List<PlanAlternative> planAlternatives) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.shardId = shardId;
        this.planAlternatives = planAlternatives;
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
     * A single plan alternative: a backend ID paired with its serialized fragment bytes.
     * Produced by {@code FragmentConversionDriver.convertAll()} using the backend's
     * {@code FragmentConvertor}.
     */
    public static class PlanAlternative {
        private final String backendId;
        private final byte[] fragmentBytes;

        public PlanAlternative(String backendId, byte[] fragmentBytes) {
            this.backendId = backendId;
            this.fragmentBytes = fragmentBytes;
        }

        public PlanAlternative(StreamInput in) throws IOException {
            this.backendId = in.readString();
            byte[] bytes = in.readByteArray();
            this.fragmentBytes = (bytes.length == 0) ? null : bytes;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(backendId);
            out.writeByteArray(fragmentBytes != null ? fragmentBytes : new byte[0]);
        }

        public String getBackendId() {
            return backendId;
        }

        public byte[] getFragmentBytes() {
            return fragmentBytes;
        }
    }
}
