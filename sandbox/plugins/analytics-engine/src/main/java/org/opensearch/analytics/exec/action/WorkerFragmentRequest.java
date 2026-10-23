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
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport request for a hash-shuffle worker fragment. Carries the per-task plan alternatives
 * and partition index — no {@code shardId}, since a worker fragment never scans a shard.
 *
 * <p>Wire shape: {@code (queryId, stageId, partitionIndex, planAlternatives)}. The
 * data-node-side handler in {@code AnalyticsSearchTransportService} routes this to
 * {@code AnalyticsSearchService.executeWorkerFragmentStreamingAsync}, which skips
 * {@code IndicesService.getShard} and reader acquisition entirely.
 *
 * @opensearch.internal
 */
public class WorkerFragmentRequest extends ActionRequest {

    private final String queryId;
    private final int stageId;
    private final int partitionIndex;
    private final List<FragmentExecutionRequest.PlanAlternative> planAlternatives;

    public WorkerFragmentRequest(
        String queryId,
        int stageId,
        int partitionIndex,
        List<FragmentExecutionRequest.PlanAlternative> planAlternatives
    ) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.partitionIndex = partitionIndex;
        this.planAlternatives = planAlternatives;
    }

    public WorkerFragmentRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.stageId = in.readInt();
        this.partitionIndex = in.readVInt();
        int n = in.readVInt();
        this.planAlternatives = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            planAlternatives.add(new FragmentExecutionRequest.PlanAlternative(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeInt(stageId);
        out.writeVInt(partitionIndex);
        out.writeVInt(planAlternatives.size());
        for (FragmentExecutionRequest.PlanAlternative alt : planAlternatives) {
            alt.writeTo(out);
        }
    }

    public String getQueryId() {
        return queryId;
    }

    public int getStageId() {
        return stageId;
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public List<FragmentExecutionRequest.PlanAlternative> getPlanAlternatives() {
        return planAlternatives;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        String desc = "queryId[" + queryId + "] stageId[" + stageId + "] partition[" + partitionIndex + "]";
        return new AnalyticsShardTask(id, type, action, desc, parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
