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
import java.util.Map;

/**
 * Per-shard QTF (Query-Then-Fetch / late-materialization) fetch-by-rowids request. Carries
 * the ascending {@code rowIds} (data-node contract) plus the column projection. Routed to
 * the data node by {@link FetchByRowIdsAction}; resolved against the same backend that ran
 * the query phase via {@code backendId} — replaces the prior 5-arg
 * {@code AnalyticsSearchService.executeFetchByRowIds} that picked a backend with
 * {@code backends.values().iterator().next()}.
 *
 * @opensearch.internal
 */
public class FetchByRowIdsRequest extends ActionRequest implements ShardInvocationRequest {

    private final String queryId;
    private final int stageId;
    private final ShardId shardId;
    private final String backendId;
    private final long[] rowIds;
    private final String[] columns;

    public FetchByRowIdsRequest(String queryId, int stageId, ShardId shardId, String backendId, long[] rowIds, String[] columns) {
        this.queryId = queryId;
        this.stageId = stageId;
        this.shardId = shardId;
        this.backendId = backendId;
        this.rowIds = rowIds;
        this.columns = columns;
    }

    public FetchByRowIdsRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.stageId = in.readInt();
        this.shardId = new ShardId(in);
        this.backendId = in.readString();
        this.rowIds = in.readLongArray();
        this.columns = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeInt(stageId);
        shardId.writeTo(out);
        out.writeString(backendId);
        out.writeLongArray(rowIds);
        out.writeStringArray(columns);
    }

    @Override
    public String getQueryId() {
        return queryId;
    }

    @Override
    public int getStageId() {
        return stageId;
    }

    @Override
    public ShardId getShardId() {
        return shardId;
    }

    public String getBackendId() {
        return backendId;
    }

    public long[] getRowIds() {
        return rowIds;
    }

    public String[] getColumns() {
        return columns;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        String desc = "queryId[" + queryId + "] stageId[" + stageId + "] shardId[" + shardId + "] rowIds[" + rowIds.length + "]";
        return new AnalyticsShardTask(id, type, action, desc, parentTaskId, headers);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
