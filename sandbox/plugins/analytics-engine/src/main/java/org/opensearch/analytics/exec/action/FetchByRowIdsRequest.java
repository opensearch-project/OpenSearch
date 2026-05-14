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
 * Transport request for QTF fetch phase.
 * Carries global row IDs and column names to the data node for targeted row retrieval.
 */
public class FetchByRowIdsRequest extends ActionRequest {

    private final String queryId;
    private final ShardId shardId;
    private final long[] rowIds;
    private final String[] columns;

    public FetchByRowIdsRequest(String queryId, ShardId shardId, long[] rowIds, String[] columns) {
        this.queryId = queryId;
        this.shardId = shardId;
        this.rowIds = rowIds;
        this.columns = columns;
    }

    public FetchByRowIdsRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.shardId = new ShardId(in);
        this.rowIds = in.readLongArray();
        this.columns = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        shardId.writeTo(out);
        out.writeLongArray(rowIds);
        out.writeStringArray(columns);
    }

    public String getQueryId() {
        return queryId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long[] getRowIds() {
        return rowIds;
    }

    public String[] getColumns() {
        return columns;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new AnalyticsShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "fetch_by_row_ids{query=" + queryId + ", shard=" + shardId + ", rows=" + rowIds.length + "}";
    }
}
