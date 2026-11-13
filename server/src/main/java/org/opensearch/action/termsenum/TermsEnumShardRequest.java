/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 * The request sent to one shard copy to list terms.
 *
 * @opensearch.internal
 */
public final class TermsEnumShardRequest extends TransportRequest implements IndicesRequest {

    private final ShardId shardId;
    private final OriginalIndices originalIndices;
    private final String field;
    private final String string;
    private final int size;
    private final boolean caseInsensitive;
    private final String searchAfter;
    private final QueryBuilder indexFilter;
    private final long nowInMillis;
    private final long timeoutMillis;

    /**
     * Creates the request for one shard.
     *
     * @param shardId the shard to enumerate
     * @param originalIndices the indices the user addressed
     * @param request the user request
     * @param nowInMillis the coordinator's clock, used to rewrite the index filter
     * @param timeoutMillis the time budget of this shard, which starts on receipt
     */
    public TermsEnumShardRequest(
        ShardId shardId,
        OriginalIndices originalIndices,
        TermsEnumRequest request,
        long nowInMillis,
        long timeoutMillis
    ) {
        this.shardId = shardId;
        this.originalIndices = originalIndices;
        this.field = request.field();
        this.string = request.string();
        this.size = request.size();
        this.caseInsensitive = request.caseInsensitive();
        this.searchAfter = request.searchAfter();
        this.indexFilter = request.indexFilter();
        this.nowInMillis = nowInMillis;
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Reads a request from the wire.
     *
     * @param in the stream to read from
     * @throws IOException if reading fails
     */
    public TermsEnumShardRequest(StreamInput in) throws IOException {
        super(in);
        shardId = new ShardId(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        field = in.readString();
        string = in.readString();
        size = in.readVInt();
        caseInsensitive = in.readBoolean();
        searchAfter = in.readOptionalString();
        indexFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        nowInMillis = in.readVLong();
        timeoutMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeString(field);
        out.writeString(string);
        out.writeVInt(size);
        out.writeBoolean(caseInsensitive);
        out.writeOptionalString(searchAfter);
        out.writeOptionalNamedWriteable(indexFilter);
        out.writeVLong(nowInMillis);
        out.writeVLong(timeoutMillis);
    }

    /** @return the shard to enumerate */
    public ShardId shardId() {
        return shardId;
    }

    /** @return the field whose terms are listed */
    public String field() {
        return field;
    }

    /** @return the required prefix */
    public String string() {
        return string;
    }

    /** @return the maximum number of terms to return */
    public int size() {
        return size;
    }

    /** @return whether the prefix match ignores ASCII case */
    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    /** @return the term after which results start, or {@code null} */
    public String searchAfter() {
        return searchAfter;
    }

    /** @return the query used to skip this shard, or {@code null} */
    public QueryBuilder indexFilter() {
        return indexFilter;
    }

    /** @return the coordinator's clock in epoch milliseconds */
    public long nowInMillis() {
        return nowInMillis;
    }

    /** @return the time budget of this shard in milliseconds, which starts on receipt */
    public long timeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "shardId[" + shardId + "]";
    }
}
