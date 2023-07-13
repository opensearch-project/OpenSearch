/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.query;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Transport request for query search
 *
 * @opensearch.internal
 */
public class QuerySearchRequest extends TransportRequest implements IndicesRequest {

    private final ShardSearchContextId contextId;
    private final AggregatedDfs dfs;
    private final OriginalIndices originalIndices;
    private final ShardSearchRequest shardSearchRequest;

    public QuerySearchRequest(
        OriginalIndices originalIndices,
        ShardSearchContextId contextId,
        ShardSearchRequest shardSearchRequest,
        AggregatedDfs dfs
    ) {
        this.contextId = contextId;
        this.dfs = dfs;
        this.shardSearchRequest = shardSearchRequest;
        this.originalIndices = originalIndices;
    }

    public QuerySearchRequest(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        dfs = new AggregatedDfs(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
        dfs.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalWriteable(shardSearchRequest);
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public AggregatedDfs dfs() {
        return dfs;
    }

    @Nullable
    public ShardSearchRequest shardSearchRequest() {
        return shardSearchRequest;
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
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers, this::getMetadataSupplier);
    }

    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append("id[");
        sb.append(contextId);
        sb.append("], ");
        sb.append("indices[");
        Strings.arrayToDelimitedString(originalIndices.indices(), ",", sb);
        sb.append("]");
        return sb.toString();
    }

    public String getMetadataSupplier() {
        return shardSearchRequest().getMetadataSupplier();
    }
}
