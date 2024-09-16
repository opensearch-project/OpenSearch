/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.ProtobufSearchShardTask;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ProtobufShardSearchRequest;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.ProtobufTaskId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * Transport request for query search
 *
 * @opensearch.internal
 */
public class ProtobufQuerySearchRequest extends TransportRequest implements IndicesRequest {

    // TODO: proto message
    private final ShardSearchContextId contextId;
    private final AggregatedDfs dfs;
    private final OriginalIndices originalIndices;
    private final ProtobufShardSearchRequest shardSearchRequest;

    public ProtobufQuerySearchRequest(
        OriginalIndices originalIndices,
        ShardSearchContextId contextId,
        ProtobufShardSearchRequest shardSearchRequest,
        AggregatedDfs dfs
    ) {
        this.contextId = contextId;
        this.dfs = dfs;
        this.shardSearchRequest = shardSearchRequest;
        this.originalIndices = originalIndices;
    }

    public ProtobufQuerySearchRequest(byte[] in) throws IOException {
        super(in);
        contextId = null;
        dfs = null;
        originalIndices = null;
        shardSearchRequest = null;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        super.writeTo(out);
        // contextId.writeTo(out);
        // dfs.writeTo(out);
        // OriginalIndices.writeOriginalIndices(originalIndices, out);
        // out.writeOptionalWriteable(shardSearchRequest);
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public AggregatedDfs dfs() {
        return dfs;
    }

    @Nullable
    public ProtobufShardSearchRequest shardSearchRequest() {
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
    public ProtobufTask createProtobufTask(long id, String type, String action, ProtobufTaskId parentTaskId, Map<String, String> headers) {
        return new ProtobufSearchShardTask(id, type, action, getDescription(), parentTaskId, headers, this::getMetadataSupplier);
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
