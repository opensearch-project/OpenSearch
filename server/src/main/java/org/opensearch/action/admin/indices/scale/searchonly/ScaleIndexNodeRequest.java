/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;

/**
 * A transport request sent to nodes to facilitate shard synchronization during search-only scaling operations.
 * <p>
 * This request is sent from the cluster manager to data nodes that host primary shards for the target index
 * during scale operations. It contains the index name and a list of shard IDs that need to be synchronized
 * before completing a scale-down operation.
 * <p>
 * When a node receives this request, it performs final sync and flush operations on the specified shards,
 * ensuring all operations are committed and the remote store is synced. This is a crucial step in
 * the scale-down process to ensure no data loss occurs when the index transitions to search-only mode.
 */
class ScaleIndexNodeRequest extends TransportRequest {
    private final String index;
    private final List<ShardId> shardIds;

    /**
     * Constructs a new NodeSearchOnlyRequest.
     *
     * @param index    the name of the index being scaled
     * @param shardIds the list of shard IDs to be synchronized on the target node
     */
    ScaleIndexNodeRequest(String index, List<ShardId> shardIds) {
        this.index = index;
        this.shardIds = shardIds;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if there is an I/O error during deserialization
     */
    ScaleIndexNodeRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shardIds = in.readList(ShardId::new);
    }

    /**
     * Serializes this request to the given output stream.
     *
     * @param out the output stream to write to
     * @throws IOException if there is an I/O error during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeList(shardIds);
    }

    /**
     * Returns the index name associated with this request.
     *
     * @return the index name
     */
    String getIndex() {
        return index;
    }

    /**
     * Returns the list of shard IDs to be synchronized.
     *
     * @return the list of shard IDs
     */
    List<ShardId> getShardIds() {
        return shardIds;
    }
}
