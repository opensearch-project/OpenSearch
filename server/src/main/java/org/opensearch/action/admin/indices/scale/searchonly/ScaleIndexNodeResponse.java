/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.List;

/**
 * Response sent from nodes after processing a {@link ScaleIndexNodeRequest} during search-only scaling operations.
 * <p>
 * This response contains information about the node that processed the request and the results of
 * synchronization attempts for each requested shard. The cluster manager uses these responses to
 * determine whether it's safe to proceed with finalizing a scale-down operation.
 * <p>
 * Each response includes details about whether shards have any uncommitted operations or need
 * additional synchronization, which would indicate the scale operation should be delayed until
 * the cluster reaches a stable state.
 */
class ScaleIndexNodeResponse extends TransportResponse {
    private final DiscoveryNode node;
    private final List<ScaleIndexShardResponse> shardResponses;

    /**
     * Constructs a new NodeSearchOnlyResponse.
     *
     * @param node           the node that processed the synchronization request
     * @param shardResponses the list of responses from individual shard synchronization attempts
     */
    ScaleIndexNodeResponse(DiscoveryNode node, List<ScaleIndexShardResponse> shardResponses) {
        this.node = node;
        this.shardResponses = shardResponses;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to read from
     * @throws IOException if there is an I/O error during deserialization
     */
    ScaleIndexNodeResponse(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        shardResponses = in.readList(ScaleIndexShardResponse::new);
    }

    /**
     * Serializes this response to the given output stream.
     *
     * @param out the output stream to write to
     * @throws IOException if there is an I/O error during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        out.writeList(shardResponses);
    }

    /**
     * Returns the node that processed the synchronization request.
     *
     * @return the discovery node information
     */
    public DiscoveryNode getNode() {
        return node;
    }

    /**
     * Returns the list of shard-level synchronization responses.
     * <p>
     * These responses contain critical information about the state of each shard,
     * including whether there are uncommitted operations or if additional synchronization
     * is needed before the scale operation can safely proceed.
     *
     * @return the list of shard responses
     */
    public List<ScaleIndexShardResponse> getShardResponses() {
        return shardResponses;
    }
}
