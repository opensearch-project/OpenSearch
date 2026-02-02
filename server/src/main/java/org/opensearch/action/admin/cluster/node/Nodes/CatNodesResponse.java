/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.Nodes;

import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A response of a cat shards request.
 *
 * @opensearch.api
 */
public class CatNodesResponse extends ActionResponse {

    private ClusterStateResponse clusterStateResponse;
    private NodesInfoResponse nodesInfoResponse;
    private NodesStatsResponse nodesStatsResponse;

    public CatNodesResponse(
        ClusterStateResponse clusterStateResponse,
        NodesInfoResponse nodesInfoResponse,
        NodesStatsResponse nodesStatsResponse
    ) {
        this.clusterStateResponse = clusterStateResponse;
        this.nodesInfoResponse = nodesInfoResponse;
        this.nodesStatsResponse = nodesStatsResponse;
    }

    public CatNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterStateResponse.writeTo(out);
        nodesInfoResponse.writeTo(out);
        nodesStatsResponse.writeTo(out);
    }

    public NodesStatsResponse getNodesStatsResponse() {
        return nodesStatsResponse;
    }

    public NodesInfoResponse getNodesInfoResponse() {
        return nodesInfoResponse;
    }

    public ClusterStateResponse getClusterStateResponse() {
        return clusterStateResponse;
    }
}
