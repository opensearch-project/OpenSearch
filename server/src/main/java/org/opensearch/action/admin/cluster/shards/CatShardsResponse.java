/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

public class CatShardsResponse extends ActionResponse {

    private ClusterStateResponse clusterStateResponse;

    private IndicesStatsResponse indicesStatsResponse;

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    public CatShardsResponse(){}

    public CatShardsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public void setClusterStateResponse(ClusterStateResponse clusterStateResponse) {
        this.clusterStateResponse = clusterStateResponse;
    }

    public ClusterStateResponse getClusterStateResponse() {
        return this.clusterStateResponse;
    }

    public void setIndicesStatsResponse(IndicesStatsResponse indicesStatsResponse) {
        this.indicesStatsResponse = indicesStatsResponse;
    }

    public IndicesStatsResponse getIndicesStatsResponse() {
        return this.indicesStatsResponse;
    }
}
