/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rest.pagination.PageToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A response of a cat shards request.
 *
 * @opensearch.api
 */
public class CatShardsResponse extends ActionResponse {

    private ClusterStateResponse clusterStateResponse = null;

    private IndicesStatsResponse indicesStatsResponse = null;
    private List<ShardRouting> responseShards = new ArrayList<>();
    private PageToken pageToken = null;

    public CatShardsResponse() {}

    public CatShardsResponse(StreamInput in) throws IOException {
        super(in);
        clusterStateResponse = new ClusterStateResponse(in);
        indicesStatsResponse = new IndicesStatsResponse(in);
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            responseShards = in.readList(ShardRouting::new);
            if (in.readBoolean()) {
                pageToken = PageToken.readPageToken(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterStateResponse.writeTo(out);
        indicesStatsResponse.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeList(responseShards);
            out.writeBoolean(pageToken != null);
            if (pageToken != null) {
                pageToken.writePageToken(out);
            }
        }
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

    public void setResponseShards(List<ShardRouting> responseShards) {
        this.responseShards = responseShards;
    }

    public List<ShardRouting> getResponseShards() {
        return this.responseShards;
    }

    public void setPageToken(PageToken pageToken) {
        this.pageToken = pageToken;
    }

    public PageToken getPageToken() {
        return this.pageToken;
    }
}
