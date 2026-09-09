/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.stats.transport;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Cluster-wide response for {@code GET _plugins/_analytics/stats}. Mirrors
 * {@code _nodes/stats} layout: each contacted node's analytics rollup is
 * rendered under its node id. The {@code _nodes} header (total/successful/failed)
 * and the {@code cluster_name} are emitted by the REST handler via
 * {@link org.opensearch.rest.action.RestActions#nodesResponse}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class AnalyticsStatsResponse extends BaseNodesResponse<AnalyticsStatsNodeResponse> implements ToXContentObject {

    public AnalyticsStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public AnalyticsStatsResponse(ClusterName clusterName, List<AnalyticsStatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<AnalyticsStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(AnalyticsStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<AnalyticsStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (AnalyticsStatsNodeResponse node : getNodes()) {
            builder.startObject(node.getNode().getId());
            node.getStats().toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
