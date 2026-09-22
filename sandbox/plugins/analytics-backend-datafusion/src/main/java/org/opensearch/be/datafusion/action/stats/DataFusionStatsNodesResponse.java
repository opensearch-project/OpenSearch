/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;

/**
 * Aggregated cluster-wide response for DataFusion stats.
 * Extends {@link BaseNodesResponse} and implements {@link ToXContentFragment}
 * to render the full JSON structure including {@code _nodes}, {@code cluster_name},
 * and per-node stats keyed by node ID.
 *
 * <p>JSON structure:
 * <pre>
 * {
 *   "_nodes": { "total": N, "successful": S, "failed": F, "failures": [...] },
 *   "cluster_name": "...",
 *   "nodes": {
 *     "node-id-1": {
 *       "name": "...",
 *       "host": "...",
 *       "transport_address": "...",
 *       // stats from DataFusionStats.toXContent
 *     }
 *   }
 * }
 * </pre>
 */
public class DataFusionStatsNodesResponse extends BaseNodesResponse<DataFusionStatsNodeResponse> implements ToXContentFragment {

    /**
     * Construct a nodes response with the given cluster name, successful node responses, and failures.
     *
     * @param clusterName the cluster name
     * @param nodes       the list of successful per-node responses
     * @param failures    the list of failed node exceptions
     */
    public DataFusionStatsNodesResponse(
        ClusterName clusterName,
        List<DataFusionStatsNodeResponse> nodes,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodes, failures);
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionStatsNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<DataFusionStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(DataFusionStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<DataFusionStatsNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        RestActions.buildNodesHeader(builder, params, this);
        builder.field("cluster_name", getClusterName().value());

        builder.startObject("nodes");
        for (DataFusionStatsNodeResponse nodeResponse : getNodes()) {
            builder.startObject(nodeResponse.getNode().getId());

            if (nodeResponse.getStats() != null) {
                nodeResponse.getStats().toXContent(builder, params);
            }

            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}
