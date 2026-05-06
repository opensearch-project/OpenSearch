/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Cluster-wide response for {@code GET /_nodes/foyer_cache/stats}.
 *
 * <p>Aggregates {@link FoyerCacheNodeResponse} instances from all targeted
 * nodes. Nodes that do not have an active Foyer cache return a response with
 * {@code stats == null} and are omitted from the REST output.
 *
 * @opensearch.internal
 */
public class FoyerCacheStatsResponse extends BaseNodesResponse<FoyerCacheNodeResponse>
    implements
        ToXContentObject {

    public FoyerCacheStatsResponse(ClusterName clusterName, List<FoyerCacheNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public FoyerCacheStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<FoyerCacheNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(FoyerCacheNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<FoyerCacheNodeResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    /**
     * Renders the response as:
     * <pre>{@code
     * {
     *   "_nodes": { "total": N, "successful": S, "failed": F },
     *   "cluster_name": "my-cluster",
     *   "nodes": {
     *     "<nodeId>": {
     *       "name": "...",
     *       "foyer_block_cache": {
     *         "overall":     { ... },
     *         "block_level": { ... }
     *       }
     *     },
     *     ...
     *   }
     * }
     * }</pre>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // _nodes summary
        builder.startObject("_nodes");
        builder.field("total",      getNodes().size() + failures().size());
        builder.field("successful", getNodes().size());
        builder.field("failed",     failures().size());
        builder.endObject();
        builder.field("cluster_name", getClusterName().value());
        // per-node entries
        builder.startObject("nodes");
        for (FoyerCacheNodeResponse node : getNodes()) {
            if (node.getStats() == null) {
                continue; // node has no Foyer cache — skip silently
            }
            builder.startObject(node.getNode().getId());
            builder.field("name", node.getNode().getName());
            node.toXContent(builder, params);  // renders foyer_block_cache {...}
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }
}
