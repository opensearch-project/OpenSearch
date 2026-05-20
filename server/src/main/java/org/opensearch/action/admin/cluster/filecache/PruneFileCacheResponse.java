/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.filecache;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Response for pruning remote file cache across multiple nodes.
 * Aggregates individual node responses and provides comprehensive operational visibility
 * including per-node metrics, cluster-wide summaries, and failure tracking.
 *
 * @opensearch.internal
 */
public class PruneFileCacheResponse extends BaseNodesResponse<NodePruneFileCacheResponse> implements ToXContentObject {

    /**
     * Constructor for stream input deserialization
     *
     * @param in the stream input
     * @throws IOException if an I/O exception occurs
     */
    public PruneFileCacheResponse(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Constructor for multi-node response aggregation
     *
     * @param clusterName the cluster name
     * @param nodes the successful node responses
     * @param failures the failed node responses
     */
    public PruneFileCacheResponse(ClusterName clusterName, List<NodePruneFileCacheResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodePruneFileCacheResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodePruneFileCacheResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodePruneFileCacheResponse> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field("acknowledged", true);

        long totalPrunedBytes = getTotalPrunedBytes();
        builder.field("total_pruned_bytes", totalPrunedBytes);

        long totalCacheCapacity = 0;

        builder.startObject("summary");
        builder.field("total_nodes_targeted", getNodes().size() + failures().size());
        builder.field("successful_nodes", getNodes().size());
        builder.field("failed_nodes", failures().size());
        for (NodePruneFileCacheResponse nodeResponse : getNodes()) {
            if (nodeResponse != null) {
                totalCacheCapacity += nodeResponse.getCacheCapacity();
            }
        }

        builder.field("total_cache_capacity", totalCacheCapacity);
        builder.endObject();

        builder.startObject("nodes");
        for (NodePruneFileCacheResponse nodeResponse : getNodes()) {
            if (nodeResponse != null && nodeResponse.getNode() != null) {
                builder.startObject(nodeResponse.getNode().getId());
                nodeResponse.toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();

        if (!failures().isEmpty()) {
            builder.startArray("failures");
            for (FailedNodeException failure : failures()) {
                builder.startObject();
                builder.field("node_id", failure.nodeId());
                builder.field("reason", failure.getDetailedMessage());
                builder.field("caused_by", failure.getCause() != null ? failure.getCause().getClass().getSimpleName() : null);
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    /**
     * Calculate total bytes pruned across all successful nodes
     *
     * @return total bytes freed by all successful prune operations
     */
    public long getTotalPrunedBytes() {
        return getNodes().stream().filter(Objects::nonNull).mapToLong(NodePruneFileCacheResponse::getPrunedBytes).sum();
    }

    /**
     * Check if the operation was partially successful (some nodes succeeded, some failed)
     *
     * @return true if some nodes succeeded and some failed
     */
    // VisibleForTesting
    public boolean isPartiallySuccessful() {
        return getNodes().isEmpty() == false && failures().isEmpty() == false;
    }

    /**
     * Check if the operation was completely successful (all targeted nodes succeeded)
     *
     * @return true if all targeted nodes succeeded
     */
    // VisibleForTesting
    public boolean isCompletelySuccessful() {
        return getNodes().isEmpty() == false && failures().isEmpty();
    }

    /**
     * @return whether the operation was acknowledged (always true for multi-node responses)
     */
    public boolean isAcknowledged() {
        return true;
    }

    /**
     * @return total bytes freed across all nodes
     */
    public long getPrunedBytes() {
        return getTotalPrunedBytes();
    }
}
