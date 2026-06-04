/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manages shard synchronization for scale operations
 */
/**
 * Manages shard synchronization operations during search-only scaling.
 * <p>
 * This manager coordinates the necessary synchronization across nodes to ensure all shards
 * are in a consistent state before finalizing scale operations. It handles:
 * <ul>
 *   <li>Dispatching sync requests to nodes hosting primary shards</li>
 *   <li>Aggregating responses from multiple nodes</li>
 *   <li>Validating that shards are ready for scale operations</li>
 *   <li>Tracking primary shard assignments across the cluster</li>
 * </ul>
 * <p>
 * The synchronization process is a critical safety mechanism that prevents data loss
 * during transitions between normal and search-only modes.
 */
class ScaleIndexShardSyncManager {

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final String transportActionName;

    /**
     * Constructs a new ShardSyncManager.
     *
     * @param clusterService     the cluster service for accessing cluster state
     * @param transportService   the transport service for sending requests to other nodes
     * @param transportActionName the transport action name for shard sync requests
     */
    ScaleIndexShardSyncManager(ClusterService clusterService, TransportService transportService, String transportActionName) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.transportActionName = transportActionName;
    }

    /**
     * Sends shard sync requests to each node that holds a primary shard.
     * <p>
     * This method:
     * <ul>
     *   <li>Groups shards by node to minimize network requests</li>
     *   <li>Creates a grouped listener to aggregate responses</li>
     *   <li>Dispatches sync requests to each relevant node</li>
     * </ul>
     * <p>
     * The listener is notified once all nodes have responded or if any errors occur.
     *
     * @param index             the name of the index being scaled
     * @param primaryShardsNodes map of shard IDs to node IDs for all primary shards
     * @param listener          the listener to notify when all nodes have responded
     */
    void sendShardSyncRequests(
        String index,
        Map<ShardId, String> primaryShardsNodes,
        ActionListener<Collection<ScaleIndexNodeResponse>> listener
    ) {
        if (primaryShardsNodes.isEmpty()) {
            listener.onFailure(new IllegalStateException("No primary shards found for index " + index));
            return;
        }

        Map<String, List<ShardId>> nodeShardGroups = primaryShardsNodes.entrySet()
            .stream()
            .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        final GroupedActionListener<ScaleIndexNodeResponse> groupedListener = new GroupedActionListener<>(listener, nodeShardGroups.size());

        for (Map.Entry<String, List<ShardId>> entry : nodeShardGroups.entrySet()) {
            final String nodeId = entry.getKey();
            final List<ShardId> shards = entry.getValue();
            final DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);

            if (targetNode == null) {
                groupedListener.onFailure(new IllegalStateException("Node [" + nodeId + "] not found"));
                continue;
            }

            sendNodeRequest(targetNode, index, shards, groupedListener);
        }
    }

    /**
     * Sends a sync request to a specific node for the given shards.
     * <p>
     * This method creates and sends a transport request to perform shard synchronization
     * on a target node, registering appropriate response and error handlers.
     *
     * @param targetNode the node to send the request to
     * @param index      the name of the index being scaled
     * @param shards     the list of shards to synchronize on the target node
     * @param listener   the listener to notify with the response
     */
    void sendNodeRequest(DiscoveryNode targetNode, String index, List<ShardId> shards, ActionListener<ScaleIndexNodeResponse> listener) {
        transportService.sendRequest(
            targetNode,
            transportActionName,
            new ScaleIndexNodeRequest(index, shards),
            new TransportResponseHandler<ScaleIndexNodeResponse>() {
                @Override
                public ScaleIndexNodeResponse read(StreamInput in) throws IOException {
                    return new ScaleIndexNodeResponse(in);
                }

                @Override
                public void handleResponse(ScaleIndexNodeResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    /**
     * Aggregates node responses and verifies that no shard reports uncommitted operations or a pending sync.
     * <p>
     * This validation ensures that all shards are in a consistent state before proceeding with
     * a scale operation. If any shard reports conditions that would make scaling unsafe, the
     * operation is failed with detailed information about which shards need more time.
     *
     * @param responses the collection of responses from all nodes
     * @param listener  the listener to notify with the aggregated result
     */
    void validateNodeResponses(Collection<ScaleIndexNodeResponse> responses, ActionListener<ScaleIndexResponse> listener) {
        boolean hasUncommittedOps = false;
        boolean needsSync = false;
        List<String> failedShards = new ArrayList<>();

        for (ScaleIndexNodeResponse nodeResponse : responses) {
            for (ScaleIndexShardResponse shardResponse : nodeResponse.getShardResponses()) {
                if (shardResponse.hasUncommittedOperations()) {
                    hasUncommittedOps = true;
                    failedShards.add(shardResponse.getShardId().toString());
                }
                if (shardResponse.needsSync()) {
                    needsSync = true;
                    failedShards.add(shardResponse.getShardId().toString());
                }
            }
        }

        if (hasUncommittedOps || needsSync) {
            String errorDetails = "Pre-scale sync failed for shards: "
                + String.join(", ", failedShards)
                + (hasUncommittedOps ? " - uncommitted operations remain" : "")
                + (needsSync ? " - sync needed" : "");
            listener.onFailure(new IllegalStateException(errorDetails));
        } else {
            listener.onResponse(new ScaleIndexResponse(responses));
        }
    }

    /**
     * Returns the primary shard node assignments for a given index.
     * <p>
     * Builds a mapping between shard IDs and the node IDs hosting their primary copies.
     * This mapping is used to determine which nodes need to be contacted for shard
     * synchronization during scale operations.
     *
     * @param indexMetadata the metadata of the index
     * @param state         the current cluster state
     * @return a map of shard IDs to node IDs for all assigned primary shards
     */
    Map<ShardId, String> getPrimaryShardAssignments(IndexMetadata indexMetadata, ClusterState state) {
        Map<ShardId, String> assignments = new HashMap<>();
        for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
            ShardId shardId = new ShardId(indexMetadata.getIndex(), i);
            ShardRouting primaryShard = state.routingTable().index(indexMetadata.getIndex().getName()).shard(i).primaryShard();
            if (primaryShard != null && primaryShard.assignedToNode()) {
                assignments.put(shardId, primaryShard.currentNodeId());
            }
        }
        return assignments;
    }
}
