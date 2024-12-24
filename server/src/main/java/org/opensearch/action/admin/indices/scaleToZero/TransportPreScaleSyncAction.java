/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scaleToZero;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
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

public class TransportPreScaleSyncAction extends HandledTransportAction<PreScaleSyncRequest, PreScaleSyncResponse> {
    private static final Logger logger = LogManager.getLogger(TransportPreScaleSyncAction.class);
    public static final String SHARD_SYNC_ACTION_NAME = "indices:admin/settings/sync_shard";

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndicesService indicesService;

    @Inject
    public TransportPreScaleSyncAction(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(PreScaleSyncAction.NAME, transportService, actionFilters, PreScaleSyncRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indicesService = indicesService;

        // Register internal action handler for shard-level sync operations
        transportService.registerRequestHandler(
            SHARD_SYNC_ACTION_NAME,
            ThreadPool.Names.SAME,
            NodePreScaleSyncRequest::new,
            (request, channel, task) -> handleShardSyncRequest(request, channel)
        );
    }

    private void handleShardSyncRequest(NodePreScaleSyncRequest request, TransportChannel channel) throws Exception {
        logger.info("The handleShardSyncRequest is called");
        final ClusterState state = clusterService.state();
        final IndexMetadata indexMetadata = state.metadata().index(request.getIndex());
        if (indexMetadata == null) {
            throw new IllegalStateException("Index " + request.getIndex() + " not found");
        }

        IndexService indexService = indicesService.indexService(indexMetadata.getIndex());
        if (indexService == null) {
            throw new IllegalStateException("IndexService not found for index " + request.getIndex());
        }

        List<ShardPreScaleSyncResponse> shardResponses = new ArrayList<>();
        for (ShardId shardId : request.getShardIds()) {
            IndexShard shard = indexService.getShardOrNull(shardId.id());
            if (shard == null) {
                continue;
            }

            // CHecking the
            if (shard.translogStats().getUncommittedOperations() > 0) {
                logger.info(
                    "Translog has {} uncommitted operations before closing shard [{}]",
                    shard.translogStats().getUncommittedOperations(),
                    shard.shardId()
                );
                shard.sync();
            }

            // Force flush
            logger.info("Doing final flush before closing shard");
            shard.flush(new FlushRequest().force(true).waitIfOngoing(true));

            // Force sync if needed
            if (shard.translogStats().getUncommittedOperations() > 0) {
                logger.info(
                    "Translog has {} uncommitted operations before closing shard [{}]",
                    shard.translogStats().getUncommittedOperations(),
                    shard.shardId()
                );
            } else {
                logger.info("Translog is empty");
            }
            shard.sync();

            shard.waitForRemoteStoreSync();

            shardResponses.add(
                new ShardPreScaleSyncResponse(shardId, shard.isSyncNeeded(), shard.translogStats().getUncommittedOperations())
            );
        }

        channel.sendResponse(new NodePreScaleSyncResponse(clusterService.localNode(), shardResponses));
    }

    @Override
    protected void doExecute(Task task, PreScaleSyncRequest request, ActionListener<PreScaleSyncResponse> listener) {
        final ClusterState state = clusterService.state();
        final String indexName = request.getIndex();
        final IndexMetadata indexMetadata = state.metadata().index(indexName);

        if (indexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(indexName));
            return;
        }

        // Get all primary shards and their node assignments
        final Map<ShardId, String> primaryShardsNodes = getPrimaryShardNodeAssignments(indexMetadata, state);

        if (primaryShardsNodes.isEmpty()) {
            listener.onFailure(new IllegalStateException("No primary shards found for index " + indexName));
            return;
        }

        // Group shards by node
        final Map<String, List<ShardId>> nodeShardGroups = primaryShardsNodes.entrySet()
            .stream()
            .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

        final GroupedActionListener<NodePreScaleSyncResponse> groupedListener = new GroupedActionListener<>(
            ActionListener.wrap(responses -> handleNodeResponses(responses, listener), listener::onFailure),
            nodeShardGroups.size()
        );

        // Send sync requests to each node
        for (Map.Entry<String, List<ShardId>> nodeShards : nodeShardGroups.entrySet()) {
            final String nodeId = nodeShards.getKey();
            logger.info("nodeId is {}", nodeShards.getKey());
            final List<ShardId> shards = nodeShards.getValue();
            logger.info("shards is {}", nodeShards.getValue());

            final DiscoveryNode targetNode = state.nodes().get(nodeId);
            if (targetNode == null) {
                groupedListener.onFailure(new IllegalStateException("Node [" + nodeId + "] not found"));
                continue;
            }

            transportService.sendRequest(
                targetNode,  // Send to DiscoveryNode instead of String nodeId
                SHARD_SYNC_ACTION_NAME,
                new NodePreScaleSyncRequest(indexName, shards),
                new TransportResponseHandler<NodePreScaleSyncResponse>() {
                    @Override
                    public void handleResponse(NodePreScaleSyncResponse response) {
                        groupedListener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        groupedListener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public NodePreScaleSyncResponse read(StreamInput in) throws IOException {
                        return new NodePreScaleSyncResponse(in);
                    }
                }
            );
        }
    }

    private Map<ShardId, String> getPrimaryShardNodeAssignments(IndexMetadata indexMetadata, ClusterState state) {
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

    private void handleNodeResponses(Collection<NodePreScaleSyncResponse> responses, ActionListener<PreScaleSyncResponse> listener) {
        boolean hasUncommittedOps = false;
        boolean needsSync = false;
        List<String> failedShards = new ArrayList<>();

        for (NodePreScaleSyncResponse nodeResponse : responses) {
            for (ShardPreScaleSyncResponse shardResponse : nodeResponse.getShardResponses()) {
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
            listener.onFailure(
                new IllegalStateException(
                    "Pre-scale sync failed for shards: "
                        + String.join(", ", failedShards)
                        + (hasUncommittedOps ? " - uncommitted operations remain" : "")
                        + (needsSync ? " - sync needed" : "")
                )
            );
            return;
        }

        listener.onResponse(new PreScaleSyncResponse(responses));
    }
}
