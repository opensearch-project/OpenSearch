/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.WorkloadManagementPlugin;
import org.opensearch.rule.CreateRuleRequest;
import org.opensearch.rule.CreateRuleResponse;
import org.opensearch.rule.RuleRoutingService;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Map;
import java.util.Optional;

/**
 * Service responsible for routing CreateRule requests to the correct node based on primary shard ownership.
 * @opensearch.experimental
 */
public class WorkloadGroupRuleRoutingService implements RuleRoutingService {
    private final Client client;
    private final ClusterService clusterService;
    private TransportService transportService;
    private static final Logger logger = LogManager.getLogger(WorkloadGroupRuleRoutingService.class);
    private static final Map<String, Object> indexSettings = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");

    /**
     * Constructor for WorkloadGroupRuleRoutingService
     * @param client
     * @param clusterService
     */
    public WorkloadGroupRuleRoutingService(Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Set {@link TransportService} for WorkloadGroupRuleRoutingService
     * @param transportService
     */
    public void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    @Override
    public void handleCreateRuleRequest(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        String indexName = WorkloadManagementPlugin.INDEX_NAME;

        try (ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext()) {
            if (clusterService.state().metadata().hasIndex(indexName)) {
                routeRequest(request, listener, indexName);
                return;
            }
            createIndex(indexName, new ActionListener<>() {
                @Override
                public void onResponse(CreateIndexResponse response) {
                    if (!response.isAcknowledged()) {
                        logger.error("Failed to create index " + indexName);
                        listener.onFailure(new IllegalStateException(indexName + " index creation not acknowledged"));
                    } else {
                        routeRequest(request, listener, indexName);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    Throwable cause = ExceptionsHelper.unwrapCause(e);
                    if (cause instanceof ResourceAlreadyExistsException) {
                        routeRequest(request, listener, indexName);
                    } else {
                        logger.error("Failed to create index {}: {}", indexName, e.getMessage());
                        listener.onFailure(e);
                    }
                }
            });
        }
    }

    /**
     * Creates the backing index if it does not exist, then runs the given success callback.
     * @param indexName the name of the index to create
     * @param listener listener to handle failures
     */
    private void createIndex(String indexName, ActionListener<CreateIndexResponse> listener) {
        final CreateIndexRequest createRequest = new CreateIndexRequest(indexName).settings(indexSettings);
        client.admin().indices().create(createRequest, listener);
    }

    /**
     * Routes the CreateRuleRequest to the primary shard node for the given index.
     * Executes locally if the current node is the primary.
     * @param request the CreateRuleRequest
     * @param listener listener to handle response or failure
     * @param indexName the index name used to find the primary shard node
     */
    private void routeRequest(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener, String indexName) {
        Optional<DiscoveryNode> optionalPrimaryNode = getPrimaryShardNode(indexName);
        if (optionalPrimaryNode.isEmpty()) {
            listener.onFailure(new IllegalStateException("Primary node for index [" + indexName + "] not found"));
            return;
        }
        DiscoveryNode primaryNode = optionalPrimaryNode.get();
        transportService.sendRequest(
            primaryNode,
            CreateRuleAction.NAME,
            request,
            new ActionListenerResponseHandler<>(listener, CreateRuleResponse::new)
        );
    }

    /**
     * Retrieves the discovery node that holds the primary shard for the given index.
     * @param indexName the index name
     */
    private Optional<DiscoveryNode> getPrimaryShardNode(String indexName) {
        ClusterState state = clusterService.state();
        return Optional.ofNullable(state.getRoutingTable().index(indexName))
            .map(table -> table.shard(0))
            .map(IndexShardRoutingTable::primaryShard)
            .filter(ShardRouting::assignedToNode)
            .map(shard -> state.nodes().get(shard.currentNodeId()));
    }
}
