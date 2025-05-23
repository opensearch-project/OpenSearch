/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.rule.CreateRuleRequest;
import org.opensearch.rule.CreateRuleResponse;
import org.opensearch.rule.RulePersistenceService;
import org.opensearch.rule.RulePersistenceServiceRegistry;
import org.opensearch.rule.service.IndexStoredRulePersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.rule.RuleFrameworkPlugin.RULE_THREAD_POOL_NAME;

/**
 * Transport action to create Rules
 * @opensearch.experimental
 */
public class TransportCreateRuleAction extends TransportAction<CreateRuleRequest, CreateRuleResponse> {
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final RulePersistenceServiceRegistry rulePersistenceServiceRegistry;
    private final Client client;
    private static final Logger logger = LogManager.getLogger(TransportCreateRuleAction.class);
    private static final Map<String, Object> indexSettings = Map.of("index.number_of_shards", 1, "index.auto_expand_replicas", "0-all");

    /**
     * Constructor for TransportCreateRuleAction
     *
     * @param threadPool - {@link ThreadPool} object
     * @param client - {@link Client} object
     * @param transportService - a {@link TransportService} object
     * @param clusterService - a {@link ClusterService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param rulePersistenceServiceRegistry - a {@link RulePersistenceServiceRegistry} object
     */
    @Inject
    public TransportCreateRuleAction(
        Client client,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        RulePersistenceServiceRegistry rulePersistenceServiceRegistry
    ) {
        super(CreateRuleAction.NAME, actionFilters, transportService.getTaskManager());
        this.client = client;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.rulePersistenceServiceRegistry = rulePersistenceServiceRegistry;

        transportService.registerRequestHandler(
            CreateRuleAction.NAME,
            ThreadPool.Names.WRITE,
            CreateRuleRequest::new,
            new TransportRequestHandler<CreateRuleRequest>() {
                @Override
                public void messageReceived(CreateRuleRequest request, TransportChannel channel, Task task) {
                    executeLocally(request, ActionListener.wrap(response -> {
                        try {
                            channel.sendResponse(response);
                        } catch (IOException e) {
                            logger.error("Failed to send CreateRuleResponse to transport channel", e);
                            throw new TransportException("Fail to send", e);
                        }
                    }, exception -> {
                        try {
                            channel.sendResponse(exception);
                        } catch (IOException e) {
                            logger.error("Failed to send exception response to transport channel", e);
                            throw new TransportException("Fail to send", e);
                        }
                    }));
                }
            }
        );
    }

    @Override
    protected void doExecute(Task task, CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        RulePersistenceService persistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
            request.getRule().getFeatureType()
        );
        if (!(persistenceService instanceof IndexStoredRulePersistenceService indexStoredRulePersistenceService)) {
            executeLocally(request, listener);
            return;
        }

        String indexName = indexStoredRulePersistenceService.getIndexName();
        try (ThreadContext.StoredContext ctx = client.threadPool().getThreadContext().stashContext()) {
            if (!clusterService.state().metadata().hasIndex(indexName)) {
                createIndex(() -> routeRequest(request, listener, indexName), indexName, listener);
                return;
            }
        }

        routeRequest(request, listener, indexName);
    }

    /**
     * Creates the backing index if it does not exist, then runs the given success callback.
     * @param onSuccess callback to run after successful index creation
     * @param indexName the name of the index to create
     * @param listener listener to handle failures
     */
    private void createIndex(Runnable onSuccess, String indexName, ActionListener<CreateRuleResponse> listener) {
        final CreateIndexRequest request = new CreateIndexRequest(indexName).settings(indexSettings);
        client.admin().indices().create(request, new ActionListener<>() {
            @Override
            public void onResponse(CreateIndexResponse response) {
                if (!response.isAcknowledged()) {
                    listener.onFailure(new IllegalStateException(indexName + " index creation not acknowledged"));
                } else {
                    onSuccess.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof ResourceAlreadyExistsException) {
                    onSuccess.run();
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    /**
     * Routes the CreateRuleRequest to the primary shard node for the given index.
     * Executes locally if the current node is the primary.
     * @param request the CreateRuleRequest
     * @param listener listener to handle response or failure
     * @param indexName the index name used to find the primary shard node
     */
    private void routeRequest(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener, String indexName) {
        DiscoveryNode primaryNode = getPrimaryShardNode(indexName);
        if (primaryNode == null) {
            listener.onFailure(new IllegalStateException("Primary node for index [" + indexName + "] not found"));
            return;
        }

        if (transportService.getLocalNode().equals(primaryNode)) {
            executeLocally(request, listener);
        } else {
            transportService.sendRequest(
                primaryNode,
                CreateRuleAction.NAME,
                request,
                new ActionListenerResponseHandler<>(listener, CreateRuleResponse::new)
            );
        }
    }

    /**
     * Retrieves the discovery node that holds the primary shard for the given index.
     * @param indexName the index name
     */
    private DiscoveryNode getPrimaryShardNode(String indexName) {
        ClusterState state = clusterService.state();
        IndexRoutingTable indexRoutingTable = state.getRoutingTable().index(indexName);
        if (indexRoutingTable == null) {
            return null;
        }
        ShardRouting primaryShard = indexRoutingTable.shard(0).primaryShard();
        if (primaryShard == null || !primaryShard.assignedToNode()) {
            return null;
        }
        return state.nodes().get(primaryShard.currentNodeId());
    }

    /**
     * Executes the create rule operation locally on the dedicated rule thread pool.
     * @param request the CreateRuleRequest
     * @param listener listener to handle response or failure
     */
    private void executeLocally(CreateRuleRequest request, ActionListener<CreateRuleResponse> listener) {
        threadPool.executor(RULE_THREAD_POOL_NAME).execute(() -> {
            RulePersistenceService persistenceService = rulePersistenceServiceRegistry.getRulePersistenceService(
                request.getRule().getFeatureType()
            );
            persistenceService.createRule(request, listener);
        });
    }
}
