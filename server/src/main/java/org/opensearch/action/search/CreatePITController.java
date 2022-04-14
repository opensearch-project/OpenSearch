/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Controller for creating PIT reader context
 * Phase 1 of create PIT request : Create PIT reader contexts in the associated shards with a temporary keep alive
 * Phase 2 of create PIT : Update PIT reader context with PIT ID and keep alive from request and
 * fail user request if any of the updates in this phase are failed
 */
public class CreatePITController implements Runnable {
    private final Runnable runner;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Task task;
    private final ActionListener<CreatePITResponse> listener;
    private final CreatePITRequest request;
    private static final Logger logger = LogManager.getLogger(CreatePITController.class);

    public CreatePITController(
        CreatePITRequest request,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry,
        Task task,
        ActionListener<CreatePITResponse> listener
    ) {
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.task = task;
        this.listener = listener;
        this.request = request;
        runner = this::executeCreatePit;
    }

    private TimeValue getCreatePitTemporaryKeepAlive() {
        return SearchService.CREATE_PIT_TEMPORARY_KEEPALIVE_SETTING.get(clusterService.getSettings());
    }

    public void executeCreatePit() {
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        searchRequest.preference(request.getPreference());
        searchRequest.routing(request.getRouting());
        searchRequest.indicesOptions(request.getIndicesOptions());
        searchRequest.allowPartialSearchResults(request.shouldAllowPartialPitCreation());

        SearchTask searchTask = new SearchTask(
            task.getId(),
            task.getType(),
            task.getAction(),
            () -> task.getDescription(),
            task.getParentTaskId(),
            task.getHeaders()
        );

        final StepListener<CreatePITResponse> createPitListener = new StepListener<>();

        final ActionListener<CreatePITResponse> updatePitIdListener = ActionListener.wrap(r -> listener.onResponse(r), e -> {
            logger.error("PIT creation failed while updating PIT ID", e);
            listener.onFailure(e);
        });
        /**
         * Phase 1 of create PIT
         */
        executeCreatePit(searchTask, searchRequest, createPitListener);

        /**
         * Phase 2 of create PIT where we update pit id in pit contexts
         */
        executeUpdatePitId(request, createPitListener, updatePitIdListener);
    }

    /**
     * Creates PIT reader context with temporary keep alive
     */
    public void executeCreatePit(Task task, SearchRequest searchRequest, StepListener<CreatePITResponse> createPitListener) {
        transportSearchAction.executeRequest(
            task,
            searchRequest,
            TransportCreatePITAction.CREATE_PIT_ACTION,
            true,
            new TransportSearchAction.SinglePhaseSearchAction() {
                @Override
                public void executeOnShardTarget(
                    SearchTask searchTask,
                    SearchShardTarget target,
                    Transport.Connection connection,
                    ActionListener<SearchPhaseResult> searchPhaseResultActionListener
                ) {
                    searchTransportService.createPitContext(
                        connection,
                        new TransportCreatePITAction.CreateReaderContextRequest(target.getShardId(), getCreatePitTemporaryKeepAlive()),
                        searchTask,
                        ActionListener.wrap(r -> searchPhaseResultActionListener.onResponse(r), searchPhaseResultActionListener::onFailure)
                    );
                }
            },
            createPitListener
        );
    }

    /**
     * Updates PIT ID, keep alive and createdTime of PIT reader context
     */
    public void executeUpdatePitId(
        CreatePITRequest request,
        StepListener<CreatePITResponse> createPitListener,
        ActionListener<CreatePITResponse> updatePitIdListener
    ) {
        createPitListener.whenComplete(createPITResponse -> {
            SearchContextId contextId = SearchContextId.decode(namedWriteableRegistry, createPITResponse.getId());
            final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = getConnectionLookupListener(contextId);
            lookupListener.whenComplete(nodelookup -> {
                final ActionListener<UpdatePitContextResponse> groupedActionListener = getGroupedListener(
                    updatePitIdListener,
                    createPITResponse,
                    contextId.shards().size(),
                    contextId.shards().values()
                );
                /**
                 * store the create time ( same create time for all PIT contexts across shards ) to be used
                 * for list PIT api
                 */
                long createTime = System.currentTimeMillis();
                for (Map.Entry<ShardId, SearchContextIdForNode> entry : contextId.shards().entrySet()) {
                    DiscoveryNode node = nodelookup.apply(entry.getValue().getClusterAlias(), entry.getValue().getNode());
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(
                            entry.getValue().getClusterAlias(),
                            node
                        );
                        searchTransportService.updatePitContext(
                            connection,
                            new UpdatePITContextRequest(
                                entry.getValue().getSearchContextId(),
                                createPITResponse.getId(),
                                request.getKeepAlive().millis(),
                                createTime
                            ),
                            groupedActionListener
                        );
                    } catch (Exception e) {
                        logger.error(() -> new ParameterizedMessage("Create pit update phase failed on node [{}]", node), e);
                        groupedActionListener.onFailure(new OpenSearchException("Create pit failed on node[" + node + "]", e));
                    }
                }
            }, updatePitIdListener::onFailure);
        }, updatePitIdListener::onFailure);
    }

    private StepListener<BiFunction<String, String, DiscoveryNode>> getConnectionLookupListener(SearchContextId contextId) {
        ClusterState state = clusterService.state();

        final Set<String> clusters = contextId.shards()
            .values()
            .stream()
            .filter(ctx -> Strings.isEmpty(ctx.getClusterAlias()) == false)
            .map(SearchContextIdForNode::getClusterAlias)
            .collect(Collectors.toSet());

        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();

        if (clusters.isEmpty() == false) {
            searchTransportService.getRemoteClusterService().collectNodes(clusters, lookupListener);
        } else {
            lookupListener.onResponse((cluster, nodeId) -> state.getNodes().get(nodeId));
        }
        return lookupListener;
    }

    private ActionListener<UpdatePitContextResponse> getGroupedListener(
        ActionListener<CreatePITResponse> updatePitIdListener,
        CreatePITResponse createPITResponse,
        int size,
        Collection<SearchContextIdForNode> contexts
    ) {
        return new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(final Collection<UpdatePitContextResponse> responses) {
                updatePitIdListener.onResponse(createPITResponse);
            }

            @Override
            public void onFailure(final Exception e) {
                cleanupContexts(contexts);
                updatePitIdListener.onFailure(e);
            }
        }, size);
    }

    /**
     * Cleanup all created PIT contexts in case of failure
     */
    private void cleanupContexts(Collection<SearchContextIdForNode> contexts) {
        ActionListener<Integer> deleteListener = new ActionListener<>() {
            @Override
            public void onResponse(Integer freed) {
                // log the number of freed contexts - this is invoke and forget call
                logger.debug(() -> new ParameterizedMessage("Cleaned up {} contexts out of {}", freed, contexts.size()));
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Cleaning up PIT contexts failed ", e);
            }
        };
        ClearScrollController.closeContexts(clusterService.state().getNodes(), searchTransportService, contexts, deleteListener);
    }

    @Override
    public void run() {
        runner.run();
    }
}
