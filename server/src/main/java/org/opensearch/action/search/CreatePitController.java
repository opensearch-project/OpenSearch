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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.opensearch.common.unit.TimeValue.timeValueSeconds;

/**
 * Controller for creating PIT reader context
 * Phase 1 of create PIT request : Create PIT reader contexts in the associated shards with a temporary keep alive
 * Phase 2 of create PIT : Update PIT reader context with PIT ID and keep alive from request and
 * fail user request if any of the updates in this phase are failed - we clean up PITs in case of such failures
 */
public class CreatePitController implements Runnable {
    private final Runnable runner;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Task task;
    private final ActionListener<CreatePitResponse> listener;
    private final CreatePitRequest request;
    private static final Logger logger = LogManager.getLogger(CreatePitController.class);
    public static final Setting<TimeValue> CREATE_PIT_TEMPORARY_KEEPALIVE_SETTING = Setting.positiveTimeSetting(
        "pit.temporary.keep_alive_interval",
        timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    public CreatePitController(
        CreatePitRequest request,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry,
        Task task,
        ActionListener<CreatePitResponse> listener
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
        return CREATE_PIT_TEMPORARY_KEEPALIVE_SETTING.get(clusterService.getSettings());
    }

    /**
     * Method for creating PIT reader context
     * Phase 1 of create PIT request : Create PIT reader contexts in the associated shards with a temporary keep alive
     * Phase 2 of create PIT : Update PIT reader context with PIT ID and keep alive from request and
     * fail user request if any of the updates in this phase are failed - we clean up PITs in case of such failures
     */
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
            new HashMap<>()
        );

        final StepListener<SearchResponse> createPitListener = new StepListener<>();

        final ActionListener<CreatePitResponse> updatePitIdListener = ActionListener.wrap(r -> listener.onResponse(r), e -> {
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
    void executeCreatePit(Task task, SearchRequest searchRequest, StepListener<SearchResponse> createPitListener) {
        logger.debug("Creating PIT context");
        transportSearchAction.executeRequest(
            task,
            searchRequest,
            TransportCreatePitAction.CREATE_PIT_ACTION,
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
                        new TransportCreatePitAction.CreateReaderContextRequest(target.getShardId(), getCreatePitTemporaryKeepAlive()),
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
    void executeUpdatePitId(
        CreatePitRequest request,
        StepListener<SearchResponse> createPitListener,
        ActionListener<CreatePitResponse> updatePitIdListener
    ) {
        createPitListener.whenComplete(searchResponse -> {
            logger.debug("Updating PIT context with PIT ID, creation time and keep alive");
            /**
             * store the create time ( same create time for all PIT contexts across shards ) to be used
             * for list PIT api
             */
            final long creationTime = System.currentTimeMillis();
            CreatePitResponse createPITResponse = new CreatePitResponse(searchResponse, creationTime);
            SearchContextId contextId = SearchContextId.decode(namedWriteableRegistry, createPITResponse.getId());
            final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = getConnectionLookupListener(contextId);
            lookupListener.whenComplete(nodelookup -> {
                final ActionListener<UpdatePitContextResponse> groupedActionListener = getGroupedListener(
                    updatePitIdListener,
                    createPITResponse,
                    contextId.shards().size(),
                    contextId.shards().values()
                );
                for (Map.Entry<ShardId, SearchContextIdForNode> entry : contextId.shards().entrySet()) {
                    DiscoveryNode node = nodelookup.apply(entry.getValue().getClusterAlias(), entry.getValue().getNode());
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(
                            entry.getValue().getClusterAlias(),
                            node
                        );
                        searchTransportService.updatePitContext(
                            connection,
                            new UpdatePitContextRequest(
                                entry.getValue().getSearchContextId(),
                                createPITResponse.getId(),
                                request.getKeepAlive().millis(),
                                creationTime
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

        if (clusters.isEmpty()) {
            lookupListener.onResponse((cluster, nodeId) -> state.getNodes().get(nodeId));
        } else {
            searchTransportService.getRemoteClusterService().collectNodes(clusters, lookupListener);
        }
        return lookupListener;
    }

    private ActionListener<UpdatePitContextResponse> getGroupedListener(
        ActionListener<CreatePitResponse> updatePitIdListener,
        CreatePitResponse createPITResponse,
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
                logger.error("Cleaning up PIT contexts failed ", e);
            }
        };
        ClearScrollController.closeContexts(clusterService.state().getNodes(), searchTransportService, contexts, deleteListener);
    }

    @Override
    public void run() {
        runner.run();
    }
}
