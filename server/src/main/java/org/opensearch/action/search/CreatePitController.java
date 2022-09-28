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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.opensearch.common.unit.TimeValue.timeValueSeconds;

/**
 * Controller for creating PIT reader context
 * Phase 1 of create PIT request : Create PIT reader contexts in the associated shards with a temporary keep alive
 * Phase 2 of create PIT : Update PIT reader context with PIT ID and keep alive from request and
 * fail user request if any of the updates in this phase are failed - we clean up PITs in case of such failures.
 * This two phase approach is used to save PIT ID as part of context which is later used for other use cases like list PIT etc.
 */
public class CreatePitController {
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private static final Logger logger = LogManager.getLogger(CreatePitController.class);
    public static final Setting<TimeValue> PIT_INIT_KEEP_ALIVE = Setting.positiveTimeSetting(
        "point_in_time.init.keep_alive",
        timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    @Inject
    public CreatePitController(
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    /**
     * This method creates PIT reader context
     */
    public void executeCreatePit(
        CreatePitRequest request,
        Task task,
        StepListener<SearchResponse> createPitListener,
        ActionListener<CreatePitResponse> updatePitIdListener
    ) {
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        searchRequest.preference(request.getPreference());
        searchRequest.routing(request.getRouting());
        searchRequest.indicesOptions(request.getIndicesOptions());
        searchRequest.allowPartialSearchResults(request.shouldAllowPartialPitCreation());
        SearchTask searchTask = searchRequest.createTask(
            task.getId(),
            task.getType(),
            task.getAction(),
            task.getParentTaskId(),
            Collections.emptyMap()
        );
        /**
         * Phase 1 of create PIT
         */
        executeCreatePit(searchTask, searchRequest, createPitListener);

        /**
         * Phase 2 of create PIT where we update pit id in pit contexts
         */
        createPitListener.whenComplete(
            searchResponse -> { executeUpdatePitId(request, searchRequest, searchResponse, updatePitIdListener); },
            updatePitIdListener::onFailure
        );
    }

    /**
     * Creates PIT reader context with temporary keep alive
     */
    void executeCreatePit(Task task, SearchRequest searchRequest, StepListener<SearchResponse> createPitListener) {
        logger.debug(
            () -> new ParameterizedMessage("Executing creation of PIT context for indices [{}]", Arrays.toString(searchRequest.indices()))
        );
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
                        new TransportCreatePitAction.CreateReaderContextRequest(
                            target.getShardId(),
                            PIT_INIT_KEEP_ALIVE.get(clusterService.getSettings())
                        ),
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
        SearchRequest searchRequest,
        SearchResponse searchResponse,
        ActionListener<CreatePitResponse> updatePitIdListener
    ) {
        logger.debug(
            () -> new ParameterizedMessage(
                "Updating PIT context with PIT ID [{}], creation time and keep alive",
                searchResponse.pointInTimeId()
            )
        );
        /**
         * store the create time ( same create time for all PIT contexts across shards ) to be used
         * for list PIT api
         */
        final long relativeStartNanos = System.nanoTime();
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(
            searchRequest.getOrCreateAbsoluteStartMillis(),
            relativeStartNanos,
            System::nanoTime
        );
        final long creationTime = timeProvider.getAbsoluteStartMillis();
        CreatePitResponse createPITResponse = new CreatePitResponse(
            searchResponse.pointInTimeId(),
            creationTime,
            searchResponse.getTotalShards(),
            searchResponse.getSuccessfulShards(),
            searchResponse.getSkippedShards(),
            searchResponse.getFailedShards(),
            searchResponse.getShardFailures()
        );
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
                    final Transport.Connection connection = searchTransportService.getConnection(entry.getValue().getClusterAlias(), node);
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
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Create pit update phase failed for PIT ID [{}] on node [{}]",
                            searchResponse.pointInTimeId(),
                            node
                        ),
                        e
                    );
                    groupedActionListener.onFailure(
                        new OpenSearchException(
                            "Create pit update phase for PIT ID [" + searchResponse.pointInTimeId() + "] failed on node[" + node + "]",
                            e
                        )
                    );
                }
            }
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
        return (StepListener<BiFunction<String, String, DiscoveryNode>>) SearchUtils.getConnectionLookupListener(
            searchTransportService.getRemoteClusterService(),
            state,
            clusters
        );
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
                updatePitIdListener.onFailure(e);
            }
        }, size);
    }
}
