/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Transport action for creating PIT reader context
 * Phase 1 of create PIT request : Create PIT reader contexts in the associated shards with a temporary keep alive
 * Phase 2 of create PIT : Update PIT reader context with PIT ID and keep alive from request and
 * fail user request if any of the updates in this phase are failed
 */
public class TransportCreatePITAction extends HandledTransportAction<CreatePITRequest, CreatePITResponse> {

    public static final String CREATE_PIT_ACTION = "create_pit";
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportCreatePITAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(CreatePITAction.NAME, transportService, actionFilters, in -> new CreatePITRequest(in));
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    public TimeValue getCreatePitTemporaryKeepAlive() {
        return SearchService.CREATE_PIT_TEMPORARY_KEEPALIVE_SETTING.get(clusterService.getSettings());
    }

    @Override
    protected void doExecute(Task task, CreatePITRequest request, ActionListener<CreatePITResponse> listener) {
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

        final ActionListener<CreatePITResponse> updatePitIdListener = ActionListener.wrap(r -> listener.onResponse(r), listener::onFailure);
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
            CREATE_PIT_ACTION,
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
                        new CreateReaderContextRequest(target.getShardId(), getCreatePitTemporaryKeepAlive()),
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
                    contextId.shards().size()
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
        int size
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

    public static class CreateReaderContextRequest extends TransportRequest {
        private final ShardId shardId;
        private final TimeValue keepAlive;

        public CreateReaderContextRequest(ShardId shardId, TimeValue keepAlive) {
            this.shardId = shardId;
            this.keepAlive = keepAlive;
        }

        public ShardId getShardId() {
            return shardId;
        }

        public TimeValue getKeepAlive() {
            return keepAlive;
        }

        public CreateReaderContextRequest(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.keepAlive = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeTimeValue(keepAlive);
        }
    }

    public static class CreateReaderContextResponse extends SearchPhaseResult {
        public CreateReaderContextResponse(ShardSearchContextId shardSearchContextId) {
            this.contextId = shardSearchContextId;
        }

        public CreateReaderContextResponse(StreamInput in) throws IOException {
            super(in);
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            contextId.writeTo(out);
        }
    }

}
