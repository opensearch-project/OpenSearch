/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

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
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Transport action for creating PIT reader context
 */
public class TransportCreatePITAction extends HandledTransportAction<CreatePITRequest, SearchResponse> {

    public static final String CREATE_PIT = "create_pit";
    private final TimeValue CREATE_PIT_TEMPORARY_KEEP_ALIVE = new TimeValue(30, TimeUnit.SECONDS);
    private final SearchService searchService;
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;

    @Inject
    public TransportCreatePITAction(
        SearchService searchService,
        TransportService transportService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(CreatePITAction.NAME, transportService, actionFilters, in -> new CreatePITRequest(in));
        this.searchService = searchService;
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    protected void doExecute(Task task, CreatePITRequest request, ActionListener<SearchResponse> listener) {
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        searchRequest.preference(request.getPreference());
        searchRequest.routing(request.getRouting());
        searchRequest.indicesOptions(request.getIndicesOptions());
        searchRequest.allowPartialSearchResults(request.isAllowPartialPitCreation());

        final StepListener<SearchResponse> createPitListener = new StepListener<>();

        final ActionListener<SearchResponse> updatePitIdListener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                listener.onResponse(searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };

        /**
         * Phase 1 of create PIT request : Create PIT reader contexts in the associated shards with a
         * temporary keep alive
         */
        transportSearchAction.executeRequest(task, searchRequest, CREATE_PIT, true, new TransportSearchAction.SinglePhaseSearchAction() {
            @Override
            public void executeOnShardTarget(
                SearchTask searchTask,
                SearchShardTarget target,
                Transport.Connection connection,
                ActionListener<SearchPhaseResult> searchPhaseResultActionListener
            ) {
                transportService.sendChildRequest(
                    connection,
                    SearchTransportService.CREATE_READER_CONTEXT_ACTION_NAME,
                    new CreateReaderContextRequest(target.getShardId(), CREATE_PIT_TEMPORARY_KEEP_ALIVE),
                    searchTask,
                    new TransportResponseHandler<CreateReaderContextResponse>() {
                        @Override
                        public CreateReaderContextResponse read(StreamInput in) throws IOException {
                            return new CreateReaderContextResponse(in);
                        }

                        @Override
                        public void handleResponse(CreateReaderContextResponse response) {
                            searchPhaseResultActionListener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            searchPhaseResultActionListener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.GENERIC;
                        }
                    }
                );
            }
        }, createPitListener);

        /**
         * Phase 2 of create PIT : Update PIT reader context with PIT ID and keep alive from request
         * Fail create pit operation if any of the updates in this phase are failed
         */
        createPitListener.whenComplete(searchResponse -> {
            SearchContextId contextId = SearchContextId.decode(namedWriteableRegistry, searchResponse.pointInTimeId());
            final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = getConnectionLookupListener(contextId);
            lookupListener.whenComplete(nodelookup -> {
                final ActionListener<TransportCreatePITAction.UpdatePitContextResponse> groupedActionListener = getGroupedListener(
                    updatePitIdListener,
                    searchResponse,
                    contextId.shards().size()
                );
                /**
                 * store the create time ( same create time for all PIT contexts across shards ) to be used
                 * for list PIT api
                 */
                TimeValue createTime = new TimeValue(System.currentTimeMillis());
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
                                searchResponse.pointInTimeId(),
                                request.getKeepAlive().millis(),
                                createTime
                            ),
                            groupedActionListener
                        );
                    } catch (Exception e) {
                        groupedActionListener.onFailure(e);
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
        ActionListener<SearchResponse> updatePitIdListener,
        SearchResponse searchResponse,
        int size
    ) {
        return new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(final Collection<UpdatePitContextResponse> responses) {
                updatePitIdListener.onResponse(searchResponse);
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

    public static class UpdatePITContextRequest extends TransportRequest {
        private final String pitId;
        private final long keepAlive;

        private final TimeValue createTime;
        private final ShardSearchContextId searchContextId;

        UpdatePITContextRequest(ShardSearchContextId searchContextId, String pitId, long keepAlive, TimeValue createTime) {
            this.pitId = pitId;
            this.searchContextId = searchContextId;
            this.keepAlive = keepAlive;
            this.createTime = createTime;
        }

        UpdatePITContextRequest(StreamInput in) throws IOException {
            super(in);
            pitId = in.readString();
            keepAlive = in.readLong();
            createTime = in.readTimeValue();
            searchContextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(pitId);
            out.writeLong(keepAlive);
            out.writeTimeValue(createTime);
            searchContextId.writeTo(out);
        }

        public ShardSearchContextId getSearchContextId() {
            return searchContextId;
        }

        public String getPitId() {
            return pitId;
        }

        public String id() {
            return this.getPitId();
        }

        public TimeValue getCreateTime() {
            return createTime;
        }
    }

    public static class UpdatePitContextResponse extends TransportResponse {
        private final String pitId;

        UpdatePitContextResponse(StreamInput in) throws IOException {
            super(in);
            pitId = in.readString();
        }

        public UpdatePitContextResponse(String pitId) {
            this.pitId = pitId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(pitId);
        }

        public String getPitId() {
            return pitId;
        }
    }
}
