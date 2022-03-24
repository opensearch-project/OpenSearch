/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;

public class TransportCreatePITAction extends HandledTransportAction<PITRequest, PITResponse> {

    public static final String CREATE_PIT = "create_pit";
    private SearchService searchService;
    private final TransportService transportService;
    private TransportSearchAction transportSearchAction;

    @Inject
    public TransportCreatePITAction(SearchService searchService,
                                    TransportService transportService,
                                    ActionFilters actionFilters,
                                    TransportSearchAction transportSearchAction) {
        super(CreatePITAction.NAME, transportService, actionFilters, in -> new PITRequest(in));
        this.searchService = searchService;
        this.transportService = transportService;
        this.transportSearchAction = transportSearchAction;
    }


    @Override
    protected void doExecute(Task task, PITRequest request, ActionListener<PITResponse> listener) {
        SearchRequest sr = new SearchRequest(request.getIndices());
        sr.preference(request.getPreference());
        sr.routing(request.getRouting());
        sr.indicesOptions(request.getIndicesOptions());
        transportSearchAction.executeRequest(task, sr, CREATE_PIT, true,
            (searchTask, target, connection, searchPhaseResultActionListener) ->
                /*TODO set a timeout based on "awaitActive"*/
                transportService.sendChildRequest(connection, SearchTransportService.CREATE_READER_CONTEXT_ACTION_NAME,

                    new CreateReaderContextRequest(target.getShardId(), request.getKeepAlive()), searchTask,

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

                    }

                    @Override
                    public String executor() {
                        return "generic"; //TODO
                    }
                }),

            new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    listener.onResponse(new PITResponse(searchResponse.pointInTimeId()));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

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
