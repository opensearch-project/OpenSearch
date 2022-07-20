/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

/**
 * Transport action for creating PIT reader context
 */
public class TransportCreatePitAction extends HandledTransportAction<CreatePitRequest, CreatePitResponse> {

    public static final String CREATE_PIT_ACTION = "create_pit";
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final CreatePitController createPitController;

    @Inject
    public TransportCreatePitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry,
        CreatePitController createPitController
    ) {
        super(CreatePitAction.NAME, transportService, actionFilters, in -> new CreatePitRequest(in));
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.createPitController = createPitController;
    }

    @Override
    protected void doExecute(Task task, CreatePitRequest request, ActionListener<CreatePitResponse> listener) {
        final StepListener<SearchResponse> createPitListener = new StepListener<>();
        final ActionListener<CreatePitResponse> updatePitIdListener = ActionListener.wrap(r -> listener.onResponse(r), e -> {
            logger.error(
                () -> new ParameterizedMessage(
                    "PIT creation failed while updating PIT ID for indices [{}]",
                    Arrays.toString(request.indices())
                )
            );
            listener.onFailure(e);
        });
        createPitController.executeCreatePit(request, task, createPitListener, updatePitIdListener);
    }

    /**
     * Request to create pit reader context with keep alive
     */
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

    /**
     * Create pit reader context response which holds the contextId
     */
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
