/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataDataStreamsService;
import org.opensearch.cluster.metadata.MetadataDataStreamsService.ModifyDataStreamsClusterStateUpdateRequest;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Applies a batch of {@link DataStreamAction} operations (add/remove backing index) to data-stream metadata atomically.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ModifyDataStreamsAction extends ActionType<AcknowledgedResponse> {

    public static final ModifyDataStreamsAction INSTANCE = new ModifyDataStreamsAction();
    public static final String NAME = "indices:admin/data_stream/modify";

    private ModifyDataStreamsAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Request carrying the data-stream actions to apply.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final List<DataStreamAction> actions;

        public Request(List<DataStreamAction> actions) {
            this.actions = new ArrayList<>(Objects.requireNonNull(actions, "actions must not be null"));
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.actions = in.readList(DataStreamAction::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(actions);
        }

        public List<DataStreamAction> getActions() {
            return actions;
        }

        /**
         * Exposes every data stream and backing index referenced by this request's actions so that index-level
         * authorization can be enforced. Without this, the security layer would evaluate the request as a bare
         * cluster action and could not restrict which data streams or indices a caller may modify.
         */
        @Override
        public String[] indices() {
            return actions.stream().flatMap(action -> Stream.of(action.dataStream(), action.index())).distinct().toArray(String[]::new);
        }

        @Override
        public IndicesOptions indicesOptions() {
            // Names are always concrete (data stream names and ".ds-" backing index names); no wildcard expansion.
            return IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (actions == null || actions.isEmpty()) {
                validationException = addValidationError("at least one data stream action must be specified", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return actions.equals(request.actions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(actions);
        }
    }

    /**
     * Transport action for modifying data streams.
     *
     * @opensearch.internal
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, AcknowledgedResponse>
        implements
            TransportIndicesResolvingAction<Request> {

        private final MetadataDataStreamsService metadataDataStreamsService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            MetadataDataStreamsService metadataDataStreamsService
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.metadataDataStreamsService = metadataDataStreamsService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AcknowledgedResponse read(StreamInput in) throws IOException {
            return new AcknowledgedResponse(in);
        }

        @Override
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
            ModifyDataStreamsClusterStateUpdateRequest updateRequest = new ModifyDataStreamsClusterStateUpdateRequest(
                request.getActions(),
                request.clusterManagerNodeTimeout(),
                request.timeout()
            );
            metadataDataStreamsService.modifyDataStream(updateRequest, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        public ResolvedIndices resolveIndices(Request request) {
            // Names are already concrete data stream and backing index names, matching Request#indices().
            return ResolvedIndices.of(request.indices());
        }
    }
}
