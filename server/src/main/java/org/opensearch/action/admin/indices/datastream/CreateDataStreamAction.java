/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataCreateDataStreamService;
import org.opensearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataStreamClusterStateUpdateRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport action for creating a datastream
 *
 * @opensearch.internal
 */
public class CreateDataStreamAction extends ActionType<AcknowledgedResponse> {

    public static final CreateDataStreamAction INSTANCE = new CreateDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/create";

    private CreateDataStreamAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Request for Creating Data Stream
     *
     * @opensearch.internal
     */
    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest {

        private final String name;

        public Request(String name) {
            this.name = name;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(name) == false) {
                validationException = ValidateActions.addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String[] indices() {
            return new String[] { name };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }

    /**
     * Transport Action for Creating Data Stream
     *
     * @opensearch.internal
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, AcknowledgedResponse> {

        private final MetadataCreateDataStreamService metadataCreateDataStreamService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            MetadataCreateDataStreamService metadataCreateDataStreamService
        ) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.metadataCreateDataStreamService = metadataCreateDataStreamService;
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
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
            throws Exception {
            CreateDataStreamClusterStateUpdateRequest updateRequest = new CreateDataStreamClusterStateUpdateRequest(
                request.name,
                request.clusterManagerNodeTimeout(),
                request.timeout()
            );
            metadataCreateDataStreamService.createDataStream(updateRequest, listener);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}
