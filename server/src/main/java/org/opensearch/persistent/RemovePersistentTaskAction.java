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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.persistent;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport action to remove a persistent task
 *
 * @opensearch.internal
 */
public class RemovePersistentTaskAction extends ActionType<PersistentTaskResponse> {

    public static final RemovePersistentTaskAction INSTANCE = new RemovePersistentTaskAction();
    public static final String NAME = "cluster:admin/persistent/remove";

    private RemovePersistentTaskAction() {
        super(NAME, PersistentTaskResponse::new);
    }

    /**
     * The request.
     *
     * @opensearch.internal
     */
    public static class Request extends ClusterManagerNodeRequest<Request> {

        private String taskId;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
        }

        public Request(String taskId) {
            this.taskId = taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(taskId, request.taskId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId);
        }
    }

    /**
     * The request builder.
     *
     * @opensearch.internal
     */
    public static class RequestBuilder extends ClusterManagerNodeOperationRequestBuilder<Request, PersistentTaskResponse, RequestBuilder> {

        protected RequestBuilder(OpenSearchClient client, RemovePersistentTaskAction action) {
            super(client, action, new Request());
        }

        public final RequestBuilder setTaskId(String taskId) {
            request.setTaskId(taskId);
            return this;
        }

    }

    /**
     * The transport action.
     *
     * @opensearch.internal
     */
    public static class TransportAction extends TransportClusterManagerNodeAction<Request, PersistentTaskResponse> {

        private final PersistentTasksClusterService persistentTasksClusterService;

        @Inject
        public TransportAction(
            TransportService transportService,
            ClusterService clusterService,
            ThreadPool threadPool,
            ActionFilters actionFilters,
            PersistentTasksClusterService persistentTasksClusterService,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                RemovePersistentTaskAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver
            );
            this.persistentTasksClusterService = persistentTasksClusterService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected PersistentTaskResponse read(StreamInput in) throws IOException {
            return new PersistentTaskResponse(in);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected final void clusterManagerOperation(
            final Request request,
            ClusterState state,
            final ActionListener<PersistentTaskResponse> listener
        ) {
            persistentTasksClusterService.removePersistentTask(
                request.taskId,
                ActionListener.delegateFailure(
                    listener,
                    (delegatedListener, task) -> delegatedListener.onResponse(new PersistentTaskResponse(task))
                )
            );
        }
    }
}
