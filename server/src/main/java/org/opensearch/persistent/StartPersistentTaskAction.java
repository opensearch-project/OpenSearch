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
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 *  This action can be used to add the record for the persistent action to the cluster state.
 *
 *  @opensearch.internal
 */
public class StartPersistentTaskAction extends ActionType<PersistentTaskResponse> {

    public static final StartPersistentTaskAction INSTANCE = new StartPersistentTaskAction();
    public static final String NAME = "cluster:admin/persistent/start";

    private StartPersistentTaskAction() {
        super(NAME, PersistentTaskResponse::new);
    }

    /**
     * Request for the action.
     *
     * @opensearch.internal
     */
    public static class Request extends ClusterManagerNodeRequest<Request> {

        private String taskId;

        private String taskName;

        private PersistentTaskParams params;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            taskId = in.readString();
            taskName = in.readString();
            params = in.readNamedWriteable(PersistentTaskParams.class);
        }

        public Request(String taskId, String taskName, PersistentTaskParams params) {
            this.taskId = taskId;
            this.taskName = taskName;
            this.params = params;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(taskId);
            out.writeString(taskName);
            out.writeNamedWriteable(params);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (this.taskId == null) {
                validationException = addValidationError("task id must be specified", validationException);
            }
            if (this.taskName == null) {
                validationException = addValidationError("action must be specified", validationException);
            }
            if (params != null) {
                if (params.getWriteableName().equals(taskName) == false) {
                    validationException = addValidationError(
                        "params have to have the same writeable name as task. params: " + params.getWriteableName() + " task: " + taskName,
                        validationException
                    );
                }
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request1 = (Request) o;
            return Objects.equals(taskId, request1.taskId)
                && Objects.equals(taskName, request1.taskName)
                && Objects.equals(params, request1.params);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, taskName, params);
        }

        public String getTaskName() {
            return taskName;
        }

        public void setTaskName(String taskName) {
            this.taskName = taskName;
        }

        public String getTaskId() {
            return taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public PersistentTaskParams getParams() {
            return params;
        }

        @Nullable
        public void setParams(PersistentTaskParams params) {
            this.params = params;
        }

    }

    /**
     * The request builder.
     *
     * @opensearch.internal
     */
    public static class RequestBuilder extends ClusterManagerNodeOperationRequestBuilder<Request, PersistentTaskResponse, RequestBuilder> {

        protected RequestBuilder(OpenSearchClient client, StartPersistentTaskAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder setTaskId(String taskId) {
            request.setTaskId(taskId);
            return this;
        }

        public RequestBuilder setAction(String action) {
            request.setTaskName(action);
            return this;
        }

        public RequestBuilder setRequest(PersistentTaskParams params) {
            request.setParams(params);
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
            PersistentTasksExecutorRegistry persistentTasksExecutorRegistry,
            PersistentTasksService persistentTasksService,
            IndexNameExpressionResolver indexNameExpressionResolver
        ) {
            super(
                StartPersistentTaskAction.NAME,
                transportService,
                clusterService,
                threadPool,
                actionFilters,
                Request::new,
                indexNameExpressionResolver
            );
            this.persistentTasksClusterService = persistentTasksClusterService;
            NodePersistentTasksExecutor executor = new NodePersistentTasksExecutor(threadPool);
            clusterService.addListener(
                new PersistentTasksNodeService(
                    persistentTasksService,
                    persistentTasksExecutorRegistry,
                    transportService.getTaskManager(),
                    executor
                )
            );
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
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
            persistentTasksClusterService.createPersistentTask(
                request.taskId,
                request.taskName,
                request.params,
                ActionListener.delegateFailure(
                    listener,
                    (delegatedListener, task) -> delegatedListener.onResponse(new PersistentTaskResponse(task))
                )
            );
        }
    }
}
