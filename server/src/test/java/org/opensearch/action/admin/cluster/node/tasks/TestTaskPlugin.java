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

package org.opensearch.action.admin.cluster.node.tasks;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.ActionType;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.action.support.tasks.BaseTasksRequest;
import org.opensearch.action.support.tasks.BaseTasksResponse;
import org.opensearch.action.support.tasks.TransportTasksAction;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.OpenSearchClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.opensearch.test.OpenSearchTestCase.waitUntil;

/**
 * A plugin that adds a cancellable blocking test task of integration testing of the task manager.
 */
public class TestTaskPlugin extends Plugin implements ActionPlugin, NetworkPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(TestTaskAction.INSTANCE, TransportTestTaskAction.class),
            new ActionHandler<>(UnblockTestTasksAction.INSTANCE, TransportUnblockTestTasksAction.class)
        );
    }

    @Override
    public Collection<String> getTaskHeaders() {
        return Collections.singleton("Custom-Task-Header");
    }

    /**
     * Intercept transport requests to verify that all of the ones that should
     * have the origin set <strong>do</strong> have the origin set and the ones
     * that should not have the origin set <strong>do not</strong> have it set.
     */
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        return Collections.singletonList(new OriginAssertingInterceptor(threadContext));
    }

    static class TestTask extends CancellableTask {

        private volatile boolean blocked = true;

        TestTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers) {
            super(id, type, action, description, parentTaskId, headers);
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return false;
        }

        public boolean isBlocked() {
            return blocked;
        }

        public void unblock() {
            blocked = false;
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
        }

        public NodeResponse(DiscoveryNode node) {
            super(node);
        }
    }

    public static class NodesResponse extends BaseNodesResponse<NodeResponse> implements ToXContentFragment {

        NodesResponse(StreamInput in) throws IOException {
            super(in);
        }

        NodesResponse(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }

        public int getFailureCount() {
            return failures().size();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("failure_count", getFailureCount());
            return builder;
        }
    }

    public static class NodeRequest extends TransportRequest {
        protected final String requestName;
        protected final boolean shouldBlock;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
            shouldBlock = in.readBoolean();
        }

        public NodeRequest(NodesRequest request, boolean shouldBlock) {
            requestName = request.requestName;
            this.shouldBlock = shouldBlock;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeBoolean(shouldBlock);
        }

        @Override
        public String getDescription() {
            return "NodeRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new TestTask(id, type, action, this.getDescription(), parentTaskId, headers);
        }
    }

    public static class NodesRequest extends BaseNodesRequest<NodesRequest> {
        private String requestName;
        private boolean shouldStoreResult = false;
        private boolean shouldBlock = true;
        private boolean shouldFail = false;

        NodesRequest(StreamInput in) throws IOException {
            super(in);
            requestName = in.readString();
            shouldStoreResult = in.readBoolean();
            shouldBlock = in.readBoolean();
            shouldFail = in.readBoolean();
        }

        NodesRequest(String requestName, String... nodesIds) {
            super(nodesIds);
            this.requestName = requestName;
        }

        public void setShouldStoreResult(boolean shouldStoreResult) {
            this.shouldStoreResult = shouldStoreResult;
        }

        @Override
        public boolean getShouldStoreResult() {
            return shouldStoreResult;
        }

        public void setShouldBlock(boolean shouldBlock) {
            this.shouldBlock = shouldBlock;
        }

        public boolean getShouldBlock() {
            return shouldBlock;
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }

        public boolean getShouldFail() {
            return shouldFail;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(requestName);
            out.writeBoolean(shouldStoreResult);
            out.writeBoolean(shouldBlock);
            out.writeBoolean(shouldFail);
        }

        @Override
        public String getDescription() {
            return "NodesRequest[" + requestName + "]";
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers) {
                @Override
                public boolean shouldCancelChildrenOnCancellation() {
                    return true;
                }
            };
        }
    }

    public static class TransportTestTaskAction extends TransportNodesAction<NodesRequest, NodesResponse, NodeRequest, NodeResponse> {

        @Inject
        public TransportTestTaskAction(ThreadPool threadPool, ClusterService clusterService, TransportService transportService) {
            super(
                TestTaskAction.NAME,
                threadPool,
                clusterService,
                transportService,
                new ActionFilters(new HashSet<>()),
                NodesRequest::new,
                NodeRequest::new,
                ThreadPool.Names.GENERIC,
                NodeResponse.class
            );
        }

        @Override
        protected NodesResponse newResponse(NodesRequest request, List<NodeResponse> responses, List<FailedNodeException> failures) {
            if (request.getShouldFail()) {
                throw new IllegalStateException("Simulating operation failure");
            }
            return new NodesResponse(clusterService.getClusterName(), responses, failures);
        }

        @Override
        protected NodeRequest newNodeRequest(NodesRequest request) {
            return new NodeRequest(request, request.getShouldBlock());
        }

        @Override
        protected NodeResponse newNodeResponse(StreamInput in) throws IOException {
            return new NodeResponse(in);
        }

        @Override
        protected void doExecute(Task task, NodesRequest request, ActionListener<NodesResponse> listener) {
            super.doExecute(task, request, listener);
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request, Task task) {
            logger.info("Test task started on the node {}", clusterService.localNode());
            if (request.shouldBlock) {
                try {
                    waitUntil(() -> {
                        if (((CancellableTask) task).isCancelled()) {
                            throw new RuntimeException("Cancelled!");
                        }
                        return ((TestTask) task).isBlocked() == false;
                    });
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            logger.info("Test task finished on the node {}", clusterService.localNode());
            return new NodeResponse(clusterService.localNode());
        }

        @Override
        protected NodeResponse nodeOperation(NodeRequest request) {
            throw new UnsupportedOperationException("the task parameter is required");
        }

    }

    public static class TestTaskAction extends ActionType<NodesResponse> {

        public static final TestTaskAction INSTANCE = new TestTaskAction();
        public static final String NAME = "cluster:admin/tasks/test";

        private TestTaskAction() {
            super(NAME, NodesResponse::new);
        }
    }

    public static class UnblockTestTaskResponse implements Writeable {

        UnblockTestTaskResponse() {

        }

        UnblockTestTaskResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    public static class UnblockTestTasksRequest extends BaseTasksRequest<UnblockTestTasksRequest> {

        UnblockTestTasksRequest() {}

        UnblockTestTasksRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public boolean match(Task task) {
            return task instanceof TestTask && super.match(task);
        }
    }

    public static class UnblockTestTasksResponse extends BaseTasksResponse {

        private List<UnblockTestTaskResponse> tasks;

        UnblockTestTasksResponse(
            List<UnblockTestTaskResponse> tasks,
            List<TaskOperationFailure> taskFailures,
            List<? extends FailedNodeException> nodeFailures
        ) {
            super(taskFailures, nodeFailures);
            this.tasks = tasks == null ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(tasks));
        }

        UnblockTestTasksResponse(StreamInput in) throws IOException {
            super(in);
            int taskCount = in.readVInt();
            List<UnblockTestTaskResponse> builder = new ArrayList<>();
            for (int i = 0; i < taskCount; i++) {
                builder.add(new UnblockTestTaskResponse(in));
            }
            tasks = Collections.unmodifiableList(builder);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(tasks.size());
            for (UnblockTestTaskResponse task : tasks) {
                task.writeTo(out);
            }
        }
    }

    /**
     * Test class for testing task operations
     */
    public static class TransportUnblockTestTasksAction extends TransportTasksAction<
        Task,
        UnblockTestTasksRequest,
        UnblockTestTasksResponse,
        UnblockTestTaskResponse> {

        @Inject
        public TransportUnblockTestTasksAction(ClusterService clusterService, TransportService transportService) {
            super(
                UnblockTestTasksAction.NAME,
                clusterService,
                transportService,
                new ActionFilters(new HashSet<>()),
                UnblockTestTasksRequest::new,
                UnblockTestTasksResponse::new,
                UnblockTestTaskResponse::new,
                ThreadPool.Names.MANAGEMENT
            );
        }

        @Override
        protected UnblockTestTasksResponse newResponse(
            UnblockTestTasksRequest request,
            List<UnblockTestTaskResponse> tasks,
            List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions
        ) {
            return new UnblockTestTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
        }

        @Override
        protected void taskOperation(UnblockTestTasksRequest request, Task task, ActionListener<UnblockTestTaskResponse> listener) {
            ((TestTask) task).unblock();
            listener.onResponse(new UnblockTestTaskResponse());
        }

    }

    public static class UnblockTestTasksAction extends ActionType<UnblockTestTasksResponse> {

        public static final UnblockTestTasksAction INSTANCE = new UnblockTestTasksAction();
        public static final String NAME = "cluster:admin/tasks/testunblock";

        private UnblockTestTasksAction() {
            super(NAME, UnblockTestTasksResponse::new);
        }
    }

    public static class UnblockTestTasksRequestBuilder extends ActionRequestBuilder<UnblockTestTasksRequest, UnblockTestTasksResponse> {

        protected UnblockTestTasksRequestBuilder(OpenSearchClient client, ActionType<UnblockTestTasksResponse> action) {
            super(client, action, new UnblockTestTasksRequest());
        }
    }

    private static class OriginAssertingInterceptor implements TransportInterceptor {
        private final ThreadContext threadContext;

        private OriginAssertingInterceptor(ThreadContext threadContext) {
            this.threadContext = threadContext;
        }

        @Override
        public AsyncSender interceptSender(AsyncSender sender) {
            return new AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    if (action.startsWith("indices:data/write/bulk[s]")) {
                        /*
                         * We can't reason about these requests because
                         * *sometimes* they should have the origin, if they are
                         * running on the node that stores the task. But
                         * sometimes they won't be and in that case they don't
                         * need the origin. Either way, the interesting work is
                         * done by checking that the main bulk request
                         * (without the [s] part) has the origin.
                         */
                        sender.sendRequest(connection, action, request, options, handler);
                        return;
                    }
                    String expectedOrigin = shouldHaveOrigin(action, request) ? TASKS_ORIGIN : null;
                    String actualOrigin = threadContext.getTransient(ThreadContext.ACTION_ORIGIN_TRANSIENT_NAME);
                    if (Objects.equals(expectedOrigin, actualOrigin)) {
                        sender.sendRequest(connection, action, request, options, handler);
                        return;
                    }
                    handler.handleException(
                        new TransportException(
                            "should have origin of ["
                                + expectedOrigin
                                + "] but was ["
                                + actualOrigin
                                + "] action was ["
                                + action
                                + "]["
                                + request
                                + "]"
                        )
                    );
                }
            };
        }

        private boolean shouldHaveOrigin(String action, TransportRequest request) {
            if (false == action.startsWith("indices:")) {
                /*
                 * The Tasks API never uses origin with non-indices actions.
                 */
                return false;
            }
            if (action.startsWith("indices:admin/refresh") || action.startsWith("indices:data/read/search")) {
                /*
                 * The test refreshes and searches to count the number of tasks
                 * in the index and the Tasks API never does either.
                 */
                return false;
            }
            if (false == (request instanceof IndicesRequest)) {
                return false;
            }
            IndicesRequest ir = (IndicesRequest) request;
            /*
             * When the API Tasks API makes an indices request it only every
             * targets the .tasks index. Other requests come from the tests.
             */
            return Arrays.equals(new String[] { ".tasks" }, ir.indices());
        }
    }
}
