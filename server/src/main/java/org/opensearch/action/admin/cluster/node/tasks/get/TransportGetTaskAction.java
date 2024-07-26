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

package org.opensearch.action.admin.cluster.node.tasks.get;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.OriginSettingClient;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.tasks.TaskResult;
import org.opensearch.tasks.TaskResultsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.opensearch.action.admin.cluster.node.tasks.list.TransportListTasksAction.waitForCompletionTimeout;

/**
 * ActionType to get a single task. If the task isn't running then it'll try to request the status from request index.
 * <p>
 * The general flow is:
 * <ul>
 * <li>If this isn't being executed on the node to which the requested TaskId belongs then move to that node.
 * <li>Look up the task and return it if it exists
 * <li>If it doesn't then look up the task from the results index
 * </ul>
 *
 * @opensearch.internal
 */
public class TransportGetTaskAction extends HandledTransportAction<GetTaskRequest, GetTaskResponse> {
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    private final TaskResourceTrackingService taskResourceTrackingService;

    @Inject
    public TransportGetTaskAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        super(GetTaskAction.NAME, transportService, actionFilters, GetTaskRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = new OriginSettingClient(client, GetTaskAction.TASKS_ORIGIN);
        this.xContentRegistry = xContentRegistry;
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    @Override
    protected void doExecute(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        if (clusterService.localNode().getId().equals(request.getTaskId().getNodeId())) {
            getRunningTaskFromNode(thisTask, request, listener);
        } else {
            runOnNodeWithTaskIfPossible(thisTask, request, listener);
        }
    }

    /**
     * Executed on the coordinating node to forward execution of the remaining work to the node that matches that requested
     * {@link TaskId#getNodeId()}. If the node isn't in the cluster then this will just proceed to
     * {@link #getFinishedTaskFromIndex(Task, GetTaskRequest, ActionListener)} on this node.
     */
    private void runOnNodeWithTaskIfPossible(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
        if (request.getTimeout() != null) {
            builder.withTimeout(request.getTimeout());
        }
        DiscoveryNode node = clusterService.state().nodes().get(request.getTaskId().getNodeId());
        if (node == null) {
            // Node is no longer part of the cluster! Try and look the task up from the results index.
            getFinishedTaskFromIndex(thisTask, request, ActionListener.wrap(listener::onResponse, e -> {
                if (e instanceof ResourceNotFoundException) {
                    e = new ResourceNotFoundException(
                        "task ["
                            + request.getTaskId()
                            + "] belongs to the node ["
                            + request.getTaskId().getNodeId()
                            + "] which isn't part of the cluster and there is no record of the task",
                        e
                    );
                }
                listener.onFailure(e);
            }));
            return;
        }
        GetTaskRequest nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
        transportService.sendRequest(
            node,
            GetTaskAction.NAME,
            nodeRequest,
            builder.build(),
            new ActionListenerResponseHandler<>(listener, GetTaskResponse::new, ThreadPool.Names.SAME)
        );
    }

    /**
     * Executed on the node that should be running the task to find and return the running task. Falls back to
     * {@link #getFinishedTaskFromIndex(Task, GetTaskRequest, ActionListener)} if the task isn't still running.
     */
    void getRunningTaskFromNode(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        Task runningTask = taskManager.getTask(request.getTaskId().getId());
        if (runningTask == null) {
            // Task isn't running, go look in the task index
            getFinishedTaskFromIndex(thisTask, request, listener);
        } else {
            if (request.getWaitForCompletion()) {
                // Shift to the generic thread pool and let it wait for the task to complete so we don't block any important threads.
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() {
                        taskManager.waitForTaskCompletion(runningTask, waitForCompletionTimeout(request.getTimeout()));
                        waitedForCompletion(thisTask, request, runningTask.taskInfo(clusterService.localNode().getId(), true), listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            } else {
                taskResourceTrackingService.refreshResourceStats(runningTask);
                TaskInfo info = runningTask.taskInfo(clusterService.localNode().getId(), true);
                listener.onResponse(new GetTaskResponse(new TaskResult(false, info)));
            }
        }
    }

    /**
     * Called after waiting for the task to complete. Attempts to load the results of the task from the tasks index. If it isn't in the
     * index then returns a snapshot of the task taken shortly after completion.
     */
    void waitedForCompletion(
        Task thisTask,
        GetTaskRequest request,
        TaskInfo snapshotOfRunningTask,
        ActionListener<GetTaskResponse> listener
    ) {
        getFinishedTaskFromIndex(thisTask, request, ActionListener.delegateResponse(listener, (delegatedListener, e) -> {
            /*
             * We couldn't load the task from the task index. Instead of 404 we should use the snapshot we took after it finished. If
             * the error isn't a 404 then we'll just throw it back to the user.
             */
            if (ExceptionsHelper.unwrap(e, ResourceNotFoundException.class) != null) {
                delegatedListener.onResponse(new GetTaskResponse(new TaskResult(true, snapshotOfRunningTask)));
            } else {
                delegatedListener.onFailure(e);
            }
        }));
    }

    /**
     * Send a {@link GetRequest} to the tasks index looking for a persisted copy of the task completed task. It'll only be found only if the
     * task's result was stored. Called on the node that once had the task if that node is still part of the cluster or on the
     * coordinating node if the node is no longer part of the cluster.
     */
    void getFinishedTaskFromIndex(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        GetRequest get = new GetRequest(TaskResultsService.TASK_INDEX, request.getTaskId().toString());
        get.setParentTask(clusterService.localNode().getId(), thisTask.getId());

        client.get(get, ActionListener.wrap(r -> onGetFinishedTaskFromIndex(r, listener), e -> {
            if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
                // We haven't yet created the index for the task results so it can't be found.
                listener.onFailure(
                    new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", e, request.getTaskId())
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Called with the {@linkplain GetResponse} from loading the task from the results index. Called on the node that once had the task if
     * that node is part of the cluster or on the coordinating node if the node wasn't part of the cluster.
     */
    void onGetFinishedTaskFromIndex(GetResponse response, ActionListener<GetTaskResponse> listener) throws IOException {
        if (false == response.isExists()) {
            listener.onFailure(new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", response.getId()));
            return;
        }
        if (response.isSourceEmpty()) {
            listener.onFailure(new OpenSearchException("Stored task status for [{}] didn't contain any source!", response.getId()));
            return;
        }
        try (
            XContentParser parser = XContentHelper.createParser(
                xContentRegistry,
                LoggingDeprecationHandler.INSTANCE,
                response.getSourceAsBytesRef()
            )
        ) {
            TaskResult result = TaskResult.PARSER.apply(parser, null);
            listener.onResponse(new GetTaskResponse(result));
        }
    }
}
