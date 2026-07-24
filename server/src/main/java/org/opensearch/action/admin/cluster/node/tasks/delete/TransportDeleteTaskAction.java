/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.tasks.delete;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResult;
import org.opensearch.tasks.TaskResultsService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.OriginSettingClient;

import java.io.IOException;

/**
 * ActionType to delete a stored completed task result. Running tasks are not cancelled by this action.
 *
 * @opensearch.internal
 */
public class TransportDeleteTaskAction extends HandledTransportAction<DeleteTaskRequest, AcknowledgedResponse> {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportDeleteTaskAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(DeleteTaskAction.NAME, transportService, actionFilters, DeleteTaskRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = new OriginSettingClient(client, GetTaskAction.TASKS_ORIGIN);
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void doExecute(Task thisTask, DeleteTaskRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (clusterService.localNode().getId().equals(request.getTaskId().getNodeId())) {
            deleteTaskResultFromLocalNode(thisTask, request, listener);
        } else {
            runOnNodeWithTaskIfPossible(thisTask, request, listener);
        }
    }

    private void runOnNodeWithTaskIfPossible(Task thisTask, DeleteTaskRequest request, ActionListener<AcknowledgedResponse> listener) {
        DiscoveryNode node = clusterService.state().nodes().get(request.getTaskId().getNodeId());
        if (node == null) {
            deleteFinishedTaskFromIndex(thisTask, request, ActionListener.wrap(listener::onResponse, e -> {
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

        DeleteTaskRequest nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
        transportService.sendRequest(
            node,
            DeleteTaskAction.NAME,
            nodeRequest,
            new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::new, ThreadPool.Names.SAME)
        );
    }

    void deleteTaskResultFromLocalNode(Task thisTask, DeleteTaskRequest request, ActionListener<AcknowledgedResponse> listener) {
        if (taskManager.getTask(request.getTaskId().getId()) != null) {
            listener.onFailure(
                new OpenSearchStatusException(
                    "task [{}] is still running and cannot be deleted; use the cancel tasks API to cancel running tasks",
                    RestStatus.CONFLICT,
                    request.getTaskId()
                )
            );
            return;
        }
        deleteFinishedTaskFromIndex(thisTask, request, listener);
    }

    void deleteFinishedTaskFromIndex(Task thisTask, DeleteTaskRequest request, ActionListener<AcknowledgedResponse> listener) {
        GetRequest get = new GetRequest(TaskResultsService.TASK_INDEX, request.getTaskId().toString());
        get.setParentTask(clusterService.localNode().getId(), thisTask.getId());

        client.get(get, ActionListener.wrap(r -> onGetFinishedTaskFromIndex(r, thisTask, listener), e -> {
            if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
                listener.onFailure(
                    new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", e, request.getTaskId())
                );
            } else {
                listener.onFailure(e);
            }
        }));
    }

    void onGetFinishedTaskFromIndex(GetResponse response, Task thisTask, ActionListener<AcknowledgedResponse> listener) throws IOException {
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
            if (result.isCompleted() == false) {
                listener.onFailure(
                    new OpenSearchStatusException(
                        "task [{}] is not completed and cannot be deleted",
                        RestStatus.CONFLICT,
                        result.getTask().getTaskId()
                    )
                );
                return;
            }
            deleteTaskResult(response.getId(), thisTask, listener);
        }
    }

    private void deleteTaskResult(String taskResultId, Task thisTask, ActionListener<AcknowledgedResponse> listener) {
        DeleteRequest delete = new DeleteRequest(TaskResultsService.TASK_INDEX, taskResultId);
        delete.setParentTask(clusterService.localNode().getId(), thisTask.getId());
        client.delete(delete, ActionListener.wrap(response -> onDeleteTaskResult(taskResultId, response, listener), listener::onFailure));
    }

    private void onDeleteTaskResult(String taskResultId, DeleteResponse response, ActionListener<AcknowledgedResponse> listener) {
        if (response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            listener.onFailure(new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", taskResultId));
            return;
        }
        listener.onResponse(new AcknowledgedResponse(true));
    }
}
