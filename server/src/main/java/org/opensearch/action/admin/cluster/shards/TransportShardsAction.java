/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.node.tasks.CreateTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.CreateTaskResponse;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.client.Client;
import org.opensearch.client.OriginSettingClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Table;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestActionListener;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.cat.AbstractCatAction;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.action.cat.RestTable;
import org.opensearch.tasks.*;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import static org.opensearch.rest.BaseRestHandler.parseDeprecatedMasterTimeoutParameter;

public class TransportShardsAction extends HandledTransportAction<GetTaskRequest, GetTaskResponse> {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportShardsAction.class);
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
//    private final ActionFilters actionFilter;
    private final NodeClient client;

    @Inject
    public TransportShardsAction(
        NodeClient client,
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(GetTaskAction.NAME, transportService, actionFilters, GetTaskRequest::new);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = client;
//        this.client = new OriginSettingClient(client, GetTaskAction.TASKS_ORIGIN);
    }

//    @Override
//    protected void doExecute(Task task, final Request request, ActionListener<Task> listener) {
//        executeRequest(task, request, listener);
//    }

    @Override
    protected void doExecute(Task task, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        Task new_task =  transportService.getTaskManager().register("transport", "Admin-action", request);
        TaskResult result = new TaskResult(false, new TaskInfo(new TaskId(clusterService.localNode().getId(), new_task.getId()), new_task.getType(), new_task.getAction(), new_task.getDescription(), new_task.getStatus(), new_task.getStartTime(), new_task.getStartTimeNanos(), true, false, new_task.getParentTaskId(), null, null));
        GetTaskResponse response = new GetTaskResponse(result);
//        new_task.getCancellationTimeout()
        response.setNewTask(new_task);
        listener.onResponse(response);

    }

//    @Override
//    protected void doExecute(Task task, CreateTaskRequest request, ActionListener<CreateTaskResponse> listener) {
//        Task new_task = transportService.getTaskManager().register("transport", "Admin-action", request);
//        TaskResult result = new TaskResult(false, new TaskInfo(new TaskId(clusterService.localNode().getId(), new_task.getId()), new_task.getType(), new_task.getAction(), new_task.getDescription(), new_task.getStatus(), new_task.getStartTime(), new_task.getStartTimeNanos(), true, false, new_task.getParentTaskId(), null, null));
////        GetTaskResponse response = new GetTaskResponse(result);
//        CreateTaskResponse response = new CreateTaskResponse(new_task);
//        listener.onResponse(response);
//    }

//    private void executeRequest(
//        Task task,
//        Request request,
//        ActionListener<Task> originalListener
//    ) {
//        Task new_task = transportService.getTaskManager().register("transport", "Admin-action", request);
//        originalListener.onResponse(new_task);
//    }



}
