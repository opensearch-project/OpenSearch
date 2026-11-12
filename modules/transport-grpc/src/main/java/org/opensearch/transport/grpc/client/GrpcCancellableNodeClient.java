/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.FilterClient;
import org.opensearch.transport.client.OriginSettingClient;
import org.opensearch.transport.client.node.NodeClient;

import io.grpc.stub.ServerCallStreamObserver;

import static org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;

/**
 * A {@link Client} that cancels the task executed locally when the gRPC call it is bound to is cancelled
 * before completion (for example when the client disconnects or the call deadline is exceeded).
 * <p>
 * This is the gRPC counterpart to {@code RestCancellableNodeClient}, which performs the same role for the
 * HTTP transport by listening for channel closure. Without it, a cancelled gRPC request would leave its
 * underlying task (such as a search) running server-side for its full duration, wasting resources on a
 * result no client will read.
 *
 * @opensearch.internal
 */
public class GrpcCancellableNodeClient extends FilterClient {
    private static final Logger logger = LogManager.getLogger(GrpcCancellableNodeClient.class);

    private final NodeClient client;
    private final ServerCallStreamObserver<?> serverCallObserver;

    /**
     * Creates a new GrpcCancellableNodeClient.
     *
     * @param client the node client used to execute the action locally
     * @param serverCallObserver the gRPC response observer whose cancellation signal drives task cancellation
     */
    public GrpcCancellableNodeClient(NodeClient client, ServerCallStreamObserver<?> serverCallObserver) {
        super(client);
        this.client = client;
        this.serverCallObserver = serverCallObserver;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        // Execute locally so we can obtain the Task and cancel it if the gRPC call is abandoned.
        Task task = client.executeLocally(action, request, listener);
        final TaskId taskId = new TaskId(client.getLocalNodeId(), task.getId());

        serverCallObserver.setOnCancelHandler(() -> {
            logger.debug("gRPC call cancelled by client, cancelling task {}", taskId);
            cancelTask(taskId);
        });

        // Guard against the race where the call is cancelled between executeLocally and setOnCancelHandler:
        // if it is already cancelled the handler may never fire, so cancel the task explicitly.
        if (serverCallObserver.isCancelled()) {
            logger.debug("gRPC call already cancelled, cancelling task {}", taskId);
            cancelTask(taskId);
        }
    }

    /**
     * Cancels the task associated with the abandoned gRPC call. Runs the cancellation as the tasks-origin
     * system user, mirroring {@code RestCancellableNodeClient}.
     *
     * @param taskId the id of the task to cancel
     */
    protected void cancelTask(TaskId taskId) {
        CancelTasksRequest req = new CancelTasksRequest().setTaskId(taskId).setReason("gRPC client cancelled the request");
        // force the origin to execute the cancellation as a system user
        new OriginSettingClient(client, TASKS_ORIGIN).admin().cluster().cancelTasks(req, ActionListener.wrap(() -> {}));
    }
}
