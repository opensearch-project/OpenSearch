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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.node.NodeClosedException;
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.OriginSettingClient;

import java.util.function.Predicate;

/**
 * This service is used by persistent tasks and allocated persistent tasks to communicate changes
 * to the cluster-manager node so that the cluster-manager can update the cluster state and can track of the states
 * of the persistent tasks.
 *
 * @opensearch.internal
 */
public class PersistentTasksService {

    private static final Logger logger = LogManager.getLogger(PersistentTasksService.class);

    public static final String PERSISTENT_TASK_ORIGIN = "persistent_tasks";

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public PersistentTasksService(ClusterService clusterService, ThreadPool threadPool, Client client) {
        this.client = new OriginSettingClient(client, PERSISTENT_TASK_ORIGIN);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Notifies the cluster-manager node to create new persistent task and to assign it to a node.
     */
    public <Params extends PersistentTaskParams> void sendStartRequest(
        final String taskId,
        final String taskName,
        final Params taskParams,
        final ActionListener<PersistentTask<Params>> listener
    ) {
        @SuppressWarnings("unchecked")
        final ActionListener<PersistentTask<?>> wrappedListener = ActionListener.map(listener, t -> (PersistentTask<Params>) t);
        StartPersistentTaskAction.Request request = new StartPersistentTaskAction.Request(taskId, taskName, taskParams);
        execute(request, StartPersistentTaskAction.INSTANCE, wrappedListener);
    }

    /**
     * Notifies the cluster-manager node about the completion of a persistent task.
     * <p>
     * When {@code failure} is {@code null}, the persistent task is considered as successfully completed.
     */
    public void sendCompletionRequest(
        final String taskId,
        final long taskAllocationId,
        final @Nullable Exception taskFailure,
        final ActionListener<PersistentTask<?>> listener
    ) {
        CompletionPersistentTaskAction.Request request = new CompletionPersistentTaskAction.Request(taskId, taskAllocationId, taskFailure);
        execute(request, CompletionPersistentTaskAction.INSTANCE, listener);
    }

    /**
     * Cancels a locally running task using the Task Manager API
     */
    void sendCancelRequest(final long taskId, final String reason, final ActionListener<CancelTasksResponse> listener) {
        CancelTasksRequest request = new CancelTasksRequest();
        request.setTaskId(new TaskId(clusterService.localNode().getId(), taskId));
        request.setReason(reason);
        try {
            client.admin().cluster().cancelTasks(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Notifies the cluster-manager node that the state of a persistent task has changed.
     * <p>
     * Persistent task implementers shouldn't call this method directly and use
     * {@link AllocatedPersistentTask#updatePersistentTaskState} instead
     */
    void sendUpdateStateRequest(
        final String taskId,
        final long taskAllocationID,
        final PersistentTaskState taskState,
        final ActionListener<PersistentTask<?>> listener
    ) {
        UpdatePersistentTaskStatusAction.Request request = new UpdatePersistentTaskStatusAction.Request(
            taskId,
            taskAllocationID,
            taskState
        );
        execute(request, UpdatePersistentTaskStatusAction.INSTANCE, listener);
    }

    /**
     * Notifies the cluster-manager node to remove a persistent task from the cluster state
     */
    public void sendRemoveRequest(final String taskId, final ActionListener<PersistentTask<?>> listener) {
        RemovePersistentTaskAction.Request request = new RemovePersistentTaskAction.Request(taskId);
        execute(request, RemovePersistentTaskAction.INSTANCE, listener);
    }

    /**
     * Executes an asynchronous persistent task action using the client.
     * <p>
     * The origin is set in the context and the listener is wrapped to ensure the proper context is restored
     */
    private <Req extends ActionRequest, Resp extends PersistentTaskResponse> void execute(
        final Req request,
        final ActionType<Resp> action,
        final ActionListener<PersistentTask<?>> listener
    ) {
        try {
            client.execute(action, request, ActionListener.map(listener, PersistentTaskResponse::getTask));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Waits for a given persistent task to comply with a given predicate, then call back the listener accordingly.
     *
     * @param taskId the persistent task id
     * @param predicate the persistent task predicate to evaluate
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     */
    public void waitForPersistentTaskCondition(
        final String taskId,
        final Predicate<PersistentTask<?>> predicate,
        final @Nullable TimeValue timeout,
        final WaitForPersistentTaskListener<?> listener
    ) {
        final Predicate<ClusterState> clusterStatePredicate = clusterState -> predicate.test(
            PersistentTasksCustomMetadata.getTaskWithId(clusterState, taskId)
        );

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
        final ClusterState clusterState = observer.setAndGetObservedState();
        if (clusterStatePredicate.test(clusterState)) {
            listener.onResponse(PersistentTasksCustomMetadata.getTaskWithId(clusterState, taskId));
        } else {
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(PersistentTasksCustomMetadata.getTaskWithId(state, taskId));
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onTimeout(timeout);
                }
            }, clusterStatePredicate);
        }
    }

    /**
     * Waits for persistent tasks to comply with a given predicate, then call back the listener accordingly.
     *
     * @param predicate the predicate to evaluate
     * @param timeout a timeout for waiting
     * @param listener the callback listener
     */
    public void waitForPersistentTasksCondition(
        final Predicate<PersistentTasksCustomMetadata> predicate,
        final @Nullable TimeValue timeout,
        final ActionListener<Boolean> listener
    ) {
        final Predicate<ClusterState> clusterStatePredicate = clusterState -> predicate.test(
            clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE)
        );

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, timeout, logger, threadPool.getThreadContext());
        if (clusterStatePredicate.test(observer.setAndGetObservedState())) {
            listener.onResponse(true);
        } else {
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    listener.onResponse(true);
                }

                @Override
                public void onClusterServiceClose() {
                    listener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(new IllegalStateException("Timed out when waiting for persistent tasks after " + timeout));
                }
            }, clusterStatePredicate, timeout);
        }
    }

    /**
     * Interface for a class that waits and listens for a persistent task.
     *
     * @opensearch.internal
     */
    public interface WaitForPersistentTaskListener<P extends PersistentTaskParams> extends ActionListener<PersistentTask<P>> {
        default void onTimeout(TimeValue timeout) {
            onFailure(new IllegalStateException("Timed out when waiting for persistent task after " + timeout));
        }
    }
}
