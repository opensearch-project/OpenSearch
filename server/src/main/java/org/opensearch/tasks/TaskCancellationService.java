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

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ChannelActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.EmptyTransportResponseHandler;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Service used to cancel a task
 *
 * @opensearch.internal
 */
public class TaskCancellationService {
    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";
    private static final Logger logger = LogManager.getLogger(TaskCancellationService.class);
    private final TransportService transportService;
    private final TaskManager taskManager;

    public TaskCancellationService(TransportService transportService) {
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        transportService.registerRequestHandler(
            BAN_PARENT_ACTION_NAME,
            ThreadPool.Names.SAME,
            BanParentTaskRequest::new,
            new BanParentRequestHandler()
        );
    }

    private String localNodeId() {
        return transportService.getLocalNode().getId();
    }

    void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        final TaskId taskId = task.taskInfo(localNodeId(), false).getTaskId();
        if (task.shouldCancelChildrenOnCancellation()) {
            logger.trace("cancelling task [{}] and its descendants", taskId);
            StepListener<Void> completedListener = new StepListener<>();
            GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(ActionListener.map(completedListener, r -> null), 3);
            Collection<DiscoveryNode> childrenNodes = taskManager.startBanOnChildrenNodes(task.getId(), () -> {
                logger.trace("child tasks of parent [{}] are completed", taskId);
                groupedListener.onResponse(null);
            });
            taskManager.cancel(task, reason, () -> {
                logger.trace("task [{}] is cancelled", taskId);
                groupedListener.onResponse(null);
            });
            StepListener<Void> banOnNodesListener = new StepListener<>();
            setBanOnNodes(reason, waitForCompletion, task, childrenNodes, banOnNodesListener);
            banOnNodesListener.whenComplete(groupedListener::onResponse, groupedListener::onFailure);
            // If we start unbanning when the last child task completed and that child task executed with a specific user, then unban
            // requests are denied because internal requests can't run with a user. We need to remove bans with the current thread context.
            final Runnable removeBansRunnable = transportService.getThreadPool()
                .getThreadContext()
                .preserveContext(() -> removeBanOnNodes(task, childrenNodes));
            // We remove bans after all child tasks are completed although in theory we can do it on a per-node basis.
            completedListener.whenComplete(r -> removeBansRunnable.run(), e -> removeBansRunnable.run());
            // if wait_for_completion is true, then only return when (1) bans are placed on child nodes, (2) child tasks are
            // completed or failed, (3) the main task is cancelled. Otherwise, return after bans are placed on child nodes.
            if (waitForCompletion) {
                completedListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
            } else {
                banOnNodesListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
            }
        } else {
            logger.trace("task [{}] doesn't have any children that should be cancelled", taskId);
            if (waitForCompletion) {
                taskManager.cancel(task, reason, () -> listener.onResponse(null));
            } else {
                taskManager.cancel(task, reason, () -> {});
                listener.onResponse(null);
            }
        }
    }

    private void setBanOnNodes(
        String reason,
        boolean waitForCompletion,
        CancellableTask task,
        Collection<DiscoveryNode> childNodes,
        ActionListener<Void> listener
    ) {
        if (childNodes.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final TaskId taskId = new TaskId(localNodeId(), task.getId());
        logger.trace("cancelling child tasks of [{}] on child nodes {}", taskId, childNodes);
        GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(
            ActionListener.map(listener, r -> null),
            childNodes.size()
        );
        final BanParentTaskRequest banRequest = BanParentTaskRequest.createSetBanParentTaskRequest(taskId, reason, waitForCompletion);
        for (DiscoveryNode node : childNodes) {
            transportService.sendRequest(
                node,
                BAN_PARENT_ACTION_NAME,
                banRequest,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        logger.trace("sent ban for tasks with the parent [{}] to the node [{}]", taskId, node);
                        groupedListener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        assert ExceptionsHelper.unwrapCause(exp) instanceof OpenSearchSecurityException == false;
                        logger.warn("Cannot send ban for tasks with the parent [{}] to the node [{}]", taskId, node);
                        groupedListener.onFailure(exp);
                    }
                }
            );
        }
    }

    private void removeBanOnNodes(CancellableTask task, Collection<DiscoveryNode> childNodes) {
        final BanParentTaskRequest request = BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(localNodeId(), task.getId()));
        for (DiscoveryNode node : childNodes) {
            logger.trace("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node);
            transportService.sendRequest(node, BAN_PARENT_ACTION_NAME, request, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleException(TransportException exp) {
                    assert ExceptionsHelper.unwrapCause(exp) instanceof OpenSearchSecurityException == false;
                    logger.info("failed to remove the parent ban for task {} on node {}", request.parentTaskId, node);
                }
            });
        }
    }

    private static class BanParentTaskRequest extends TransportRequest {

        private final TaskId parentTaskId;
        private final boolean ban;
        private final boolean waitForCompletion;
        private final String reason;

        static BanParentTaskRequest createSetBanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
            return new BanParentTaskRequest(parentTaskId, reason, waitForCompletion);
        }

        static BanParentTaskRequest createRemoveBanParentTaskRequest(TaskId parentTaskId) {
            return new BanParentTaskRequest(parentTaskId);
        }

        private BanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
            this.parentTaskId = parentTaskId;
            this.ban = true;
            this.reason = reason;
            this.waitForCompletion = waitForCompletion;
        }

        private BanParentTaskRequest(TaskId parentTaskId) {
            this.parentTaskId = parentTaskId;
            this.ban = false;
            this.reason = null;
            this.waitForCompletion = false;
        }

        private BanParentTaskRequest(StreamInput in) throws IOException {
            super(in);
            parentTaskId = TaskId.readFromStream(in);
            ban = in.readBoolean();
            reason = ban ? in.readString() : null;
            waitForCompletion = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            parentTaskId.writeTo(out);
            out.writeBoolean(ban);
            if (ban) {
                out.writeString(reason);
            }
            out.writeBoolean(waitForCompletion);
        }
    }

    private class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            if (request.ban) {
                logger.debug(
                    "Received ban for the parent [{}] on the node [{}], reason: [{}]",
                    request.parentTaskId,
                    localNodeId(),
                    request.reason
                );
                final List<CancellableTask> childTasks = taskManager.setBan(request.parentTaskId, request.reason);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                    ActionListener.map(
                        new ChannelActionListener<>(channel, BAN_PARENT_ACTION_NAME, request),
                        r -> TransportResponse.Empty.INSTANCE
                    ),
                    childTasks.size() + 1
                );
                for (CancellableTask childTask : childTasks) {
                    cancelTaskAndDescendants(childTask, request.reason, request.waitForCompletion, listener);
                }
                listener.onResponse(null);
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId, localNodeId());
                taskManager.removeBan(request.parentTaskId);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }
}
