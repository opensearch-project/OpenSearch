/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ProtobufChannelActionListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtobufEmptyTransportResponseHandler;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.ProtobufTransportRequestHandler;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

/**
 * Service used to cancel a task
*
* @opensearch.internal
*/
public class ProtobufTaskCancellationService {
    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";
    private static final Logger logger = LogManager.getLogger(ProtobufTaskCancellationService.class);
    private final TransportService transportService;
    private final TaskManager taskManager;

    public ProtobufTaskCancellationService(TransportService transportService) {
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        transportService.registerRequestHandlerProtobuf(
            BAN_PARENT_ACTION_NAME,
            ThreadPool.Names.SAME,
            BanParentTaskRequest::new,
            new BanParentRequestHandler()
        );
    }

    private String localNodeId() {
        return transportService.getLocalNode().getId();
    }

    void cancelTaskAndDescendants(ProtobufCancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        final ProtobufTaskId taskId = task.taskInfo(localNodeId(), false).getTaskId();
        if (task.shouldCancelChildrenOnCancellation()) {
            logger.trace("cancelling task [{}] and its descendants", taskId);
            StepListener<Void> completedListener = new StepListener<>();
            GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(ActionListener.map(completedListener, r -> null), 3);
            Collection<DiscoveryNode> childrenNodes = taskManager.startBanOnChildrenNodesProtobuf(task.getId(), () -> {
                logger.trace("child tasks of parent [{}] are completed", taskId);
                groupedListener.onResponse(null);
            });
            taskManager.cancelProtobufTask(task, reason, () -> {
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
                taskManager.cancelProtobufTask(task, reason, () -> listener.onResponse(null));
            } else {
                taskManager.cancelProtobufTask(task, reason, () -> {});
                listener.onResponse(null);
            }
        }
    }

    private void setBanOnNodes(
        String reason,
        boolean waitForCompletion,
        ProtobufCancellableTask task,
        Collection<DiscoveryNode> childNodes,
        ActionListener<Void> listener
    ) {
        if (childNodes.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final ProtobufTaskId taskId = new ProtobufTaskId(localNodeId(), task.getId());
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
                new ProtobufEmptyTransportResponseHandler(ThreadPool.Names.SAME) {
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

    private void removeBanOnNodes(ProtobufCancellableTask task, Collection<DiscoveryNode> childNodes) {
        final BanParentTaskRequest request = BanParentTaskRequest.createRemoveBanParentTaskRequest(
            new ProtobufTaskId(localNodeId(), task.getId())
        );
        for (DiscoveryNode node : childNodes) {
            logger.trace("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node);
            transportService.sendRequest(
                node,
                BAN_PARENT_ACTION_NAME,
                request,
                new ProtobufEmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        assert ExceptionsHelper.unwrapCause(exp) instanceof OpenSearchSecurityException == false;
                        logger.info("failed to remove the parent ban for task {} on node {}", request.parentTaskId, node);
                    }
                }
            );
        }
    }

    private static class BanParentTaskRequest extends TransportRequest {

        private final ProtobufTaskId parentTaskId;
        private final boolean ban;
        private final boolean waitForCompletion;
        private final String reason;

        static BanParentTaskRequest createSetBanParentTaskRequest(ProtobufTaskId parentTaskId, String reason, boolean waitForCompletion) {
            return new BanParentTaskRequest(parentTaskId, reason, waitForCompletion);
        }

        static BanParentTaskRequest createRemoveBanParentTaskRequest(ProtobufTaskId parentTaskId) {
            return new BanParentTaskRequest(parentTaskId);
        }

        private BanParentTaskRequest(ProtobufTaskId parentTaskId, String reason, boolean waitForCompletion) {
            this.parentTaskId = parentTaskId;
            this.ban = true;
            this.reason = reason;
            this.waitForCompletion = waitForCompletion;
        }

        private BanParentTaskRequest(ProtobufTaskId parentTaskId) {
            this.parentTaskId = parentTaskId;
            this.ban = false;
            this.reason = null;
            this.waitForCompletion = false;
        }

        private BanParentTaskRequest(byte[] in) throws IOException {
            super(in);
            parentTaskId = null;
            ban = false;
            reason = null;
            waitForCompletion = false;
        }

        @Override
        public void writeTo(OutputStream out) throws IOException {
            super.writeTo(out);
        }
    }

    private class BanParentRequestHandler implements ProtobufTransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, ProtobufTask task)
            throws Exception {
            if (request.ban) {
                logger.debug(
                    "Received ban for the parent [{}] on the node [{}], reason: [{}]",
                    request.parentTaskId,
                    localNodeId(),
                    request.reason
                );
                final List<ProtobufCancellableTask> childTasks = taskManager.setBanProtobuf(request.parentTaskId, request.reason);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                    ActionListener.map(
                        new ProtobufChannelActionListener<>(channel, BAN_PARENT_ACTION_NAME, request),
                        r -> TransportResponse.Empty.INSTANCE
                    ),
                    childTasks.size() + 1
                );
                for (ProtobufCancellableTask childTask : childTasks) {
                    cancelTaskAndDescendants(childTask, request.reason, request.waitForCompletion, listener);
                }
                listener.onResponse(null);
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId, localNodeId());
                taskManager.removeBanProtobuf(request.parentTaskId);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }
}
