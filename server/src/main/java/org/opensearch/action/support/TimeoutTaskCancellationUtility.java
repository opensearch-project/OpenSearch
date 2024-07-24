/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.client.OriginSettingClient;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchService;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.action.admin.cluster.node.tasks.get.GetTaskAction.TASKS_ORIGIN;
import static org.opensearch.action.search.TransportSearchAction.SEARCH_CANCEL_AFTER_TIME_INTERVAL_SETTING;

/**
 * Utility to cancel a timeout task
 *
 * @opensearch.internal
 */
public class TimeoutTaskCancellationUtility {

    private static final Logger logger = LogManager.getLogger(TimeoutTaskCancellationUtility.class);

    /**
     * Wraps a listener with a timeout listener {@link TimeoutRunnableListener} to schedule the task cancellation for provided tasks on
     * generic thread pool
     * @param client - {@link NodeClient}
     * @param taskToCancel - task to schedule cancellation for
     * @param timeout - {@link TimeValue}
     * @param listener - original listener associated with the task
     * @return wrapped listener
     */
    public static <Response> ActionListener<Response> wrapWithCancellationListener(
        NodeClient client,
        CancellableTask taskToCancel,
        TimeValue timeout,
        ActionListener<Response> listener
    ) {
        final TimeValue timeoutInterval = (taskToCancel.getCancellationTimeout() == null)
            ? timeout
            : taskToCancel.getCancellationTimeout();
        // Note: -1 (or no timeout) will help to turn off cancellation. The combinations will be request level set at -1 or request level
        // set to null and cluster level set to -1.
        ActionListener<Response> listenerToReturn = listener;
        if (timeoutInterval.equals(SearchService.NO_TIMEOUT)) {
            return listenerToReturn;
        }

        try {
            final TimeoutRunnableListener<Response> wrappedListener = new TimeoutRunnableListener<>(timeoutInterval, listener, () -> {
                final CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                cancelTasksRequest.setTaskId(new TaskId(client.getLocalNodeId(), taskToCancel.getId()));
                cancelTasksRequest.setReason("Cancellation timeout of " + timeoutInterval + " is expired");
                // force the origin to execute the cancellation as a system user
                new OriginSettingClient(client, TASKS_ORIGIN).admin()
                    .cluster()
                    .cancelTasks(
                        cancelTasksRequest,
                        ActionListener.wrap(
                            r -> logger.debug(
                                "Scheduled cancel task with timeout: {} for original task: {} is successfully completed",
                                timeoutInterval,
                                cancelTasksRequest.getTaskId()
                            ),
                            e -> logger.error(
                                new ParameterizedMessage(
                                    "Scheduled cancel task with timeout: {} for original task: {} is failed",
                                    timeoutInterval,
                                    cancelTasksRequest.getTaskId()
                                ),
                                e
                            )
                        )
                    );
            });
            wrappedListener.cancellable = client.threadPool().schedule(wrappedListener, timeoutInterval, ThreadPool.Names.GENERIC);
            listenerToReturn = wrappedListener;
        } catch (Exception ex) {
            // if there is any exception in scheduling the cancellation task then continue without it
            logger.warn("Failed to schedule the cancellation task for original task: {}, will continue without it", taskToCancel.getId());
        }
        return listenerToReturn;
    }

    /**
     * Timeout listener which executes the provided runnable after timeout is expired and if a response/failure is not yet received.
     * If either a response/failure is received before timeout then the scheduled task is cancelled and response/failure is sent back to
     * the original listener.
     *
     * @opensearch.internal
     */
    private static class TimeoutRunnableListener<Response> implements ActionListener<Response>, Runnable {

        private static final Logger logger = LogManager.getLogger(TimeoutRunnableListener.class);

        // Runnable to execute after timeout
        private final TimeValue timeout;
        private final ActionListener<Response> originalListener;
        private final Runnable timeoutRunnable;
        private final AtomicBoolean executeRunnable = new AtomicBoolean(true);
        private volatile Scheduler.ScheduledCancellable cancellable;
        private final long creationTime;

        TimeoutRunnableListener(TimeValue timeout, ActionListener<Response> listener, Runnable runAfterTimeout) {
            this.timeout = timeout;
            this.originalListener = listener;
            this.timeoutRunnable = runAfterTimeout;
            this.creationTime = System.nanoTime();
        }

        @Override
        public void onResponse(Response response) {
            checkAndCancel();
            originalListener.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            checkAndCancel();
            originalListener.onFailure(e);
        }

        @Override
        public void run() {
            try {
                if (executeRunnable.compareAndSet(true, false)) {
                    timeoutRunnable.run();
                } // else do nothing since either response/failure is already sent to client
            } catch (Exception ex) {
                // ignore the exception
                logger.error(
                    new ParameterizedMessage(
                        "Ignoring the failure to run the provided runnable after timeout of {} with " + "exception",
                        timeout
                    ),
                    ex
                );
            }
        }

        private void checkAndCancel() {
            if (executeRunnable.compareAndSet(true, false)) {
                logger.debug(
                    "Aborting the scheduled cancel task after {}",
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - creationTime)
                );
                // timer has not yet expired so cancel it
                cancellable.cancel();
            }
        }
    }
}
