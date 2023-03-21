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

package org.opensearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Assertions;
import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.AckedClusterStateTaskListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterState.Builder;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.coordination.ClusterStatePublisher;
import org.opensearch.cluster.coordination.FailedToCommitClusterStateException;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.text.Text;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.discovery.Discovery;
import org.opensearch.node.Node;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.common.util.concurrent.OpenSearchExecutors.daemonThreadFactory;

/**
 * Main Master Node Service
 *
 * @opensearch.internal
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link ClusterManagerService}.
 */
@Deprecated
public class MasterService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(MasterService.class);

    public static final Setting<TimeValue> MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "cluster.service.slow_master_task_logging_threshold",
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    // The setting below is going to replace the above.
    // To keep backwards compatibility, the old usage is remained, and it's also used as the fallback for the new usage.
    public static final Setting<TimeValue> CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "cluster.service.slow_cluster_manager_task_logging_threshold",
        MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    static final String CLUSTER_MANAGER_UPDATE_THREAD_NAME = "clusterManagerService#updateTask";

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #CLUSTER_MANAGER_UPDATE_THREAD_NAME} */
    @Deprecated
    static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    ClusterStatePublisher clusterStatePublisher;

    private final String nodeName;

    private java.util.function.Supplier<ClusterState> clusterStateSupplier;

    private volatile TimeValue slowTaskLoggingThreshold;

    protected final ThreadPool threadPool;

    private volatile PrioritizedOpenSearchThreadPoolExecutor threadPoolExecutor;
    private volatile Batcher taskBatcher;
    protected final ClusterManagerTaskThrottler clusterManagerTaskThrottler;
    private final ClusterManagerThrottlingStats throttlingStats;

    public MasterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));

        this.slowTaskLoggingThreshold = CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_MANAGER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
            this::setSlowTaskLoggingThreshold
        );

        this.throttlingStats = new ClusterManagerThrottlingStats();
        this.clusterManagerTaskThrottler = new ClusterManagerTaskThrottler(
            settings,
            clusterSettings,
            this::getMinNodeVersion,
            throttlingStats
        );
        this.threadPool = threadPool;
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(ClusterStatePublisher publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
        threadPoolExecutor = createThreadPoolExecutor();
        taskBatcher = new Batcher(logger, threadPoolExecutor, clusterManagerTaskThrottler);
    }

    protected PrioritizedOpenSearchThreadPoolExecutor createThreadPoolExecutor() {
        return OpenSearchExecutors.newSinglePrioritizing(
            nodeName + "/" + CLUSTER_MANAGER_UPDATE_THREAD_NAME,
            daemonThreadFactory(nodeName, CLUSTER_MANAGER_UPDATE_THREAD_NAME),
            threadPool.getThreadContext(),
            threadPool.scheduler()
        );
    }

    @SuppressWarnings("unchecked")
    class Batcher extends TaskBatcher {

        Batcher(Logger logger, PrioritizedOpenSearchThreadPoolExecutor threadExecutor, TaskBatcherListener taskBatcherListener) {
            super(logger, threadExecutor, taskBatcherListener);
        }

        @Override
        protected void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout) {
            threadPool.generic()
                .execute(
                    () -> tasks.forEach(
                        task -> ((UpdateTask) task).listener.onFailure(
                            task.source,
                            new ProcessClusterEventTimeoutException(timeout, task.source)
                        )
                    )
                );
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary) {
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) batchingKey;
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;
            runTasks(new TaskInputs(taskExecutor, updateTasks, tasksSummary));
        }

        class UpdateTask extends BatchedTask {
            final ClusterStateTaskListener listener;

            UpdateTask(
                Priority priority,
                String source,
                Object task,
                ClusterStateTaskListener listener,
                ClusterStateTaskExecutor<?> executor
            ) {
                super(priority, source, executor, task);
                this.listener = listener;
            }

            @Override
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((ClusterStateTaskExecutor<Object>) batchingKey).describeTasks(
                    tasks.stream().map(BatchedTask::getTask).collect(Collectors.toList())
                );
            }
        }
    }

    @Override
    protected synchronized void doStop() {
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {}

    /**
     * The current cluster state exposed by the discovery layer. Package-visible for tests.
     */
    ClusterState state() {
        return clusterStateSupplier.get();
    }

    private static boolean isClusterManagerUpdateThread() {
        return Thread.currentThread().getName().contains(CLUSTER_MANAGER_UPDATE_THREAD_NAME)
            || Thread.currentThread().getName().contains(MASTER_UPDATE_THREAD_NAME);
    }

    public static boolean assertClusterManagerUpdateThread() {
        assert isClusterManagerUpdateThread() : "not called from the cluster-manager service thread";
        return true;
    }

    public static boolean assertNotClusterManagerUpdateThread(String reason) {
        assert isClusterManagerUpdateThread() == false : "Expected current thread ["
            + Thread.currentThread()
            + "] to not be the cluster-manager service thread. Reason: ["
            + reason
            + "]";
        return true;
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #assertClusterManagerUpdateThread()} */
    @Deprecated
    public static boolean assertMasterUpdateThread() {
        return assertClusterManagerUpdateThread();
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #assertNotClusterManagerUpdateThread(String)} */
    @Deprecated
    public static boolean assertNotMasterUpdateThread(String reason) {
        return assertNotClusterManagerUpdateThread(reason);
    }

    private void runTasks(TaskInputs taskInputs) {
        final String summary = taskInputs.summary;
        if (!lifecycle.started()) {
            logger.debug("processing [{}]: ignoring, cluster-manager service not started", summary);
            return;
        }

        logger.debug("executing cluster state update for [{}]", summary);
        final ClusterState previousClusterState = state();

        if (!previousClusterState.nodes().isLocalNodeElectedClusterManager() && taskInputs.runOnlyWhenClusterManager()) {
            logger.debug("failing [{}]: local node is no longer cluster-manager", summary);
            taskInputs.onNoLongerClusterManager();
            return;
        }

        final long computationStartTime = threadPool.relativeTimeInMillis();
        final TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState);
        taskOutputs.notifyFailedTasks();
        final TimeValue computationTime = getTimeSince(computationStartTime);
        logExecutionTime(computationTime, "compute cluster state update", summary);

        if (taskOutputs.clusterStateUnchanged()) {
            final long notificationStartTime = threadPool.relativeTimeInMillis();
            taskOutputs.notifySuccessfulTasksOnUnchangedClusterState();
            final TimeValue executionTime = getTimeSince(notificationStartTime);
            logExecutionTime(executionTime, "notify listeners on unchanged cluster state", summary);
        } else {
            final ClusterState newClusterState = taskOutputs.newClusterState;
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
            } else {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
            }
            final long publicationStartTime = threadPool.relativeTimeInMillis();
            try {
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(summary, newClusterState, previousClusterState);
                // new cluster state, notify all listeners
                final DiscoveryNodes.Delta nodesDelta = clusterChangedEvent.nodesDelta();
                if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                    String nodesDeltaSummary = nodesDelta.shortSummary();
                    if (nodesDeltaSummary.length() > 0) {
                        logger.info(
                            "{}, term: {}, version: {}, delta: {}",
                            summary,
                            newClusterState.term(),
                            newClusterState.version(),
                            nodesDeltaSummary
                        );
                    }
                }

                logger.debug("publishing cluster state version [{}]", newClusterState.version());
                publish(clusterChangedEvent, taskOutputs, publicationStartTime);
            } catch (Exception e) {
                handleException(summary, publicationStartTime, newClusterState, e);
            }
        }
    }

    private TimeValue getTimeSince(long startTimeMillis) {
        return TimeValue.timeValueMillis(Math.max(0, threadPool.relativeTimeInMillis() - startTimeMillis));
    }

    protected void publish(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis) {
        final PlainActionFuture<Void> fut = new PlainActionFuture<Void>() {
            @Override
            protected boolean blockingAllowed() {
                return isClusterManagerUpdateThread() || super.blockingAllowed();
            }
        };
        clusterStatePublisher.publish(clusterChangedEvent, fut, taskOutputs.createAckListener(threadPool, clusterChangedEvent.state()));

        // indefinitely wait for publication to complete
        try {
            FutureUtils.get(fut);
            onPublicationSuccess(clusterChangedEvent, taskOutputs);
        } catch (Exception e) {
            onPublicationFailed(clusterChangedEvent, taskOutputs, startTimeMillis, e);
        }
    }

    void onPublicationSuccess(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs) {
        final long notificationStartTime = threadPool.relativeTimeInMillis();
        taskOutputs.processedDifferentClusterState(clusterChangedEvent.previousState(), clusterChangedEvent.state());

        try {
            taskOutputs.clusterStatePublished(clusterChangedEvent);
        } catch (Exception e) {
            logger.error(
                () -> new ParameterizedMessage(
                    "exception thrown while notifying executor of new cluster state publication [{}]",
                    clusterChangedEvent.source()
                ),
                e
            );
        }
        final TimeValue executionTime = getTimeSince(notificationStartTime);
        logExecutionTime(
            executionTime,
            "notify listeners on successful publication of cluster state (version: "
                + clusterChangedEvent.state().version()
                + ", uuid: "
                + clusterChangedEvent.state().stateUUID()
                + ')',
            clusterChangedEvent.source()
        );
    }

    void onPublicationFailed(ClusterChangedEvent clusterChangedEvent, TaskOutputs taskOutputs, long startTimeMillis, Exception exception) {
        if (exception instanceof FailedToCommitClusterStateException) {
            final long version = clusterChangedEvent.state().version();
            logger.warn(
                () -> new ParameterizedMessage(
                    "failing [{}]: failed to commit cluster state version [{}]",
                    clusterChangedEvent.source(),
                    version
                ),
                exception
            );
            taskOutputs.publishingFailed((FailedToCommitClusterStateException) exception);
        } else {
            handleException(clusterChangedEvent.source(), startTimeMillis, clusterChangedEvent.state(), exception);
        }
    }

    private void handleException(String summary, long startTimeMillis, ClusterState newClusterState, Exception e) {
        final TimeValue executionTime = getTimeSince(startTimeMillis);
        final long version = newClusterState.version();
        final String stateUUID = newClusterState.stateUUID();
        final String fullState = newClusterState.toString();
        logger.warn(
            new ParameterizedMessage(
                "took [{}] and then failed to publish updated cluster state (version: {}, uuid: {}) for [{}]:\n{}",
                executionTime,
                version,
                stateUUID,
                summary,
                fullState
            ),
            e
        );
        // TODO: do we want to call updateTask.onFailure here?
    }

    private TaskOutputs calculateTaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState) {
        ClusterTasksResult<Object> clusterTasksResult = executeTasks(taskInputs, previousClusterState);
        ClusterState newClusterState = patchVersions(previousClusterState, clusterTasksResult);
        return new TaskOutputs(
            taskInputs,
            previousClusterState,
            newClusterState,
            getNonFailedTasks(taskInputs, clusterTasksResult),
            clusterTasksResult.executionResults
        );
    }

    private ClusterState patchVersions(ClusterState previousClusterState, ClusterTasksResult<?> executionResult) {
        ClusterState newClusterState = executionResult.resultingState;

        if (previousClusterState != newClusterState) {
            // only the cluster-manager controls the version numbers
            Builder builder = incrementVersion(newClusterState);
            if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                builder.routingTable(
                    RoutingTable.builder(newClusterState.routingTable()).version(newClusterState.routingTable().version() + 1).build()
                );
            }
            if (previousClusterState.metadata() != newClusterState.metadata()) {
                builder.metadata(Metadata.builder(newClusterState.metadata()).version(newClusterState.metadata().version() + 1));
            }

            newClusterState = builder.build();
        }

        return newClusterState;
    }

    public Builder incrementVersion(ClusterState clusterState) {
        return ClusterState.builder(clusterState).incrementVersion();
    }

    /**
     * Submits a cluster state update task; unlike {@link #submitStateUpdateTask(String, Object, ClusterStateTaskConfig,
     * ClusterStateTaskExecutor, ClusterStateTaskListener)}, submitted updates will not be batched.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     *                   task
     *
     */
    public <T extends ClusterStateTaskConfig & ClusterStateTaskExecutor<T> & ClusterStateTaskListener> void submitStateUpdateTask(
        String source,
        T updateTask
    ) {
        submitStateUpdateTask(source, updateTask, updateTask, updateTask, updateTask);
    }

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param listener callback after the cluster state update task
     *                 completes
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTask(
        String source,
        T task,
        ClusterStateTaskConfig config,
        ClusterStateTaskExecutor<T> executor,
        ClusterStateTaskListener listener
    ) {
        submitStateUpdateTasks(source, Collections.singletonMap(task, listener), config, executor);
    }

    /**
     * Output created by executing a set of tasks provided as TaskInputs
     */
    class TaskOutputs {
        final TaskInputs taskInputs;
        final ClusterState previousClusterState;
        final ClusterState newClusterState;
        final List<Batcher.UpdateTask> nonFailedTasks;
        final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

        TaskOutputs(
            TaskInputs taskInputs,
            ClusterState previousClusterState,
            ClusterState newClusterState,
            List<Batcher.UpdateTask> nonFailedTasks,
            Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults
        ) {
            this.taskInputs = taskInputs;
            this.previousClusterState = previousClusterState;
            this.newClusterState = newClusterState;
            this.nonFailedTasks = nonFailedTasks;
            this.executionResults = executionResults;
        }

        void publishingFailed(FailedToCommitClusterStateException t) {
            nonFailedTasks.forEach(task -> task.listener.onFailure(task.source(), t));
        }

        void processedDifferentClusterState(ClusterState previousClusterState, ClusterState newClusterState) {
            nonFailedTasks.forEach(task -> task.listener.clusterStateProcessed(task.source(), previousClusterState, newClusterState));
        }

        void clusterStatePublished(ClusterChangedEvent clusterChangedEvent) {
            taskInputs.executor.clusterStatePublished(clusterChangedEvent);
        }

        Discovery.AckListener createAckListener(ThreadPool threadPool, ClusterState newClusterState) {
            return new DelegatingAckListener(
                nonFailedTasks.stream()
                    .filter(task -> task.listener instanceof AckedClusterStateTaskListener)
                    .map(
                        task -> new AckCountDownListener(
                            (AckedClusterStateTaskListener) task.listener,
                            newClusterState.version(),
                            newClusterState.nodes(),
                            threadPool
                        )
                    )
                    .collect(Collectors.toList())
            );
        }

        boolean clusterStateUnchanged() {
            return previousClusterState == newClusterState;
        }

        void notifyFailedTasks() {
            // fail all tasks that have failed
            for (Batcher.UpdateTask updateTask : taskInputs.updateTasks) {
                assert executionResults.containsKey(updateTask.task) : "missing " + updateTask;
                final ClusterStateTaskExecutor.TaskResult taskResult = executionResults.get(updateTask.task);
                if (taskResult.isSuccess() == false) {
                    updateTask.listener.onFailure(updateTask.source(), taskResult.getFailure());
                }
            }
        }

        void notifySuccessfulTasksOnUnchangedClusterState() {
            nonFailedTasks.forEach(task -> {
                if (task.listener instanceof AckedClusterStateTaskListener) {
                    // no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AckedClusterStateTaskListener) task.listener).onAllNodesAcked(null);
                }
                task.listener.clusterStateProcessed(task.source(), newClusterState, newClusterState);
            });
        }
    }

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        return Arrays.stream(threadPoolExecutor.getPending()).map(pending -> {
            assert pending.task instanceof SourcePrioritizedRunnable
                : "thread pool executor should only use SourcePrioritizedRunnable instances but found: "
                    + pending.task.getClass().getName();
            SourcePrioritizedRunnable task = (SourcePrioritizedRunnable) pending.task;
            return new PendingClusterTask(
                pending.insertionOrder,
                pending.priority,
                new Text(task.source()),
                task.getAgeInMillis(),
                pending.executing
            );
        }).collect(Collectors.toList());
    }

    /**
     * Returns the number of throttled pending tasks.
     */
    public long numberOfThrottledPendingTasks() {
        return throttlingStats.getTotalThrottledTaskCount();
    }

    /**
     * Returns the stats of throttled pending tasks.
     */
    public ClusterManagerThrottlingStats getThrottlingStats() {
        return throttlingStats;
    }

    /**
     * Returns the min version of nodes in cluster
     */
    public Version getMinNodeVersion() {
        return state().getNodes().getMinNodeVersion();
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return threadPoolExecutor.getNumberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return threadPoolExecutor.getMaxTaskWaitTime();
    }

    private SafeClusterStateTaskListener safe(ClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> contextSupplier) {
        if (listener instanceof AckedClusterStateTaskListener) {
            return new SafeAckedClusterStateTaskListener((AckedClusterStateTaskListener) listener, contextSupplier, logger);
        } else {
            return new SafeClusterStateTaskListener(listener, contextSupplier, logger);
        }
    }

    private static class SafeClusterStateTaskListener implements ClusterStateTaskListener {
        private final ClusterStateTaskListener listener;
        protected final Supplier<ThreadContext.StoredContext> context;
        private final Logger logger;

        SafeClusterStateTaskListener(ClusterStateTaskListener listener, Supplier<ThreadContext.StoredContext> context, Logger logger) {
            this.listener = listener;
            this.context = context;
            this.logger = logger;
        }

        @Override
        public void onFailure(String source, Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onFailure(source, e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error(() -> new ParameterizedMessage("exception thrown by listener notifying of failure from [{}]", source), inner);
            }
        }

        @Override
        public void onNoLongerClusterManager(String source) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onNoLongerClusterManager(source);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "exception thrown by listener while notifying no longer cluster-manager from [{}]",
                        source
                    ),
                    e
                );
            }
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.clusterStateProcessed(source, oldState, newState);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "exception thrown by listener while notifying of cluster state processed from [{}], old cluster state:\n"
                            + "{}\nnew cluster state:\n{}",
                        source,
                        oldState,
                        newState
                    ),
                    e
                );
            }
        }
    }

    private static class SafeAckedClusterStateTaskListener extends SafeClusterStateTaskListener implements AckedClusterStateTaskListener {
        private final AckedClusterStateTaskListener listener;
        private final Logger logger;

        SafeAckedClusterStateTaskListener(
            AckedClusterStateTaskListener listener,
            Supplier<ThreadContext.StoredContext> context,
            Logger logger
        ) {
            super(listener, context, logger);
            this.listener = listener;
            this.logger = logger;
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        @Override
        public void onAllNodesAcked(@Nullable Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onAllNodesAcked(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        @Override
        public void onAckTimeout() {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onAckTimeout();
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying on ack timeout", e);
            }
        }

        @Override
        public TimeValue ackTimeout() {
            return listener.ackTimeout();
        }
    }

    private void logExecutionTime(TimeValue executionTime, String activity, String summary) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn("took [{}], which is over [{}], to {} for [{}]", executionTime, slowTaskLoggingThreshold, activity, summary);
        } else {
            logger.debug("took [{}] to {} for [{}]", executionTime, activity, summary);
        }
    }

    private static class DelegatingAckListener implements Discovery.AckListener {

        private final List<Discovery.AckListener> listeners;

        private DelegatingAckListener(List<Discovery.AckListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onCommit(TimeValue commitTime) {
            for (Discovery.AckListener listener : listeners) {
                listener.onCommit(commitTime);
            }
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            for (Discovery.AckListener listener : listeners) {
                listener.onNodeAck(node, e);
            }
        }
    }

    private static class AckCountDownListener implements Discovery.AckListener {

        private static final Logger logger = LogManager.getLogger(AckCountDownListener.class);

        private final AckedClusterStateTaskListener ackedTaskListener;
        private final CountDown countDown;
        private final DiscoveryNode clusterManagerNode;
        private final ThreadPool threadPool;
        private final long clusterStateVersion;
        private volatile Scheduler.Cancellable ackTimeoutCallback;
        private Exception lastFailure;

        AckCountDownListener(
            AckedClusterStateTaskListener ackedTaskListener,
            long clusterStateVersion,
            DiscoveryNodes nodes,
            ThreadPool threadPool
        ) {
            this.ackedTaskListener = ackedTaskListener;
            this.clusterStateVersion = clusterStateVersion;
            this.threadPool = threadPool;
            this.clusterManagerNode = nodes.getClusterManagerNode();
            int countDown = 0;
            for (DiscoveryNode node : nodes) {
                // we always wait for at least the cluster-manager node
                if (node.equals(clusterManagerNode) || ackedTaskListener.mustAck(node)) {
                    countDown++;
                }
            }
            logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
            this.countDown = new CountDown(countDown + 1); // we also wait for onCommit to be called
        }

        @Override
        public void onCommit(TimeValue commitTime) {
            TimeValue ackTimeout = ackedTaskListener.ackTimeout();
            if (ackTimeout == null) {
                ackTimeout = TimeValue.ZERO;
            }
            final TimeValue timeLeft = TimeValue.timeValueNanos(Math.max(0, ackTimeout.nanos() - commitTime.nanos()));
            if (timeLeft.nanos() == 0L) {
                onTimeout();
            } else if (countDown.countDown()) {
                finish();
            } else {
                this.ackTimeoutCallback = threadPool.schedule(this::onTimeout, timeLeft, ThreadPool.Names.GENERIC);
                // re-check if onNodeAck has not completed while we were scheduling the timeout
                if (countDown.isCountedDown()) {
                    ackTimeoutCallback.cancel();
                }
            }
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (node.equals(clusterManagerNode) == false && ackedTaskListener.mustAck(node) == false) {
                return;
            }
            if (e == null) {
                logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
            } else {
                this.lastFailure = e;
                logger.debug(
                    () -> new ParameterizedMessage(
                        "ack received from node [{}], cluster_state update (version: {})",
                        node,
                        clusterStateVersion
                    ),
                    e
                );
            }

            if (countDown.countDown()) {
                finish();
            }
        }

        private void finish() {
            logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
            if (ackTimeoutCallback != null) {
                ackTimeoutCallback.cancel();
            }
            ackedTaskListener.onAllNodesAcked(lastFailure);
        }

        public void onTimeout() {
            if (countDown.fastForward()) {
                logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
                ackedTaskListener.onAckTimeout();
            }
        }
    }

    private ClusterTasksResult<Object> executeTasks(TaskInputs taskInputs, ClusterState previousClusterState) {
        ClusterTasksResult<Object> clusterTasksResult;
        try {
            List<Object> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.task).collect(Collectors.toList());
            clusterTasksResult = taskInputs.executor.execute(previousClusterState, inputs);
            if (previousClusterState != clusterTasksResult.resultingState
                && previousClusterState.nodes().isLocalNodeElectedClusterManager()
                && (clusterTasksResult.resultingState.nodes().isLocalNodeElectedClusterManager() == false)) {
                throw new AssertionError("update task submitted to ClusterManagerService cannot remove cluster-manager");
            }
        } catch (Exception e) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "failed to execute cluster state update (on version: [{}], uuid: [{}]) for [{}]\n{}{}{}",
                    previousClusterState.version(),
                    previousClusterState.stateUUID(),
                    taskInputs.summary,
                    previousClusterState.nodes(),
                    previousClusterState.routingTable(),
                    previousClusterState.getRoutingNodes()
                ), // may be expensive => construct message lazily
                e
            );
            clusterTasksResult = ClusterTasksResult.builder()
                .failures(taskInputs.updateTasks.stream().map(updateTask -> updateTask.task)::iterator, e)
                .build(previousClusterState);
        }

        assert clusterTasksResult.executionResults != null;
        assert clusterTasksResult.executionResults.size() == taskInputs.updateTasks.size() : String.format(
            Locale.ROOT,
            "expected [%d] task result%s but was [%d]",
            taskInputs.updateTasks.size(),
            taskInputs.updateTasks.size() == 1 ? "" : "s",
            clusterTasksResult.executionResults.size()
        );
        if (Assertions.ENABLED) {
            ClusterTasksResult<Object> finalClusterTasksResult = clusterTasksResult;
            taskInputs.updateTasks.forEach(updateTask -> {
                assert finalClusterTasksResult.executionResults.containsKey(updateTask.task) : "missing task result for " + updateTask;
            });
        }

        return clusterTasksResult;
    }

    private List<Batcher.UpdateTask> getNonFailedTasks(TaskInputs taskInputs, ClusterTasksResult<Object> clusterTasksResult) {
        return taskInputs.updateTasks.stream().filter(updateTask -> {
            assert clusterTasksResult.executionResults.containsKey(updateTask.task) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult = clusterTasksResult.executionResults.get(updateTask.task);
            return taskResult.isSuccess();
        }).collect(Collectors.toList());
    }

    /**
     * Represents a set of tasks to be processed together with their executor
     */
    private class TaskInputs {
        final String summary;
        final List<Batcher.UpdateTask> updateTasks;
        final ClusterStateTaskExecutor<Object> executor;

        TaskInputs(ClusterStateTaskExecutor<Object> executor, List<Batcher.UpdateTask> updateTasks, String summary) {
            this.summary = summary;
            this.executor = executor;
            this.updateTasks = updateTasks;
        }

        boolean runOnlyWhenClusterManager() {
            return executor.runOnlyOnClusterManager();
        }

        void onNoLongerClusterManager() {
            updateTasks.forEach(task -> task.listener.onNoLongerClusterManager(task.source()));
        }
    }

    /**
     * Functionality for register task key to cluster manager node.
     *
     * @param taskKey - task key of task
     * @param throttlingEnabled - throttling is enabled for task or not i.e does data node perform retries on it or not
     * @return throttling task key which needs to be passed while submitting task to cluster manager
     */
    public ClusterManagerTaskThrottler.ThrottlingKey registerClusterManagerTask(String taskKey, boolean throttlingEnabled) {
        return clusterManagerTaskThrottler.registerClusterManagerTask(taskKey, throttlingEnabled);
    }

    /**
     * Submits a batch of cluster state update tasks; submitted updates are guaranteed to be processed together,
     * potentially with more tasks of the same executor.
     *
     * @param source   the source of the cluster state update task
     * @param tasks    a map of update tasks and their corresponding listeners
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T> void submitStateUpdateTasks(
        final String source,
        final Map<T, ClusterStateTaskListener> tasks,
        final ClusterStateTaskConfig config,
        final ClusterStateTaskExecutor<T> executor
    ) {
        if (!lifecycle.started()) {
            return;
        }
        final ThreadContext threadContext = threadPool.getThreadContext();
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();

            List<Batcher.UpdateTask> safeTasks = tasks.entrySet()
                .stream()
                .map(e -> taskBatcher.new UpdateTask(config.priority(), source, e.getKey(), safe(e.getValue(), supplier), executor))
                .collect(Collectors.toList());
            taskBatcher.submitTasks(safeTasks, config.timeout());
        } catch (OpenSearchRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (!lifecycle.stoppedOrClosed()) {
                throw e;
            }
        }
    }

}
