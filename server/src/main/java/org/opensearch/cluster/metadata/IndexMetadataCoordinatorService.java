/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.*;
import org.opensearch.cluster.coordination.IndexMetadataStatePublisher;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterManagerService;
import org.opensearch.cluster.service.NoOpTaskBatcherListener;
import org.opensearch.cluster.service.TaskBatcher;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.core.Assertions;
import org.opensearch.discovery.Discovery;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service for coordinating index metadata updates without cluster state publication.
 * Similar to ClusterService.submitStateUpdateTask but skips the publish/commit phases.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class IndexMetadataCoordinatorService extends AbstractLifecycleComponent {

    private static final Logger log = LogManager.getLogger(IndexMetadataCoordinatorService.class);
    private final ThreadPool threadPool;
    private volatile PrioritizedOpenSearchThreadPoolExecutor threadPoolExecutor;
    private volatile IndexMetadataTaskBatcher taskBatcher;

    private java.util.function.Supplier<ClusterState> clusterStateSupplier;
    private java.util.function.Supplier<Integer> indexMetadataStateVersionSupplier;

    IndexMetadataStatePublisher indexMetadataStatePublisher;

    public IndexMetadataCoordinatorService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    protected void doStart() {
        this.threadPoolExecutor = createThreadPoolExecutor();
        this.taskBatcher = new IndexMetadataTaskBatcher(threadPoolExecutor);
    }

    private PrioritizedOpenSearchThreadPoolExecutor createThreadPoolExecutor() {
        return OpenSearchExecutors.newSinglePrioritizing(
            "indexMetadataCoordinator",
            OpenSearchExecutors.daemonThreadFactory("indexMetadataCoordinator"),
            threadPool.getThreadContext(),
            threadPool.scheduler()
        );
    }

    /**
     * Submits a batch of index metadata update tasks without publishing to the cluster.
     */
    public <T> void submitIndexMetadataUpdateTasks(
        final String source,
        final Map<T, ClusterStateTaskListener> tasks,
        final ClusterStateTaskConfig config,
        final ClusterStateTaskExecutor<T> executor
    ) {
        List<IndexMetadataTaskBatcher.UpdateTask> safeTasks = tasks.entrySet()
            .stream()
            .map(e -> taskBatcher.new UpdateTask(config.priority(), source, e.getKey(), e.getValue(), executor))
            .collect(Collectors.toList());
        taskBatcher.submitTasks(safeTasks, config.timeout());
    }

    /**
     * The current cluster state exposed by the discovery layer. Package-visible for tests.
     */
    ClusterState state() {
        return clusterStateSupplier.get();
    }

    public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    public synchronized void setIndexMetadataStateVersionSupplier(java.util.function.Supplier<Integer> indexMetadataStateVersionSupplier) {
        this.indexMetadataStateVersionSupplier = indexMetadataStateVersionSupplier;
    }

    int indexMetadataStateVersion() {
        return indexMetadataStateVersionSupplier.get();
    }

    public synchronized void setIndexMetadataStatePublisher(IndexMetadataStatePublisher publisher) {
        indexMetadataStatePublisher = publisher;
    }

    class IndexMetadataTaskBatcher extends TaskBatcher {

        IndexMetadataTaskBatcher(PrioritizedOpenSearchThreadPoolExecutor threadExecutor) {
            super(LogManager.getLogger(IndexMetadataTaskBatcher.class), threadExecutor, new NoOpTaskBatcherListener());
        }

        @Override
        protected void onTimeout(List<? extends BatchedTask> tasks, org.opensearch.common.unit.TimeValue timeout) {
            tasks.forEach(task -> ((UpdateTask) task).listener.onFailure(
                task.source(),
                new ProcessClusterEventTimeoutException(timeout, task.source())
            ));
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, Function<Boolean, String> taskSummaryGenerator) {
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) batchingKey;
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;
            runTasks(new TaskInputs(taskExecutor, updateTasks, taskSummaryGenerator));
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

    /**
     * Represents a set of tasks to be processed together with their executor
     */
    public class TaskInputs {

        final List<IndexMetadataTaskBatcher.UpdateTask> updateTasks;
        final ClusterStateTaskExecutor<Object> executor;
        final Function<Boolean, String> taskSummaryGenerator;

        TaskInputs(
            ClusterStateTaskExecutor<Object> executor,
            List<IndexMetadataTaskBatcher.UpdateTask> updateTasks,
            final Function<Boolean, String> taskSummaryGenerator
        ) {
            this.executor = executor;
            this.updateTasks = updateTasks;
            this.taskSummaryGenerator = taskSummaryGenerator;
        }
    }

    class TaskOutputs {
        final TaskInputs taskInputs;
        final ClusterState previousClusterState;
        final ClusterState newClusterState;
        final List<IndexMetadataTaskBatcher.UpdateTask> nonFailedTasks;
        final Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults;

        TaskOutputs(
            TaskInputs taskInputs,
            ClusterState previousClusterState,
            ClusterState newClusterState,
            List<IndexMetadataTaskBatcher.UpdateTask> nonFailedTasks,
            Map<Object, ClusterStateTaskExecutor.TaskResult> executionResults
        ) {
            this.taskInputs = taskInputs;
            this.previousClusterState = previousClusterState;
            this.newClusterState = newClusterState;
            this.nonFailedTasks = nonFailedTasks;
            this.executionResults = executionResults;
        }

        void notifyFailedTasks() {
            // fail all tasks that have failed
            for (IndexMetadataTaskBatcher.UpdateTask updateTask : taskInputs.updateTasks) {
                assert executionResults.containsKey(updateTask.getTask()) : "missing " + updateTask;
                final ClusterStateTaskExecutor.TaskResult taskResult = executionResults.get(updateTask.getTask());
                if (taskResult.isSuccess() == false) {
                    updateTask.listener.onFailure(updateTask.source(), taskResult.getFailure());
                }
            }
        }

        boolean clusterStateUnchanged() {
            return previousClusterState == newClusterState;
        }

        void notifySuccessfulTasksOnUnchangedClusterState() {
            nonFailedTasks.forEach(task -> {
                if (task.listener instanceof AsyncClusterStateUpdateTask) {
                    // no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    ((AsyncClusterStateUpdateTask) task.listener).onRemoteAcked(null);
                }
                task.listener.clusterStateProcessed(task.source(), newClusterState, newClusterState);
            });
        }

        IndexMetadataStatePublisher.IndexMetadataUpdateAckListener createAckListener() {
            return new DelegatingIndexMetadataUpdateAckListener(
                nonFailedTasks.stream()
                    .filter(task -> task.listener instanceof AsyncClusterStateUpdateTask<?>)
                    .map(
                        task -> new AckCountDownListener((AsyncClusterStateUpdateTask) task.listener)
                    )
                    .collect(Collectors.toList())
            );
        }
    }

    private static class DelegatingIndexMetadataUpdateAckListener implements Discovery.IndexMetadataUpdateAckListener {

        private final List<Discovery.IndexMetadataUpdateAckListener> listeners;

        private DelegatingIndexMetadataUpdateAckListener(List<Discovery.IndexMetadataUpdateAckListener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onRemoteAck(Exception e) {
            for (Discovery.IndexMetadataUpdateAckListener listener : listeners) {
                listener.onRemoteAck(e);
            }
        }
    }

    private static class AckCountDownListener implements IndexMetadataStatePublisher.IndexMetadataUpdateAckListener {

        private final AsyncClusterStateUpdateTask ackedTaskListener;

        AckCountDownListener(
            AsyncClusterStateUpdateTask ackedTaskListener
        ) {
            this.ackedTaskListener = ackedTaskListener;
        }

        private void finish() {
            ackedTaskListener.onRemoteAcked(null);
        }

        @Override
        public void onRemoteAck(Exception e) {
            finish();
        }
    }

    private TimeValue getTimeSince(long startTimeNanos) {
        return TimeValue.timeValueMillis(TimeValue.nsecToMSec(threadPool.preciseRelativeTimeInNanos() - startTimeNanos));
    }


    private void runTasks(TaskInputs taskInputs) {
        final String summary;
        if (log.isTraceEnabled()) {
            summary = taskInputs.taskSummaryGenerator.apply(true);
        } else {
            summary = taskInputs.taskSummaryGenerator.apply(false);
        }

        if (!lifecycle.started()) {
            log.info("processing [{}]: ignoring, index metadata coordinator service not started", summary);
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("executing cluster state update for [{}]", summary);
        } else {
            log.debug("executing cluster state update for [{}]", summary);
        }

        final ClusterState previousClusterState = state();

        final long computationStartTime = threadPool.preciseRelativeTimeInNanos();
        final TaskOutputs taskOutputs = calculateTaskOutputs(taskInputs, previousClusterState, summary);
        taskOutputs.notifyFailedTasks();
        final TimeValue computationTime = getTimeSince(computationStartTime);
        log.info("took [{}] to {} for [{}]", computationTime, "compute cluster state update", summary);

        if (taskOutputs.clusterStateUnchanged()) {
            final long notificationStartTime = threadPool.preciseRelativeTimeInNanos();
            taskOutputs.notifySuccessfulTasksOnUnchangedClusterState();
            final TimeValue executionTime = getTimeSince(notificationStartTime);
            log.info("took [{}] to {} for [{}]", executionTime, "notify listeners on unchanged cluster state", summary);
        } else {
            final ClusterState newClusterState = taskOutputs.newClusterState;
            if (log.isTraceEnabled()) {
                log.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
            } else {
                log.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
            }
            final long publicationStartTime = threadPool.preciseRelativeTimeInNanos();
            try {
                ClusterChangedEvent clusterChangedEvent = new ClusterChangedEvent(summary, newClusterState, previousClusterState);
                log.info("publishing cluster state version [{}]", newClusterState.version());

                indexMetadataStatePublisher.publishIndexMetadata(clusterChangedEvent, indexMetadataStateVersion()+1, taskOutputs.createAckListener());
            } catch (Exception e) {
                log.warn("Failed");
            }
        }
    }

    private TaskOutputs calculateTaskOutputs(TaskInputs taskInputs, ClusterState previousClusterState, String taskSummary) {
        ClusterStateTaskExecutor.ClusterTasksResult<Object> clusterTasksResult = executeTasks(taskInputs, previousClusterState, taskSummary);
        return new TaskOutputs(
            taskInputs,
            previousClusterState,
            clusterTasksResult.resultingState,
            getNonFailedTasks(taskInputs, clusterTasksResult),
            clusterTasksResult.executionResults
        );
    }

    private List<IndexMetadataTaskBatcher.UpdateTask> getNonFailedTasks(TaskInputs taskInputs, ClusterStateTaskExecutor.ClusterTasksResult<Object> clusterTasksResult) {
        return taskInputs.updateTasks.stream().filter(updateTask -> {
            assert clusterTasksResult.executionResults.containsKey(updateTask.getTask()) : "missing " + updateTask;
            final ClusterStateTaskExecutor.TaskResult taskResult = clusterTasksResult.executionResults.get(updateTask.getTask());
            return taskResult.isSuccess();
        }).collect(Collectors.toList());
    }

    private ClusterStateTaskExecutor.ClusterTasksResult<Object> executeTasks(TaskInputs taskInputs, ClusterState previousClusterState, String taskSummary) {
        ClusterStateTaskExecutor.ClusterTasksResult<Object> clusterTasksResult;
        try {
            List<Object> inputs = taskInputs.updateTasks.stream().map(tUpdateTask -> tUpdateTask.getTask()).collect(Collectors.toList());
            clusterTasksResult = taskInputs.executor.execute(previousClusterState, inputs);
        } catch (Exception e) {
            log.trace(
                () -> new ParameterizedMessage(
                    "failed to execute cluster state update (on version: [{}], uuid: [{}]) for [{}]\n{}{}{}",
                    previousClusterState.version(),
                    previousClusterState.stateUUID(),
                    taskSummary,
                    previousClusterState.nodes(),
                    previousClusterState.routingTable(),
                    previousClusterState.getRoutingNodes()
                ), // may be expensive => construct message lazily
                e
            );
            clusterTasksResult = ClusterStateTaskExecutor.ClusterTasksResult.builder()
                .failures(taskInputs.updateTasks.stream().map(updateTask -> updateTask.getTask())::iterator, e)
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
            ClusterStateTaskExecutor.ClusterTasksResult<Object> finalClusterTasksResult = clusterTasksResult;
            taskInputs.updateTasks.forEach(updateTask -> {
                assert finalClusterTasksResult.executionResults.containsKey(updateTask.getTask()) : "missing task result for " + updateTask;
            });
        }

        return clusterTasksResult;
    }

    @Override
    protected synchronized void doStop() {
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {}

    /**
     * Listener for index metadata update tasks that provides access to the computed state
     * without waiting for cluster publication.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.0.0")
    public interface IndexMetadataUpdateListener<T> {
        /**
         * Called when the task execution completes successfully.
         * The newState contains the computed changes but is not yet published.
         */
        void onResponse(ClusterState newState);

        /**
         * Called when the task execution fails.
         */
        void onFailure(Exception e);
    }


}
