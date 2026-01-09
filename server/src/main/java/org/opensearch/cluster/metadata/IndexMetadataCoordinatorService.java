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
import org.opensearch.cluster.*;
import org.opensearch.cluster.coordination.IndexMetadataStatePublisher;
import org.opensearch.cluster.service.NoOpTaskBatcherListener;
import org.opensearch.cluster.service.TaskBatcher;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
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
        final Map<T, IndexMetadataUpdateListener<T>> tasks,
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
                new ProcessClusterEventTimeoutException(timeout, task.source())
            ));
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, Function<Boolean, String> taskSummaryGenerator) {
            ClusterStateTaskExecutor<Object> taskExecutor = (ClusterStateTaskExecutor<Object>) batchingKey;
            List<UpdateTask> updateTasks = (List<UpdateTask>) tasks;

            log.info("[IMC] Running batched tasks " + tasks.size());

            try {
                ClusterState currentState = state();
                ClusterStateTaskExecutor.ClusterTasksResult<Object> result = taskExecutor.execute(
                    currentState,
                    updateTasks.stream().map(BatchedTask::getTask).collect(Collectors.toList())
                );

                ClusterState newClusterState = result.resultingState;

                if (newClusterState != currentState) {
                    int lastAcceptedIndexMetadataVersion = indexMetadataStateVersion();

                    log.info("[IMC] Cluster State Changed, publishing new IndexMetadata");
                    indexMetadataStatePublisher.publishIndexMetadata(newClusterState,lastAcceptedIndexMetadataVersion+1);
                }

                for (UpdateTask updateTask : updateTasks) {
                    ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.get(updateTask.getTask());
                    if (taskResult.isSuccess()) {
                        updateTask.listener.onResponse(newClusterState);
                    } else {
                        updateTask.listener.onFailure(taskResult.getFailure());
                    }
                }
            } catch (Exception e) {
                updateTasks.forEach(task -> task.listener.onFailure(e));
            }
        }

        class UpdateTask extends BatchedTask {
            final IndexMetadataUpdateListener<Object> listener;

            UpdateTask(
                Priority priority,
                String source,
                Object task,
                IndexMetadataUpdateListener<?> listener,
                ClusterStateTaskExecutor<?> executor
            ) {
                super(priority, source, executor, task);
                this.listener = (IndexMetadataUpdateListener<Object>) listener;
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
