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

package org.opensearch.cluster.service;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.PrioritizedOpenSearchThreadPoolExecutor;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Batching support for {@link PrioritizedOpenSearchThreadPoolExecutor}
 * Tasks that share the same batching key are batched (see {@link BatchedTask#batchingKey})
 *
 * @opensearch.internal
 */
public abstract class TaskBatcher {

    private final Logger logger;
    private final PrioritizedOpenSearchThreadPoolExecutor threadExecutor;
    // package visible for tests
    final Map<Object, LinkedHashSet<BatchedTask>> tasksPerBatchingKey = new ConcurrentHashMap<>();
    final Map<Object, Map<Object, BatchedTask>> taskIdentityPerBatchingKey = new ConcurrentHashMap<>();
    private final TaskBatcherListener taskBatcherListener;

    public TaskBatcher(Logger logger, PrioritizedOpenSearchThreadPoolExecutor threadExecutor, TaskBatcherListener taskBatcherListener) {
        this.logger = logger;
        this.threadExecutor = threadExecutor;
        this.taskBatcherListener = taskBatcherListener;
    }

    public void submitTasks(List<? extends BatchedTask> tasks, @Nullable TimeValue timeout) throws OpenSearchRejectedExecutionException {
        if (tasks.isEmpty()) {
            return;
        }
        final BatchedTask firstTask = tasks.get(0);
        assert tasks.stream().allMatch(t -> t.batchingKey == firstTask.batchingKey)
            : "tasks submitted in a batch should share the same batching key: " + tasks;
        assert tasks.stream().allMatch(t -> t.getTask().getClass() == firstTask.getTask().getClass())
            : "tasks submitted in a batch should be of same class: " + tasks;

        taskBatcherListener.onBeginSubmit(tasks);

        try {
            // convert to an identity map to check for dups based on task identity
            final Map<Object, BatchedTask> tasksIdentity = tasks.stream()
                .collect(Collectors.toMap(BatchedTask::getTask, Function.identity(), (a, b) -> {
                    throw new IllegalStateException("cannot add duplicate task: " + a);
                }, IdentityHashMap::new));
            LinkedHashSet<BatchedTask> newTasks = new LinkedHashSet<>(tasks);
            // Need to maintain below order in which task identity map and task map are updated.
            // For insert: First insert identity in taskIdentity map with dup check and then insert task in taskMap.
            // For remove: First remove task from taskMap and then remove identity from taskIdentity map.
            // We are inserting identity first and removing at last to ensure no duplicate tasks are enqueued.
            // Changing this order might lead to duplicate tasks in queue.
            taskIdentityPerBatchingKey.merge(firstTask.batchingKey, tasksIdentity, (existingIdentities, newIdentities) -> {
                for (Object newIdentity : newIdentities.keySet()) {
                    // check that there won't be two tasks with the same identity for the same batching key
                    if (existingIdentities.containsKey(newIdentity)) {
                        BatchedTask duplicateTask = newIdentities.get(newIdentity);
                        throw new IllegalStateException(
                            "task ["
                                + duplicateTask.describeTasks(Collections.singletonList(duplicateTask))
                                + "] with source ["
                                + duplicateTask.source
                                + "] is already queued"
                        );
                    }
                }
                existingIdentities.putAll(newIdentities);
                return existingIdentities;
            });
            // since we have checked for dup tasks in above map, we can add all new task in map.
            tasksPerBatchingKey.merge(firstTask.batchingKey, newTasks, (existingTasks, updatedTasks) -> {
                existingTasks.addAll(updatedTasks);
                return existingTasks;
            });
        } catch (Exception e) {
            taskBatcherListener.onSubmitFailure(tasks);
            throw e;
        }

        if (timeout != null) {
            threadExecutor.execute(firstTask, timeout, () -> onTimeoutInternal(tasks, timeout));
        } else {
            threadExecutor.execute(firstTask);
        }
    }

    void onTimeoutInternal(List<? extends BatchedTask> tasks, TimeValue timeout) {
        final ArrayList<BatchedTask> toRemove = new ArrayList<>();
        final ArrayList<Object> toRemoveIdentities = new ArrayList<>();
        for (BatchedTask task : tasks) {
            if (task.processed.getAndSet(true) == false) {
                logger.debug("task [{}] timed out after [{}]", task.source, timeout);
                toRemove.add(task);
                toRemoveIdentities.add(task.getTask());
            }
        }
        if (toRemove.isEmpty() == false) {
            BatchedTask firstTask = toRemove.get(0);
            Object batchingKey = firstTask.batchingKey;
            assert tasks.stream().allMatch(t -> t.batchingKey == batchingKey)
                : "tasks submitted in a batch should share the same batching key: " + tasks;
            // While removing task, need to remove task first from taskMap and then remove identity from identityMap.
            // Changing this order might lead to duplicate task during submission.
            tasksPerBatchingKey.computeIfPresent(batchingKey, (tasksKey, existingTasks) -> {
                existingTasks.removeAll(toRemove);
                if (existingTasks.isEmpty()) {
                    return null;
                }
                return existingTasks;
            });
            taskIdentityPerBatchingKey.computeIfPresent(batchingKey, (tasksKey, existingIdentities) -> {
                toRemoveIdentities.stream().forEach(existingIdentities::remove);
                if (existingIdentities.isEmpty()) {
                    return null;
                }
                return existingIdentities;
            });
            taskBatcherListener.onTimeout(toRemove);
            onTimeout(toRemove, timeout);
        }
    }

    /**
     * Action to be implemented by the specific batching implementation.
     * All tasks have the same batching key.
     */
    protected abstract void onTimeout(List<? extends BatchedTask> tasks, TimeValue timeout);

    void runIfNotProcessed(BatchedTask updateTask) {
        // if this task is already processed, it shouldn't execute other tasks with same batching key that arrived later,
        // to give other tasks with different batching key a chance to execute.
        if (updateTask.processed.get() == false) {
            final List<BatchedTask> toExecute = new ArrayList<>();
            // While removing task, need to remove task first from taskMap and then remove identity from identityMap.
            // Changing this order might lead to duplicate task during submission.
            LinkedHashSet<BatchedTask> pending = tasksPerBatchingKey.remove(updateTask.batchingKey);
            taskIdentityPerBatchingKey.remove(updateTask.batchingKey);
            if (pending != null) {
                for (BatchedTask task : pending) {
                    if (task.processed.getAndSet(true) == false) {
                        logger.trace("will process {}", task);
                        toExecute.add(task);
                    } else {
                        logger.trace("skipping {}, already processed", task);
                    }
                }
            }

            if (toExecute.isEmpty() == false) {
                Function<Boolean, String> taskSummaryGenerator = (longSummaryRequired) -> {
                    if (longSummaryRequired == null || !longSummaryRequired) {
                        final List<BatchedTask> sampleTasks = toExecute.stream()
                            .limit(Math.min(1000, toExecute.size()))
                            .collect(Collectors.toList());
                        return buildShortSummary(updateTask.batchingKey, toExecute.size(), getSummary(updateTask, sampleTasks));
                    }
                    return getSummary(updateTask, toExecute);
                };
                taskBatcherListener.onBeginProcessing(toExecute);
                run(updateTask.batchingKey, toExecute, taskSummaryGenerator);
            }
        }
    }

    private String getSummary(final BatchedTask updateTask, final List<BatchedTask> toExecute) {
        final Map<String, List<BatchedTask>> processTasksBySource = new HashMap<>();
        for (final BatchedTask task : toExecute) {
            processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task);
        }
        return processTasksBySource.entrySet().stream().map(entry -> {
            String tasks = updateTask.describeTasks(entry.getValue());
            return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
        }).reduce((s1, s2) -> s1 + ", " + s2).orElse("");
    }

    private String buildShortSummary(final Object batchingKey, final int taskCount, final String sampleTasks) {
        return "Tasks batched with key: "
            + batchingKey.toString().split("\\$")[0]
            + ", count:"
            + taskCount
            + " and sample tasks: "
            + sampleTasks;
    }

    /**
     * Action to be implemented by the specific batching implementation
     * All tasks have the given batching key.
     */
    protected abstract void run(Object batchingKey, List<? extends BatchedTask> tasks, Function<Boolean, String> taskSummaryGenerator);

    /**
     * Represents a runnable task that supports batching.
     * Implementors of TaskBatcher can subclass this to add a payload to the task.
     */
    protected abstract class BatchedTask extends SourcePrioritizedRunnable {
        /**
         * whether the task has been processed already
         */
        protected final AtomicBoolean processed = new AtomicBoolean();

        /**
         * the object that is used as batching key
         */
        protected final Object batchingKey;
        /**
         * the task object that is wrapped
         */
        protected final Object task;

        protected BatchedTask(Priority priority, String source, Object batchingKey, Object task) {
            super(priority, source);
            this.batchingKey = batchingKey;
            this.task = task;
        }

        @Override
        public void run() {
            runIfNotProcessed(this);
        }

        @Override
        public String toString() {
            String taskDescription = describeTasks(Collections.singletonList(this));
            if (taskDescription.isEmpty()) {
                return "[" + source + "]";
            } else {
                return "[" + source + "[" + taskDescription + "]]";
            }
        }

        public abstract String describeTasks(List<? extends BatchedTask> tasks);

        public Object getTask() {
            return task;
        }
    }
}
