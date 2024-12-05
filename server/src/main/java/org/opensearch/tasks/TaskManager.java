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

package org.opensearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.SetOnce;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.ConcurrentMapLong;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.Assertions;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TcpChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 *
 * @opensearch.internal
 */
public class TaskManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);

    public static final String TASK_RESOURCE_CONSUMERS_ATTRIBUTES = "task_resource_consumers.enabled";

    public static final Setting<Boolean> TASK_RESOURCE_CONSUMERS_ENABLED = Setting.boolSetting(
        TASK_RESOURCE_CONSUMERS_ATTRIBUTES,
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Rest headers that are copied to the task
     */
    private final List<String> taskHeaders;
    private final ThreadPool threadPool;

    private final ConcurrentMapLong<Task> tasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final ConcurrentMapLong<CancellableTaskHolder> cancellableTasks = ConcurrentCollections
        .newConcurrentMapLongWithAggressiveConcurrency();

    private final AtomicLong taskIdGenerator = new AtomicLong();

    private final Map<TaskId, String> banedParents = new ConcurrentHashMap<>();

    private TaskResultsService taskResultsService;
    private final SetOnce<TaskResourceTrackingService> taskResourceTrackingService = new SetOnce<>();

    private volatile DiscoveryNodes lastDiscoveryNodes = DiscoveryNodes.EMPTY_NODES;

    private final ByteSizeValue maxHeaderSize;
    private final Map<TcpChannel, ChannelPendingTaskTracker> channelPendingTaskTrackers = ConcurrentCollections.newConcurrentMap();
    private final SetOnce<TaskCancellationService> cancellationService = new SetOnce<>();

    private volatile boolean taskResourceConsumersEnabled;
    private final Set<Consumer<Task>> taskResourceConsumer;
    private final List<TaskEventListeners> taskEventListeners = new ArrayList<>();

    public static TaskManager createTaskManagerWithClusterSettings(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        Set<String> taskHeaders
    ) {
        final TaskManager taskManager = new TaskManager(settings, threadPool, taskHeaders);
        clusterSettings.addSettingsUpdateConsumer(TASK_RESOURCE_CONSUMERS_ENABLED, taskManager::setTaskResourceConsumersEnabled);
        return taskManager;
    }

    public TaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        this.threadPool = threadPool;
        this.taskHeaders = new ArrayList<>(taskHeaders);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        this.taskResourceConsumersEnabled = TASK_RESOURCE_CONSUMERS_ENABLED.get(settings);
        taskResourceConsumer = new HashSet<>();
    }

    /**
     * Listener that gets invoked during an event such as task cancellation/completion.
     */
    public interface TaskEventListeners {
        default void onTaskCancelled(CancellableTask task) {}

        default void onTaskCompleted(Task task) {}
    }

    public void addTaskEventListeners(TaskEventListeners taskEventListeners) {
        this.taskEventListeners.add(taskEventListeners);
    }

    public void registerTaskResourceConsumer(Consumer<Task> consumer) {
        taskResourceConsumer.add(consumer);
    }

    public void setTaskResultsService(TaskResultsService taskResultsService) {
        assert this.taskResultsService == null;
        this.taskResultsService = taskResultsService;
    }

    public void setTaskCancellationService(TaskCancellationService taskCancellationService) {
        this.cancellationService.set(taskCancellationService);
    }

    public void setTaskResourceTrackingService(TaskResourceTrackingService taskResourceTrackingService) {
        this.taskResourceTrackingService.set(taskResourceTrackingService);
    }

    public void setTaskResourceConsumersEnabled(boolean taskResourceConsumersEnabled) {
        this.taskResourceConsumersEnabled = taskResourceConsumersEnabled;
    }

    /**
     * Registers a task without parent task
     */
    public Task register(String type, String action, TaskAwareRequest request) {
        Map<String, String> headers = new HashMap<>();
        long headerSize = 0;
        long maxSize = maxHeaderSize.getBytes();
        ThreadContext threadContext = threadPool.getThreadContext();
        for (String key : taskHeaders) {
            String httpHeader = threadContext.getHeader(key);
            if (httpHeader != null) {
                headerSize += key.length() * 2 + httpHeader.length() * 2;
                if (headerSize > maxSize) {
                    throw new IllegalArgumentException("Request exceeded the maximum size of task headers " + maxHeaderSize);
                }
                headers.put(key, httpHeader);
            }
        }
        Task task = request.createTask(taskIdGenerator.incrementAndGet(), type, action, request.getParentTask(), headers);
        Objects.requireNonNull(task);
        assert task.getParentTaskId().equals(request.getParentTask()) : "Request [ " + request + "] didn't preserve it parentTaskId";
        if (logger.isTraceEnabled()) {
            logger.trace("register {} [{}] [{}] [{}]", task.getId(), type, action, task.getDescription());
        }

        if (task.supportsResourceTracking()) {
            boolean success = task.addResourceTrackingCompletionListener(new NotifyOnceListener<>() {
                @Override
                protected void innerOnResponse(Task task) {
                    // Stop tracking the task once the last thread has been marked inactive.
                    if (taskResourceTrackingService.get() != null && task.supportsResourceTracking()) {
                        taskResourceTrackingService.get().stopTracking(task);
                    }
                }

                @Override
                protected void innerOnFailure(Exception e) {
                    ExceptionsHelper.reThrowIfNotNull(e);
                }
            });

            if (success == false) {
                logger.debug(
                    "failed to register a completion listener as task resource tracking has already completed [taskId={}]",
                    task.getId()
                );
            }
        }

        if (task instanceof CancellableTask) {
            registerCancellableTask(task);
        } else {
            Task previousTask = tasks.put(task.getId(), task);
            assert previousTask == null;
        }
        return task;
    }

    private void registerCancellableTask(Task task) {
        CancellableTask cancellableTask = (CancellableTask) task;
        CancellableTaskHolder holder = new CancellableTaskHolder(cancellableTask);
        CancellableTaskHolder oldHolder = cancellableTasks.put(task.getId(), holder);
        assert oldHolder == null;
        // Check if this task was banned before we start it. The empty check is used to avoid
        // computing the hash code of the parent taskId as most of the time banedParents is empty.
        if (task.getParentTaskId().isSet() && banedParents.isEmpty() == false) {
            String reason = banedParents.get(task.getParentTaskId());
            if (reason != null) {
                try {
                    holder.cancel(reason);
                    throw new TaskCancelledException("Task cancelled before it started: " + reason);
                } finally {
                    // let's clean up the registration
                    unregister(task);
                }
            }
        }
    }

    /**
     * Cancels a task
     * <p>
     * After starting cancellation on the parent task, the task manager tries to cancel all children tasks
     * of the current task. Once cancellation of the children tasks is done, the listener is triggered.
     * If the task is completed or unregistered from TaskManager, then the listener is called immediately.
     */
    public void cancel(CancellableTask task, String reason, Runnable listener) {
        CancellableTaskHolder holder = cancellableTasks.get(task.getId());
        List<Exception> exceptions = new ArrayList<>();
        for (TaskEventListeners taskEventListener : taskEventListeners) {
            try {
                taskEventListener.onTaskCancelled(task);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        // Throwing exception in case any of the cancellation listener results into exception.
        // Should we just swallow such exceptions?
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        if (holder != null) {
            logger.trace("cancelling task with id {}", task.getId());
            holder.cancel(reason, listener);
        } else {
            listener.run();
        }
    }

    /**
     * Unregister the task
     */
    public Task unregister(Task task) {
        logger.trace("unregister task for id: {}", task.getId());
        List<Exception> exceptions = new ArrayList<>();
        for (TaskEventListeners taskEventListener : taskEventListeners) {
            try {
                taskEventListener.onTaskCompleted(task);
            } catch (Exception e) {
                exceptions.add(e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);

        // Decrement the task's self-thread as part of unregistration.
        task.decrementResourceTrackingThreads();

        if (taskResourceConsumersEnabled) {
            for (Consumer<Task> taskConsumer : taskResourceConsumer) {
                try {
                    taskConsumer.accept(task);
                } catch (Exception e) {
                    logger.error("error encountered when updating the consumer", e);
                }
            }
        }

        if (task instanceof CancellableTask) {
            CancellableTaskHolder holder = cancellableTasks.remove(task.getId());
            if (holder != null) {
                holder.finish();
                return holder.getTask();
            } else {
                return null;
            }
        } else {
            return tasks.remove(task.getId());
        }
    }

    /**
     * Register a node on which a child task will execute. The returned {@link Releasable} must be called
     * to unregister the child node once the child task is completed or failed.
     */
    public Releasable registerChildNode(long taskId, DiscoveryNode node) {
        final CancellableTaskHolder holder = cancellableTasks.get(taskId);
        if (holder != null) {
            logger.trace("register child node [{}] task [{}]", node, taskId);
            holder.registerChildNode(node);
            return Releasables.releaseOnce(() -> {
                logger.trace("unregister child node [{}] task [{}]", node, taskId);
                holder.unregisterChildNode(node);
            });
        }
        return () -> {};
    }

    public DiscoveryNode localNode() {
        return lastDiscoveryNodes.getLocalNode();
    }

    /**
     * Stores the task failure
     */
    public <Response extends ActionResponse> void storeResult(Task task, Exception error, ActionListener<Response> listener) {
        DiscoveryNode localNode = lastDiscoveryNodes.getLocalNode();
        if (localNode == null) {
            // too early to store anything, shouldn't really be here - just pass the error along
            listener.onFailure(error);
            return;
        }
        final TaskResult taskResult;
        try {
            taskResult = task.result(localNode.getId(), error);
        } catch (IOException ex) {
            logger.warn(() -> new ParameterizedMessage("couldn't store error {}", ExceptionsHelper.detailedMessage(error)), ex);
            listener.onFailure(ex);
            return;
        }
        taskResultsService.storeResult(taskResult, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                listener.onFailure(error);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("couldn't store error {}", ExceptionsHelper.detailedMessage(error)), e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Stores the task result
     */
    public <Response extends ActionResponse> void storeResult(Task task, Response response, ActionListener<Response> listener) {
        DiscoveryNode localNode = lastDiscoveryNodes.getLocalNode();
        if (localNode == null) {
            // too early to store anything, shouldn't really be here - just pass the response along
            logger.warn("couldn't store response {}, the node didn't join the cluster yet", response);
            listener.onResponse(response);
            return;
        }
        final TaskResult taskResult;
        try {
            taskResult = task.result(localNode.getId(), response);
        } catch (IOException ex) {
            logger.warn(() -> new ParameterizedMessage("couldn't store response {}", response), ex);
            listener.onFailure(ex);
            return;
        }

        taskResultsService.storeResult(taskResult, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("couldn't store response {}", response), e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Returns the list of currently running tasks on the node
     */
    public Map<Long, Task> getTasks() {
        HashMap<Long, Task> taskHashMap = new HashMap<>(this.tasks);
        for (CancellableTaskHolder holder : cancellableTasks.values()) {
            taskHashMap.put(holder.getTask().getId(), holder.getTask());
        }
        return Collections.unmodifiableMap(taskHashMap);
    }

    /**
     * Returns the list of currently running tasks on the node that can be cancelled
     */
    public Map<Long, CancellableTask> getCancellableTasks() {
        HashMap<Long, CancellableTask> taskHashMap = new HashMap<>();
        for (CancellableTaskHolder holder : cancellableTasks.values()) {
            taskHashMap.put(holder.getTask().getId(), holder.getTask());
        }
        return Collections.unmodifiableMap(taskHashMap);
    }

    /**
     * Returns a task with given id, or null if the task is not found.
     */
    public Task getTask(long id) {
        Task task = tasks.get(id);
        if (task != null) {
            return task;
        } else {
            return getCancellableTask(id);
        }
    }

    /**
     * Returns a cancellable task with given id, or null if the task is not found.
     */
    public CancellableTask getCancellableTask(long id) {
        CancellableTaskHolder holder = cancellableTasks.get(id);
        if (holder != null) {
            return holder.getTask();
        } else {
            return null;
        }
    }

    /**
     * Returns the number of currently banned tasks.
     * <p>
     * Will be used in task manager stats and for debugging.
     */
    public int getBanCount() {
        return banedParents.size();
    }

    /**
     * Bans all tasks with the specified parent task from execution, cancels all tasks that are currently executing.
     * <p>
     * This method is called when a parent task that has children is cancelled.
     *
     * @return a list of pending cancellable child tasks
     */
    public List<CancellableTask> setBan(TaskId parentTaskId, String reason) {
        logger.trace("setting ban for the parent task {} {}", parentTaskId, reason);

        // Set the ban first, so the newly created tasks cannot be registered
        synchronized (banedParents) {
            if (lastDiscoveryNodes.nodeExists(parentTaskId.getNodeId())) {
                // Only set the ban if the node is the part of the cluster
                banedParents.put(parentTaskId, reason);
            }
        }
        return cancellableTasks.values().stream().filter(t -> t.hasParent(parentTaskId)).map(t -> t.task).collect(Collectors.toList());
    }

    /**
     * Removes the ban for the specified parent task.
     * <p>
     * This method is called when a previously banned task finally cancelled
     */
    public void removeBan(TaskId parentTaskId) {
        logger.trace("removing ban for the parent task {}", parentTaskId);
        banedParents.remove(parentTaskId);
    }

    // for testing
    public Set<TaskId> getBannedTaskIds() {
        return Collections.unmodifiableSet(banedParents.keySet());
    }

    public Collection<DiscoveryNode> startBanOnChildrenNodes(long taskId, Runnable onChildTasksCompleted) {
        return startBanOnChildrenNodes(taskId, onChildTasksCompleted, "unknown");
    }

    /**
     * Start rejecting new child requests as the parent task was cancelled.
     *
     * @param taskId                the parent task id
     * @param onChildTasksCompleted called when all child tasks are completed or failed
     * @param reason                the ban reason
     * @return the set of current nodes that have outstanding child tasks
     */
    public Collection<DiscoveryNode> startBanOnChildrenNodes(long taskId, Runnable onChildTasksCompleted, String reason) {
        final CancellableTaskHolder holder = cancellableTasks.get(taskId);
        if (holder != null) {
            return holder.startBan(onChildTasksCompleted, reason);
        } else {
            onChildTasksCompleted.run();
            return Collections.emptySet();
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        lastDiscoveryNodes = event.state().getNodes();
        if (event.nodesRemoved()) {
            synchronized (banedParents) {
                lastDiscoveryNodes = event.state().getNodes();
                // Remove all bans that were registered by nodes that are no longer in the cluster state
                Iterator<TaskId> banIterator = banedParents.keySet().iterator();
                while (banIterator.hasNext()) {
                    TaskId taskId = banIterator.next();
                    if (lastDiscoveryNodes.nodeExists(taskId.getNodeId()) == false) {
                        logger.debug(
                            "Removing ban for the parent [{}] on the node [{}], reason: the parent node is gone",
                            taskId,
                            event.state().getNodes().getLocalNode()
                        );
                        banIterator.remove();
                    }
                }
            }
        }
    }

    /**
     * Blocks the calling thread, waiting for the task to vanish from the TaskManager.
     */
    public void waitForTaskCompletion(Task task, long untilInNanos) {
        while (System.nanoTime() - untilInNanos < 0) {
            if (getTask(task.getId()) == null) {
                return;
            }
            try {
                Thread.sleep(WAIT_FOR_COMPLETION_POLL.millis());
            } catch (InterruptedException e) {
                throw new OpenSearchException("Interrupted waiting for completion of [{}]", e, task);
            }
        }
        throw new OpenSearchTimeoutException("Timed out waiting for completion of [{}]", task);
    }

    /**
     * Takes actions when a task is registered and its execution starts
     *
     * @param task getting executed.
     * @return AutoCloseable to free up resources (clean up thread context) when task execution block returns
     */
    public ThreadContext.StoredContext taskExecutionStarted(Task task) {
        if (taskResourceTrackingService.get() == null) return () -> {};

        return taskResourceTrackingService.get().startTracking(task);
    }

    private static class CancellableTaskHolder {
        private final CancellableTask task;
        private boolean finished = false;
        private List<Runnable> cancellationListeners = null;
        private Map<DiscoveryNode, Integer> childTasksPerNode = null;
        private boolean banChildren = false;
        private String banReason;
        private List<Runnable> childTaskCompletedListeners = null;

        CancellableTaskHolder(CancellableTask task) {
            this.task = task;
        }

        void cancel(String reason, Runnable listener) {
            final Runnable toRun;
            synchronized (this) {
                if (finished) {
                    assert cancellationListeners == null;
                    toRun = listener;
                } else {
                    toRun = () -> {};
                    if (listener != null) {
                        if (cancellationListeners == null) {
                            cancellationListeners = new ArrayList<>();
                        }
                        cancellationListeners.add(listener);
                    }
                }
            }
            try {
                task.cancel(reason);
            } finally {
                if (toRun != null) {
                    toRun.run();
                }
            }
        }

        void cancel(String reason) {
            task.cancel(reason);
        }

        /**
         * Marks task as finished.
         */
        public void finish() {
            final List<Runnable> listeners;
            synchronized (this) {
                this.finished = true;
                if (cancellationListeners != null) {
                    listeners = cancellationListeners;
                    cancellationListeners = null;
                } else {
                    listeners = Collections.emptyList();
                }
            }
            // We need to call the listener outside of the synchronised section to avoid potential bottle necks
            // in the listener synchronization
            notifyListeners(listeners);
        }

        private void notifyListeners(List<Runnable> listeners) {
            assert Thread.holdsLock(this) == false;
            Exception rootException = null;
            for (Runnable listener : listeners) {
                try {
                    listener.run();
                } catch (RuntimeException inner) {
                    rootException = ExceptionsHelper.useOrSuppress(rootException, inner);
                }
            }
            ExceptionsHelper.reThrowIfNotNull(rootException);
        }

        public boolean hasParent(TaskId parentTaskId) {
            return task.getParentTaskId().equals(parentTaskId);
        }

        public CancellableTask getTask() {
            return task;
        }

        synchronized void registerChildNode(DiscoveryNode node) {
            if (banChildren) {
                throw new TaskCancelledException("The parent task was cancelled, shouldn't start any child tasks, " + banReason);
            }
            if (childTasksPerNode == null) {
                childTasksPerNode = new HashMap<>();
            }
            childTasksPerNode.merge(node, 1, Integer::sum);
        }

        void unregisterChildNode(DiscoveryNode node) {
            final List<Runnable> listeners;
            synchronized (this) {
                if (childTasksPerNode.merge(node, -1, Integer::sum) == 0) {
                    childTasksPerNode.remove(node);
                }
                if (childTasksPerNode.isEmpty() && this.childTaskCompletedListeners != null) {
                    listeners = childTaskCompletedListeners;
                    childTaskCompletedListeners = null;
                } else {
                    listeners = Collections.emptyList();
                }
            }
            notifyListeners(listeners);
        }

        Set<DiscoveryNode> startBan(Runnable onChildTasksCompleted, String reason) {
            final Set<DiscoveryNode> pendingChildNodes;
            final Runnable toRun;
            synchronized (this) {
                banChildren = true;
                assert reason != null;
                banReason = reason;
                if (childTasksPerNode == null) {
                    pendingChildNodes = Collections.emptySet();
                } else {
                    pendingChildNodes = Set.copyOf(childTasksPerNode.keySet());
                }
                if (pendingChildNodes.isEmpty()) {
                    assert childTaskCompletedListeners == null;
                    toRun = onChildTasksCompleted;
                } else {
                    toRun = () -> {};
                    if (childTaskCompletedListeners == null) {
                        childTaskCompletedListeners = new ArrayList<>();
                    }
                    childTaskCompletedListeners.add(onChildTasksCompleted);
                }
            }
            toRun.run();
            return pendingChildNodes;
        }
    }

    /**
     * Start tracking a cancellable task with its tcp channel, so if the channel gets closed we can get a set of
     * pending tasks associated that channel and cancel them as these results won't be retrieved by the parent task.
     *
     * @return a releasable that should be called when this pending task is completed
     */
    public Releasable startTrackingCancellableChannelTask(TcpChannel channel, CancellableTask task) {
        assert cancellableTasks.containsKey(task.getId()) : "task [" + task.getId() + "] is not registered yet";
        final ChannelPendingTaskTracker tracker = channelPendingTaskTrackers.compute(channel, (k, curr) -> {
            if (curr == null) {
                curr = new ChannelPendingTaskTracker();
            }
            curr.addTask(task);
            return curr;
        });
        if (tracker.registered.compareAndSet(false, true)) {
            channel.addCloseListener(ActionListener.wrap(r -> {
                final ChannelPendingTaskTracker removedTracker = channelPendingTaskTrackers.remove(channel);
                assert removedTracker == tracker;
                cancelTasksOnChannelClosed(tracker.drainTasks());
            }, e -> { assert false : new AssertionError("must not be here", e); }));
        }
        return () -> tracker.removeTask(task);
    }

    // for testing
    final int numberOfChannelPendingTaskTrackers() {
        return channelPendingTaskTrackers.size();
    }

    private static class ChannelPendingTaskTracker {
        final AtomicBoolean registered = new AtomicBoolean();
        final Semaphore permits = Assertions.ENABLED ? new Semaphore(Integer.MAX_VALUE) : null;
        final Set<CancellableTask> pendingTasks = ConcurrentCollections.newConcurrentSet();

        void addTask(CancellableTask task) {
            assert permits.tryAcquire() : "tracker was drained";
            final boolean added = pendingTasks.add(task);
            assert added : "task " + task.getId() + " is in the pending list already";
            assert releasePermit();
        }

        boolean acquireAllPermits() {
            permits.acquireUninterruptibly(Integer.MAX_VALUE);
            return true;
        }

        boolean releasePermit() {
            permits.release();
            return true;
        }

        Set<CancellableTask> drainTasks() {
            assert acquireAllPermits(); // do not release permits so we can't add tasks to this tracker after draining
            return Collections.unmodifiableSet(pendingTasks);
        }

        void removeTask(CancellableTask task) {
            final boolean removed = pendingTasks.remove(task);
            assert removed : "task " + task.getId() + " is not in the pending list";
        }
    }

    private void cancelTasksOnChannelClosed(Set<CancellableTask> tasks) {
        if (tasks.isEmpty() == false) {
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to cancel tasks on channel closed", e);
                }

                @Override
                protected void doRun() {
                    for (CancellableTask task : tasks) {
                        cancelTaskAndDescendants(task, "channel was closed", false, ActionListener.wrap(() -> {}));
                    }
                }
            });
        }
    }

    public void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        final TaskCancellationService service = cancellationService.get();
        if (service != null) {
            service.cancelTaskAndDescendants(task, reason, waitForCompletion, listener);
        } else {
            assert false : "TaskCancellationService is not initialized";
            throw new IllegalStateException("TaskCancellationService is not initialized");
        }
    }
}
