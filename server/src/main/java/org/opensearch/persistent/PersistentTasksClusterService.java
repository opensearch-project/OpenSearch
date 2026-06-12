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
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateTaskConfig;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.ClusterStateTaskListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.NotClusterManagerException;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;
import org.opensearch.core.action.ActionListener;
import org.opensearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.opensearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.opensearch.persistent.decider.AssignmentDecision;
import org.opensearch.persistent.decider.EnableAssignmentDecider;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;

import static org.opensearch.cluster.service.ClusterManagerTask.UPDATE_TASK_STATE;

/**
 * Component that runs only on the cluster-manager node and is responsible for assigning running tasks to nodes
 *
 * @opensearch.internal
 */
public class PersistentTasksClusterService implements ClusterStateListener, Closeable {

    public static final Setting<TimeValue> CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.persistent_tasks.allocation.recheck_interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(PersistentTasksClusterService.class);

    private final ClusterService clusterService;
    private final PersistentTasksExecutorRegistry registry;
    private final EnableAssignmentDecider decider;
    private final ThreadPool threadPool;
    private final PeriodicRechecker periodicRechecker;
    private final ClusterManagerTaskThrottler.ThrottlingKey updatePersistentTaskKey;
    private final PersistentTaskUpdateExecutor updateTaskStateExecutor;

    public PersistentTasksClusterService(
        Settings settings,
        PersistentTasksExecutorRegistry registry,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.clusterService = clusterService;
        this.registry = registry;
        this.decider = new EnableAssignmentDecider(settings, clusterService.getClusterSettings());
        this.threadPool = threadPool;
        this.periodicRechecker = new PeriodicRechecker(CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING.get(settings));
        if (DiscoveryNode.isClusterManagerNode(settings)) {
            clusterService.addListener(this);
        }
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(CLUSTER_TASKS_ALLOCATION_RECHECK_INTERVAL_SETTING, this::setRecheckInterval);

        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        updatePersistentTaskKey = clusterService.registerClusterManagerTask(UPDATE_TASK_STATE, true);
        this.updateTaskStateExecutor = new PersistentTaskUpdateExecutor();
    }

    // Represents a persistent task update operation that can be batched.
    static class PersistentTaskUpdateEntry {

        enum OperationType {
            CREATE,
            UPDATE_STATE,
            COMPLETE,
            REMOVE,
            UNASSIGN
        }

        final OperationType operationType;
        final String taskId;
        final long allocationId;
        final PersistentTaskState taskState;
        // Fields used only for CREATE
        final String taskName;
        final PersistentTaskParams taskParams;
        // Field used only for UNASSIGN
        final String unassignReason;

        private PersistentTaskUpdateEntry(
            OperationType operationType,
            String taskId,
            long allocationId,
            PersistentTaskState taskState,
            String taskName,
            PersistentTaskParams taskParams,
            String unassignReason
        ) {
            this.operationType = operationType;
            this.taskId = taskId;
            this.allocationId = allocationId;
            this.taskState = taskState;
            this.taskName = taskName;
            this.taskParams = taskParams;
            this.unassignReason = unassignReason;
        }

        static PersistentTaskUpdateEntry create(String taskId, String taskName, PersistentTaskParams taskParams) {
            return new PersistentTaskUpdateEntry(OperationType.CREATE, taskId, 0, null, taskName, taskParams, null);
        }

        static PersistentTaskUpdateEntry updateState(String taskId, long allocationId, PersistentTaskState taskState) {
            return new PersistentTaskUpdateEntry(OperationType.UPDATE_STATE, taskId, allocationId, taskState, null, null, null);
        }

        static PersistentTaskUpdateEntry complete(String taskId, long allocationId) {
            return new PersistentTaskUpdateEntry(OperationType.COMPLETE, taskId, allocationId, null, null, null, null);
        }

        static PersistentTaskUpdateEntry remove(String taskId) {
            return new PersistentTaskUpdateEntry(OperationType.REMOVE, taskId, 0, null, null, null, null);
        }

        static PersistentTaskUpdateEntry unassign(String taskId, long allocationId, String reason) {
            return new PersistentTaskUpdateEntry(OperationType.UNASSIGN, taskId, allocationId, null, null, null, reason);
        }

        @Override
        public String toString() {
            return operationType + "[" + taskId + "]";
        }
    }

    /**
     * Singleton executor that batches all persistent task operations.
     * Shares a single batching key so TaskBatcher accumulates concurrent submissions
     * into one cluster state publish cycle.
     */
    class PersistentTaskUpdateExecutor implements ClusterStateTaskExecutor<PersistentTaskUpdateEntry> {

        @Override
        public ClusterTasksResult<PersistentTaskUpdateEntry> execute(ClusterState currentState, List<PersistentTaskUpdateEntry> tasks) {
            ClusterTasksResult.Builder<PersistentTaskUpdateEntry> resultBuilder = ClusterTasksResult.builder();
            ClusterState state = currentState;

            for (PersistentTaskUpdateEntry entry : tasks) {
                try {
                    switch (entry.operationType) {
                        case CREATE:
                            state = applyCreate(state, entry);
                            break;
                        case UPDATE_STATE:
                            state = applyUpdateState(state, entry);
                            break;
                        case COMPLETE:
                            state = applyComplete(state, entry);
                            break;
                        case REMOVE:
                            state = applyRemove(state, entry);
                            break;
                        case UNASSIGN:
                            state = applyUnassign(state, entry);
                            break;
                    }
                    resultBuilder.success(entry);
                } catch (Exception e) {
                    resultBuilder.failure(entry, e);
                }
            }

            return resultBuilder.build(state);
        }

        private ClusterState applyCreate(ClusterState state, PersistentTaskUpdateEntry entry) {
            PersistentTasksCustomMetadata.Builder tasksBuilder = builder(state);
            if (tasksBuilder.hasTask(entry.taskId)) {
                throw new ResourceAlreadyExistsException("task with id {" + entry.taskId + "} already exist");
            }
            PersistentTasksExecutor<PersistentTaskParams> taskExecutor = registry.getPersistentTaskExecutorSafe(entry.taskName);
            taskExecutor.validate(entry.taskParams, state);
            Assignment assignment = createAssignment(entry.taskName, entry.taskParams, state);
            return update(state, tasksBuilder.addTask(entry.taskId, entry.taskName, entry.taskParams, assignment));
        }

        private ClusterState applyUpdateState(ClusterState state, PersistentTaskUpdateEntry entry) {
            PersistentTasksCustomMetadata.Builder tasksBuilder = builder(state);
            if (tasksBuilder.hasTask(entry.taskId, entry.allocationId)) {
                return update(state, tasksBuilder.updateTaskState(entry.taskId, entry.taskState));
            }
            if (tasksBuilder.hasTask(entry.taskId)) {
                logger.warn("trying to update state on task {} with unexpected allocation id {}", entry.taskId, entry.allocationId);
            } else {
                logger.warn("trying to update state on non-existing task {}", entry.taskId);
            }
            throw new ResourceNotFoundException("the task with id {} and allocation id {} doesn't exist", entry.taskId, entry.allocationId);
        }

        private ClusterState applyComplete(ClusterState state, PersistentTaskUpdateEntry entry) {
            PersistentTasksCustomMetadata.Builder tasksBuilder = builder(state);
            if (tasksBuilder.hasTask(entry.taskId, entry.allocationId)) {
                tasksBuilder.removeTask(entry.taskId);
                return update(state, tasksBuilder);
            }
            if (tasksBuilder.hasTask(entry.taskId)) {
                PersistentTask<?> existingTask = PersistentTasksCustomMetadata.getTaskWithId(state, entry.taskId);
                logger.warn(
                    "The task [{}] with id [{}] has a different allocation id [{}], status is not updated",
                    existingTask != null ? existingTask.getTaskName() : "unknown",
                    entry.taskId,
                    entry.allocationId
                );
            } else {
                logger.warn("The task [{}] wasn't found, status is not updated", entry.taskId);
            }
            throw new ResourceNotFoundException(
                "the task with id [" + entry.taskId + "] and allocation id [" + entry.allocationId + "] not found"
            );
        }

        private ClusterState applyRemove(ClusterState state, PersistentTaskUpdateEntry entry) {
            PersistentTasksCustomMetadata.Builder tasksBuilder = builder(state);
            if (tasksBuilder.hasTask(entry.taskId)) {
                return update(state, tasksBuilder.removeTask(entry.taskId));
            }
            throw new ResourceNotFoundException("the task with id {} doesn't exist", entry.taskId);
        }

        private ClusterState applyUnassign(ClusterState state, PersistentTaskUpdateEntry entry) {
            PersistentTasksCustomMetadata.Builder tasksBuilder = builder(state);
            if (tasksBuilder.hasTask(entry.taskId, entry.allocationId)) {
                logger.trace("Unassigning task {} with allocation id {}", entry.taskId, entry.allocationId);
                return update(state, tasksBuilder.reassignTask(entry.taskId, unassignedAssignment(entry.unassignReason)));
            }
            throw new ResourceNotFoundException("the task with id {} and allocation id {} doesn't exist", entry.taskId, entry.allocationId);
        }

        @Override
        public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
            return updatePersistentTaskKey;
        }
    }

    // visible for testing only
    public void setRecheckInterval(TimeValue recheckInterval) {
        periodicRechecker.setInterval(recheckInterval);
    }

    // visible for testing only
    PeriodicRechecker getPeriodicRechecker() {
        return periodicRechecker;
    }

    @Override
    public void close() {
        periodicRechecker.close();
    }

    /**
     * Creates a new persistent task on cluster-manager node
     *
     * @param taskId     the task's id
     * @param taskName   the task's name
     * @param taskParams the task's parameters
     * @param listener   the listener that will be called when task is started
     */
    public <Params extends PersistentTaskParams> void createPersistentTask(
        String taskId,
        String taskName,
        Params taskParams,
        ActionListener<PersistentTask<?>> listener
    ) {
        submitPersistentTaskUpdate("create persistent task", PersistentTaskUpdateEntry.create(taskId, taskName, taskParams), listener);
    }

    /**
     * Restarts a record about a running persistent task from cluster state
     *
     * @param id           the id of the persistent task
     * @param allocationId the allocation id of the persistent task
     * @param failure      the reason for restarting the task or null if the task completed successfully
     * @param listener     the listener that will be called when task is removed
     */
    public void completePersistentTask(String id, long allocationId, Exception failure, ActionListener<PersistentTask<?>> listener) {
        if (failure != null) {
            logger.warn("persistent task " + id + " failed", failure);
        }
        submitPersistentTaskUpdate(
            failure != null ? "finish persistent task (failed)" : "finish persistent task (success)",
            PersistentTaskUpdateEntry.complete(id, allocationId),
            listener
        );
    }

    /**
     * Removes the persistent task
     *
     * @param id       the id of a persistent task
     * @param listener the listener that will be called when task is removed
     */
    public void removePersistentTask(String id, ActionListener<PersistentTask<?>> listener) {
        submitPersistentTaskUpdate("remove persistent task", PersistentTaskUpdateEntry.remove(id), listener);
    }

    /**
     * Update the state of a persistent task
     *
     * @param taskId           the id of a persistent task
     * @param taskAllocationId the expected allocation id of the persistent task
     * @param taskState        new state
     * @param listener         the listener that will be called when task is removed
     */
    public void updatePersistentTaskState(
        final String taskId,
        final long taskAllocationId,
        final PersistentTaskState taskState,
        final ActionListener<PersistentTask<?>> listener
    ) {
        submitPersistentTaskUpdate(
            "update task state [" + taskId + "]",
            PersistentTaskUpdateEntry.updateState(taskId, taskAllocationId, taskState),
            listener
        );
    }

    private void submitPersistentTaskUpdate(String source, PersistentTaskUpdateEntry entry, ActionListener<PersistentTask<?>> listener) {
        clusterService.submitStateUpdateTask(
            source,
            entry,
            ClusterStateTaskConfig.build(Priority.NORMAL),
            updateTaskStateExecutor,
            new ClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    // For COMPLETE/REMOVE use oldState since the task is gone in newState
                    ClusterState stateForLookup = (entry.operationType == PersistentTaskUpdateEntry.OperationType.COMPLETE
                        || entry.operationType == PersistentTaskUpdateEntry.OperationType.REMOVE) ? oldState : newState;
                    PersistentTask<?> task = PersistentTasksCustomMetadata.getTaskWithId(stateForLookup, entry.taskId);
                    listener.onResponse(task);

                    // For CREATE, trigger periodic rechecker if the task ended up unassigned
                    if (entry.operationType == PersistentTaskUpdateEntry.OperationType.CREATE
                        && task != null
                        && task.isAssigned() == false
                        && periodicRechecker.isScheduled() == false) {
                        periodicRechecker.rescheduleIfNecessary();
                    }
                }
            }
        );
    }

    /**
     * This unassigns a task from any node, i.e. it is assigned to a {@code null} node with the provided reason.
     * <p>
     * Since the assignment executor node is null, the {@link PersistentTasksClusterService} will attempt to reassign it to a valid
     * node quickly.
     *
     * @param taskId           the id of a persistent task
     * @param taskAllocationId the expected allocation id of the persistent task
     * @param reason           the reason for unassigning the task from any node
     * @param listener         the listener that will be called when task is unassigned
     */
    public void unassignPersistentTask(
        final String taskId,
        final long taskAllocationId,
        final String reason,
        final ActionListener<PersistentTask<?>> listener
    ) {
        submitPersistentTaskUpdate(
            "unassign persistent task from any node",
            PersistentTaskUpdateEntry.unassign(taskId, taskAllocationId, reason),
            listener
        );
    }

    /**
     * Creates a new {@link Assignment} for the given persistent task.
     *
     * @param taskName the task's name
     * @param taskParams the task's parameters
     * @param currentState the current {@link ClusterState}

     * @return a new {@link Assignment}
     */
    private <Params extends PersistentTaskParams> Assignment createAssignment(
        final String taskName,
        final Params taskParams,
        final ClusterState currentState
    ) {
        PersistentTasksExecutor<Params> persistentTasksExecutor = registry.getPersistentTaskExecutorSafe(taskName);

        AssignmentDecision decision = decider.canAssign();
        if (decision.getType() == AssignmentDecision.Type.NO) {
            return unassignedAssignment("persistent task [" + taskName + "] cannot be assigned [" + decision.getReason() + "]");
        }

        return persistentTasksExecutor.getAssignment(taskParams, currentState);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeClusterManager()) {
            if (shouldReassignPersistentTasks(event)) {
                // We want to avoid a periodic check duplicating this work
                periodicRechecker.cancel();
                logger.trace("checking task reassignment for cluster state {}", event.state().getVersion());
                reassignPersistentTasks();
            }
        } else {
            periodicRechecker.cancel();
        }
    }

    /**
     * Submit a cluster state update to reassign any persistent tasks that need reassigning
     */
    private void reassignPersistentTasks() {
        clusterService.submitStateUpdateTask("reassign persistent tasks", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return reassignTasks(currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn("failed to reassign persistent tasks", e);
                if (e instanceof NotClusterManagerException == false) {
                    // There must be a task that's worth rechecking because there was one
                    // that caused this method to be called and the method failed to assign it,
                    // but only do this if the node is still the master
                    periodicRechecker.rescheduleIfNecessary();
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (isAnyTaskUnassigned(newState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE))) {
                    periodicRechecker.rescheduleIfNecessary();
                }
            }
        });
    }

    /**
     * Returns true if the cluster state change(s) require to reassign some persistent tasks. It can happen in the following
     * situations: a node left or is added, the routing table changed, the cluster-manager node changed, the metadata changed or the
     * persistent tasks changed.
     */
    boolean shouldReassignPersistentTasks(final ClusterChangedEvent event) {
        final PersistentTasksCustomMetadata tasks = event.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks == null) {
            return false;
        }

        boolean masterChanged = event.previousState().nodes().isLocalNodeElectedClusterManager() == false;

        if (persistentTasksChanged(event)
            || event.nodesChanged()
            || event.routingTableChanged()
            || event.metadataChanged()
            || masterChanged) {

            for (PersistentTask<?> task : tasks.tasks()) {
                if (needsReassignment(task.getAssignment(), event.state().nodes())) {
                    Assignment assignment = createAssignment(task.getTaskName(), task.getParams(), event.state());
                    if (Objects.equals(assignment, task.getAssignment()) == false) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns true if any persistent task is unassigned.
     */
    private boolean isAnyTaskUnassigned(final PersistentTasksCustomMetadata tasks) {
        return tasks != null && tasks.tasks().stream().anyMatch(task -> task.getAssignment().isAssigned() == false);
    }

    /**
     * Evaluates the cluster state and tries to assign tasks to nodes.
     *
     * @param currentState the cluster state to analyze
     * @return an updated version of the cluster state
     */
    ClusterState reassignTasks(final ClusterState currentState) {
        ClusterState clusterState = currentState;

        final PersistentTasksCustomMetadata tasks = currentState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks != null) {
            logger.trace("reassigning {} persistent tasks", tasks.tasks().size());
            final DiscoveryNodes nodes = currentState.nodes();

            // We need to check if removed nodes were running any of the tasks and reassign them
            for (PersistentTask<?> task : tasks.tasks()) {
                if (needsReassignment(task.getAssignment(), nodes)) {
                    Assignment assignment = createAssignment(task.getTaskName(), task.getParams(), clusterState);
                    if (Objects.equals(assignment, task.getAssignment()) == false) {
                        logger.trace(
                            "reassigning task {} from node {} to node {}",
                            task.getId(),
                            task.getAssignment().getExecutorNode(),
                            assignment.getExecutorNode()
                        );
                        clusterState = update(clusterState, builder(clusterState).reassignTask(task.getId(), assignment));
                    } else {
                        logger.trace("ignoring task {} because assignment is the same {}", task.getId(), assignment);
                    }
                } else {
                    logger.trace("ignoring task {} because it is still running", task.getId());
                }
            }
        }
        return clusterState;
    }

    /** Returns true if the persistent tasks are not equal between the previous and the current cluster state **/
    static boolean persistentTasksChanged(final ClusterChangedEvent event) {
        String type = PersistentTasksCustomMetadata.TYPE;
        return Objects.equals(event.state().metadata().custom(type), event.previousState().metadata().custom(type)) == false;
    }

    /** Returns true if the task is not assigned or is assigned to a non-existing node */
    public static boolean needsReassignment(final Assignment assignment, final DiscoveryNodes nodes) {
        return (assignment.isAssigned() == false || nodes.nodeExists(assignment.getExecutorNode()) == false);
    }

    private static PersistentTasksCustomMetadata.Builder builder(ClusterState currentState) {
        return PersistentTasksCustomMetadata.builder(currentState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE));
    }

    private static ClusterState update(ClusterState currentState, PersistentTasksCustomMetadata.Builder tasksInProgress) {
        if (tasksInProgress.isChanged()) {
            return ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasksInProgress.build()))
                .build();
        } else {
            return currentState;
        }
    }

    private static Assignment unassignedAssignment(String reason) {
        return new Assignment(null, reason);
    }

    /**
     * Class to periodically try to reassign unassigned persistent tasks.
     */
    class PeriodicRechecker extends AbstractAsyncTask {

        PeriodicRechecker(TimeValue recheckInterval) {
            super(logger, threadPool, recheckInterval, false);
        }

        @Override
        protected boolean mustReschedule() {
            return true;
        }

        @Override
        public void runInternal() {
            if (clusterService.localNode().isClusterManagerNode()) {
                final ClusterState state = clusterService.state();
                logger.trace("periodic persistent task assignment check running for cluster state {}", state.getVersion());
                if (isAnyTaskUnassigned(state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE))) {
                    reassignPersistentTasks();
                }
            }
        }

        @Override
        public String toString() {
            return "persistent_task_recheck";
        }
    }
}
