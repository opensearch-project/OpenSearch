/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers.NodeDuressTracker;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.stats.WorkloadGroupStats;
import org.opensearch.wlm.stats.WorkloadGroupStats.WorkloadGroupStatsHolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * As of now this is a stub and main implementation PR will be raised soon.Coming PR will collate these changes with core WorkloadGroupService changes
 * @opensearch.experimental
 */
public class WorkloadGroupService extends AbstractLifecycleComponent
    implements
        ClusterStateListener,
        TaskResourceTrackingService.TaskCompletionListener {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupService.class);
    private final WorkloadGroupTaskCancellationService taskCancellationService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final WorkloadManagementSettings workloadManagementSettings;
    private Set<WorkloadGroup> activeWorkloadGroups;
    private final Set<WorkloadGroup> deletedWorkloadGroups;
    private final NodeDuressTrackers nodeDuressTrackers;
    private final WorkloadGroupsStateAccessor queryGroupsStateAccessor;

    public WorkloadGroupService(
        WorkloadGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        WorkloadGroupsStateAccessor queryGroupsStateAccessor
    ) {

        this(
            taskCancellationService,
            clusterService,
            threadPool,
            workloadManagementSettings,
            new NodeDuressTrackers(
                Map.of(
                    ResourceType.CPU,
                    new NodeDuressTracker(
                        () -> workloadManagementSettings.getNodeLevelCpuCancellationThreshold() < ProcessProbe.getInstance()
                            .getProcessCpuPercent() / 100.0,
                        workloadManagementSettings::getDuressStreak
                    ),
                    ResourceType.MEMORY,
                    new NodeDuressTracker(
                        () -> workloadManagementSettings.getNodeLevelMemoryCancellationThreshold() <= JvmStats.jvmStats()
                            .getMem()
                            .getHeapUsedPercent() / 100.0,
                        workloadManagementSettings::getDuressStreak
                    )
                )
            ),
            queryGroupsStateAccessor,
            new HashSet<>(),
            new HashSet<>()
        );
    }

    public WorkloadGroupService(
        WorkloadGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        NodeDuressTrackers nodeDuressTrackers,
        WorkloadGroupsStateAccessor queryGroupsStateAccessor,
        Set<WorkloadGroup> activeWorkloadGroups,
        Set<WorkloadGroup> deletedWorkloadGroups
    ) {
        this.taskCancellationService = taskCancellationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.workloadManagementSettings = workloadManagementSettings;
        this.nodeDuressTrackers = nodeDuressTrackers;
        this.activeWorkloadGroups = activeWorkloadGroups;
        this.deletedWorkloadGroups = deletedWorkloadGroups;
        this.queryGroupsStateAccessor = queryGroupsStateAccessor;
        activeWorkloadGroups.forEach(queryGroup -> this.queryGroupsStateAccessor.addNewWorkloadGroup(queryGroup.get_id()));
        this.queryGroupsStateAccessor.addNewWorkloadGroup(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        this.clusterService.addListener(this);
    }

    /**
     * run at regular interval
     */
    void doRun() {
        if (workloadManagementSettings.getWlmMode() == WlmMode.DISABLED) {
            return;
        }
        taskCancellationService.cancelTasks(nodeDuressTrackers::isNodeInDuress, activeWorkloadGroups, deletedWorkloadGroups);
        taskCancellationService.pruneDeletedWorkloadGroups(deletedWorkloadGroups);
    }

    /**
     * {@link AbstractLifecycleComponent} lifecycle method
     */
    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(() -> {
            try {
                doRun();
            } catch (Exception e) {
                logger.debug("Exception occurred in Query Sandbox service", e);
            }
        }, this.workloadManagementSettings.getWorkloadGroupServiceRunInterval(), ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // Retrieve the current and previous cluster states
        Metadata previousMetadata = event.previousState().metadata();
        Metadata currentMetadata = event.state().metadata();

        // Extract the query groups from both the current and previous cluster states
        Map<String, WorkloadGroup> previousWorkloadGroups = previousMetadata.queryGroups();
        Map<String, WorkloadGroup> currentWorkloadGroups = currentMetadata.queryGroups();

        // Detect new query groups added in the current cluster state
        for (String queryGroupName : currentWorkloadGroups.keySet()) {
            if (!previousWorkloadGroups.containsKey(queryGroupName)) {
                // New query group detected
                WorkloadGroup newWorkloadGroup = currentWorkloadGroups.get(queryGroupName);
                // Perform any necessary actions with the new query group
                queryGroupsStateAccessor.addNewWorkloadGroup(newWorkloadGroup.get_id());
            }
        }

        // Detect query groups deleted in the current cluster state
        for (String queryGroupName : previousWorkloadGroups.keySet()) {
            if (!currentWorkloadGroups.containsKey(queryGroupName)) {
                // Query group deleted
                WorkloadGroup deletedWorkloadGroup = previousWorkloadGroups.get(queryGroupName);
                // Perform any necessary actions with the deleted query group
                this.deletedWorkloadGroups.add(deletedWorkloadGroup);
                queryGroupsStateAccessor.removeWorkloadGroup(deletedWorkloadGroup.get_id());
            }
        }
        this.activeWorkloadGroups = new HashSet<>(currentMetadata.queryGroups().values());
    }

    /**
     * updates the failure stats for the query group
     *
     * @param queryGroupId query group identifier
     */
    public void incrementFailuresFor(final String queryGroupId) {
        WorkloadGroupState queryGroupState = queryGroupsStateAccessor.getWorkloadGroupState(queryGroupId);
        // This can happen if the request failed for a deleted query group
        // or new queryGroup is being created and has not been acknowledged yet
        if (queryGroupState == null) {
            return;
        }
        queryGroupState.failures.inc();
    }

    /**
     * @return node level query group stats
     */
    public WorkloadGroupStats nodeStats(Set<String> queryGroupIds, Boolean requestedBreached) {
        final Map<String, WorkloadGroupStatsHolder> statsHolderMap = new HashMap<>();
        Map<String, WorkloadGroupState> existingStateMap = queryGroupsStateAccessor.getWorkloadGroupStateMap();
        if (!queryGroupIds.contains("_all")) {
            for (String id : queryGroupIds) {
                if (!existingStateMap.containsKey(id)) {
                    throw new ResourceNotFoundException("WorkloadGroup with id " + id + " does not exist");
                }
            }
        }
        if (existingStateMap != null) {
            existingStateMap.forEach((queryGroupId, currentState) -> {
                boolean shouldInclude = queryGroupIds.contains("_all") || queryGroupIds.contains(queryGroupId);
                if (shouldInclude) {
                    if (requestedBreached == null || requestedBreached == resourceLimitBreached(queryGroupId, currentState)) {
                        statsHolderMap.put(queryGroupId, WorkloadGroupStatsHolder.from(currentState));
                    }
                }
            });
        }
        return new WorkloadGroupStats(statsHolderMap);
    }

    /**
     * @return if the WorkloadGroup breaches any resource limit based on the LastRecordedUsage
     */
    public boolean resourceLimitBreached(String id, WorkloadGroupState currentState) {
        WorkloadGroup queryGroup = clusterService.state().metadata().queryGroups().get(id);
        if (queryGroup == null) {
            throw new ResourceNotFoundException("WorkloadGroup with id " + id + " does not exist");
        }

        for (ResourceType resourceType : TRACKED_RESOURCES) {
            if (queryGroup.getResourceLimits().containsKey(resourceType)) {
                final double threshold = getNormalisedRejectionThreshold(queryGroup.getResourceLimits().get(resourceType), resourceType);
                final double lastRecordedUsage = currentState.getResourceState().get(resourceType).getLastRecordedUsage();
                if (threshold < lastRecordedUsage) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param queryGroupId query group identifier
     */
    public void rejectIfNeeded(String queryGroupId) {
        if (workloadManagementSettings.getWlmMode() != WlmMode.ENABLED) {
            return;
        }

        if (queryGroupId == null || queryGroupId.equals(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())) return;
        WorkloadGroupState queryGroupState = queryGroupsStateAccessor.getWorkloadGroupState(queryGroupId);

        // This can happen if the request failed for a deleted query group
        // or new queryGroup is being created and has not been acknowledged yet or invalid query group id
        if (queryGroupState == null) {
            return;
        }

        // rejections will not happen for SOFT mode WorkloadGroups unless node is in duress
        Optional<WorkloadGroup> optionalWorkloadGroup = activeWorkloadGroups.stream().filter(x -> x.get_id().equals(queryGroupId)).findFirst();

        if (optionalWorkloadGroup.isPresent()
            && (optionalWorkloadGroup.get().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.SOFT
                && !nodeDuressTrackers.isNodeInDuress())) return;

        optionalWorkloadGroup.ifPresent(queryGroup -> {
            boolean reject = false;
            final StringBuilder reason = new StringBuilder();
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                if (queryGroup.getResourceLimits().containsKey(resourceType)) {
                    final double threshold = getNormalisedRejectionThreshold(
                        queryGroup.getResourceLimits().get(resourceType),
                        resourceType
                    );
                    final double lastRecordedUsage = queryGroupState.getResourceState().get(resourceType).getLastRecordedUsage();
                    if (threshold < lastRecordedUsage) {
                        reject = true;
                        reason.append(resourceType)
                            .append(" limit is breaching for ENFORCED type WorkloadGroup: (")
                            .append(threshold)
                            .append(" < ")
                            .append(lastRecordedUsage)
                            .append("). ");
                        queryGroupState.getResourceState().get(resourceType).rejections.inc();
                        // should not double count even if both the resource limits are breaching
                        break;
                    }
                }
            }
            if (reject) {
                queryGroupState.totalRejections.inc();
                throw new OpenSearchRejectedExecutionException(
                    "WorkloadGroup " + queryGroupId + " is already contended. " + reason.toString()
                );
            }
        });
    }

    private double getNormalisedRejectionThreshold(double limit, ResourceType resourceType) {
        if (resourceType == ResourceType.CPU) {
            return limit * workloadManagementSettings.getNodeLevelCpuRejectionThreshold();
        } else if (resourceType == ResourceType.MEMORY) {
            return limit * workloadManagementSettings.getNodeLevelMemoryRejectionThreshold();
        }
        throw new IllegalArgumentException(resourceType + " is not supported in WLM yet");
    }

    public Set<WorkloadGroup> getActiveWorkloadGroups() {
        return activeWorkloadGroups;
    }

    public Set<WorkloadGroup> getDeletedWorkloadGroups() {
        return deletedWorkloadGroups;
    }

    /**
     * This method determines whether the task should be accounted by SBP if both features co-exist
     * @param t WorkloadGroupTask
     * @return whether or not SBP handle it
     */
    public boolean shouldSBPHandle(Task t) {
        WorkloadGroupTask task = (WorkloadGroupTask) t;
        boolean isInvalidWorkloadGroupTask = true;
        if (task.isWorkloadGroupSet() && !WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get().equals(task.getWorkloadGroupId())) {
            isInvalidWorkloadGroupTask = activeWorkloadGroups.stream()
                .noneMatch(queryGroup -> queryGroup.get_id().equals(task.getWorkloadGroupId()));
        }
        return workloadManagementSettings.getWlmMode() != WlmMode.ENABLED || isInvalidWorkloadGroupTask;
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (!(task instanceof WorkloadGroupTask) || !((WorkloadGroupTask) task).isWorkloadGroupSet()) {
            return;
        }
        final WorkloadGroupTask queryGroupTask = (WorkloadGroupTask) task;
        String queryGroupId = queryGroupTask.getWorkloadGroupId();

        // set the default queryGroupId if not existing in the active query groups
        String finalWorkloadGroupId = queryGroupId;
        boolean exists = activeWorkloadGroups.stream().anyMatch(queryGroup -> queryGroup.get_id().equals(finalWorkloadGroupId));

        if (!exists) {
            queryGroupId = WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get();
        }

        queryGroupsStateAccessor.getWorkloadGroupState(queryGroupId).totalCompletions.inc();
    }
}
