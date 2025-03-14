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
import org.opensearch.cluster.metadata.QueryGroup;
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
import org.opensearch.wlm.cancellation.QueryGroupTaskCancellationService;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.stats.QueryGroupStats;
import org.opensearch.wlm.stats.QueryGroupStats.QueryGroupStatsHolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * As of now this is a stub and main implementation PR will be raised soon.Coming PR will collate these changes with core QueryGroupService changes
 * @opensearch.experimental
 */
public class QueryGroupService extends AbstractLifecycleComponent
    implements
        ClusterStateListener,
        TaskResourceTrackingService.TaskCompletionListener {

    private static final Logger logger = LogManager.getLogger(QueryGroupService.class);
    private final QueryGroupTaskCancellationService taskCancellationService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final WorkloadManagementSettings workloadManagementSettings;
    private Set<QueryGroup> activeQueryGroups;
    private final Set<QueryGroup> deletedQueryGroups;
    private final NodeDuressTrackers nodeDuressTrackers;
    private final QueryGroupsStateAccessor queryGroupsStateAccessor;

    public QueryGroupService(
        QueryGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        QueryGroupsStateAccessor queryGroupsStateAccessor
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

    public QueryGroupService(
        QueryGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        NodeDuressTrackers nodeDuressTrackers,
        QueryGroupsStateAccessor queryGroupsStateAccessor,
        Set<QueryGroup> activeQueryGroups,
        Set<QueryGroup> deletedQueryGroups
    ) {
        this.taskCancellationService = taskCancellationService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.workloadManagementSettings = workloadManagementSettings;
        this.nodeDuressTrackers = nodeDuressTrackers;
        this.activeQueryGroups = activeQueryGroups;
        this.deletedQueryGroups = deletedQueryGroups;
        this.queryGroupsStateAccessor = queryGroupsStateAccessor;
        activeQueryGroups.forEach(queryGroup -> this.queryGroupsStateAccessor.addNewQueryGroup(queryGroup.get_id()));
        this.queryGroupsStateAccessor.addNewQueryGroup(QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get());
        this.clusterService.addListener(this);
    }

    /**
     * run at regular interval
     */
    void doRun() {
        if (workloadManagementSettings.getWlmMode() == WlmMode.DISABLED) {
            return;
        }
        taskCancellationService.cancelTasks(nodeDuressTrackers::isNodeInDuress, activeQueryGroups, deletedQueryGroups);
        taskCancellationService.pruneDeletedQueryGroups(deletedQueryGroups);
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
        }, this.workloadManagementSettings.getQueryGroupServiceRunInterval(), ThreadPool.Names.GENERIC);
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
        Map<String, QueryGroup> previousQueryGroups = previousMetadata.queryGroups();
        Map<String, QueryGroup> currentQueryGroups = currentMetadata.queryGroups();

        // Detect new query groups added in the current cluster state
        for (String queryGroupName : currentQueryGroups.keySet()) {
            if (!previousQueryGroups.containsKey(queryGroupName)) {
                // New query group detected
                QueryGroup newQueryGroup = currentQueryGroups.get(queryGroupName);
                // Perform any necessary actions with the new query group
                queryGroupsStateAccessor.addNewQueryGroup(newQueryGroup.get_id());
            }
        }

        // Detect query groups deleted in the current cluster state
        for (String queryGroupName : previousQueryGroups.keySet()) {
            if (!currentQueryGroups.containsKey(queryGroupName)) {
                // Query group deleted
                QueryGroup deletedQueryGroup = previousQueryGroups.get(queryGroupName);
                // Perform any necessary actions with the deleted query group
                this.deletedQueryGroups.add(deletedQueryGroup);
                queryGroupsStateAccessor.removeQueryGroup(deletedQueryGroup.get_id());
            }
        }
        this.activeQueryGroups = new HashSet<>(currentMetadata.queryGroups().values());
    }

    /**
     * updates the failure stats for the query group
     *
     * @param queryGroupId query group identifier
     */
    public void incrementFailuresFor(final String queryGroupId) {
        QueryGroupState queryGroupState = queryGroupsStateAccessor.getQueryGroupState(queryGroupId);
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
    public QueryGroupStats nodeStats(Set<String> queryGroupIds, Boolean requestedBreached) {
        final Map<String, QueryGroupStatsHolder> statsHolderMap = new HashMap<>();
        Map<String, QueryGroupState> existingStateMap = queryGroupsStateAccessor.getQueryGroupStateMap();
        if (!queryGroupIds.contains("_all")) {
            for (String id : queryGroupIds) {
                if (!existingStateMap.containsKey(id)) {
                    throw new ResourceNotFoundException("QueryGroup with id " + id + " does not exist");
                }
            }
        }
        if (existingStateMap != null) {
            existingStateMap.forEach((queryGroupId, currentState) -> {
                boolean shouldInclude = queryGroupIds.contains("_all") || queryGroupIds.contains(queryGroupId);
                if (shouldInclude) {
                    if (requestedBreached == null || requestedBreached == resourceLimitBreached(queryGroupId, currentState)) {
                        statsHolderMap.put(queryGroupId, QueryGroupStatsHolder.from(currentState));
                    }
                }
            });
        }
        return new QueryGroupStats(statsHolderMap);
    }

    /**
     * @return if the QueryGroup breaches any resource limit based on the LastRecordedUsage
     */
    public boolean resourceLimitBreached(String id, QueryGroupState currentState) {
        QueryGroup queryGroup = clusterService.state().metadata().queryGroups().get(id);
        if (queryGroup == null) {
            throw new ResourceNotFoundException("QueryGroup with id " + id + " does not exist");
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

        if (queryGroupId == null || queryGroupId.equals(QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get())) return;
        QueryGroupState queryGroupState = queryGroupsStateAccessor.getQueryGroupState(queryGroupId);

        // This can happen if the request failed for a deleted query group
        // or new queryGroup is being created and has not been acknowledged yet or invalid query group id
        if (queryGroupState == null) {
            return;
        }

        // rejections will not happen for SOFT mode QueryGroups unless node is in duress
        Optional<QueryGroup> optionalQueryGroup = activeQueryGroups.stream().filter(x -> x.get_id().equals(queryGroupId)).findFirst();

        if (optionalQueryGroup.isPresent()
            && (optionalQueryGroup.get().getResiliencyMode() == MutableQueryGroupFragment.ResiliencyMode.SOFT
                && !nodeDuressTrackers.isNodeInDuress())) return;

        optionalQueryGroup.ifPresent(queryGroup -> {
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
                            .append(" limit is breaching for ENFORCED type QueryGroup: (")
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
                    "QueryGroup " + queryGroupId + " is already contended. " + reason.toString()
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

    public Set<QueryGroup> getActiveQueryGroups() {
        return activeQueryGroups;
    }

    public Set<QueryGroup> getDeletedQueryGroups() {
        return deletedQueryGroups;
    }

    /**
     * This method determines whether the task should be accounted by SBP if both features co-exist
     * @param t QueryGroupTask
     * @return whether or not SBP handle it
     */
    public boolean shouldSBPHandle(Task t) {
        QueryGroupTask task = (QueryGroupTask) t;
        boolean isInvalidQueryGroupTask = true;
        if (task.isQueryGroupSet() && !QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get().equals(task.getQueryGroupId())) {
            isInvalidQueryGroupTask = activeQueryGroups.stream()
                .noneMatch(queryGroup -> queryGroup.get_id().equals(task.getQueryGroupId()));
        }
        return workloadManagementSettings.getWlmMode() != WlmMode.ENABLED || isInvalidQueryGroupTask;
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (!(task instanceof QueryGroupTask) || !((QueryGroupTask) task).isQueryGroupSet()) {
            return;
        }
        final QueryGroupTask queryGroupTask = (QueryGroupTask) task;
        String queryGroupId = queryGroupTask.getQueryGroupId();

        // set the default queryGroupId if not existing in the active query groups
        String finalQueryGroupId = queryGroupId;
        boolean exists = activeQueryGroups.stream().anyMatch(queryGroup -> queryGroup.get_id().equals(finalQueryGroupId));

        if (!exists) {
            queryGroupId = QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get();
        }

        queryGroupsStateAccessor.getQueryGroupState(queryGroupId).totalCompletions.inc();
    }
}
