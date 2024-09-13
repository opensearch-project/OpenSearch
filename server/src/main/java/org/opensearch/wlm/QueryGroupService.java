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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateApplier;
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
import java.util.Set;

import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.TRACKED_RESOURCES;

/**
 * As of now this is a stub and main implementation PR will be raised soon.Coming PR will collate these changes with core QueryGroupService changes
 */
public class QueryGroupService extends AbstractLifecycleComponent
    implements
        ClusterStateApplier,
        TaskResourceTrackingService.TaskCompletionListener {
    // This map does not need to be concurrent since we will process the cluster state change serially and update
    // this map with new additions and deletions of entries. QueryGroupState is thread safe
    private final Map<String, QueryGroupState> queryGroupStateMap;
    private static final Logger logger = LogManager.getLogger(QueryGroupService.class);

    private final QueryGroupTaskCancellationService taskCancellationService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final WorkloadManagementSettings workloadManagementSettings;
    private final Set<QueryGroup> activeQueryGroups;
    private final Set<QueryGroup> deletedQueryGroups;
    private final NodeDuressTrackers nodeDuressTrackers;

    public QueryGroupService(
        QueryGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings
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
            new HashMap<>(),
            new HashSet<>(clusterService.state().metadata().queryGroups().values()),
            new HashSet<>()
        );
    }

    public QueryGroupService(
        QueryGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        NodeDuressTrackers nodeDuressTrackers,
        Map<String, QueryGroupState> stateMap,
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
        activeQueryGroups.forEach(queryGroup -> stateMap.putIfAbsent(queryGroup.get_id(), new QueryGroupState()));
        this.queryGroupStateMap = stateMap;
        this.queryGroupStateMap.put(QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get(), new QueryGroupState());
        taskCancellationService.setQueryGroupStateMapAccessor(this::getQueryGroupState);
    }

    /**
     * run at regular interval
     */
    protected void doRun() {
        if (workloadManagementSettings.getWlmMode() == WlmMode.DISABLED) {
            return;
        }
        taskCancellationService.refreshQueryGroups(activeQueryGroups, deletedQueryGroups);
        taskCancellationService.cancelTasks(nodeDuressTrackers::isNodeInDuress);
    }

    private QueryGroupState getQueryGroupState(final String queryGroupId) {
        return queryGroupStateMap.get(queryGroupId);
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
    public void applyClusterState(ClusterChangedEvent event) {
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
                this.activeQueryGroups.add(newQueryGroup);
                queryGroupStateMap.put(newQueryGroup.get_id(), new QueryGroupState());
            }
        }

        // Detect query groups deleted in the current cluster state
        for (String queryGroupName : previousQueryGroups.keySet()) {
            if (!currentQueryGroups.containsKey(queryGroupName)) {
                // Query group deleted
                QueryGroup deletedQueryGroup = previousQueryGroups.get(queryGroupName);
                // Perform any necessary actions with the deleted query group
                this.deletedQueryGroups.add(deletedQueryGroup);
                queryGroupStateMap.remove(deletedQueryGroup.get_id());
            }
        }
    }

    /**
     * updates the failure stats for the query group
     *
     * @param queryGroupId query group identifier
     */
    public void incrementFailuresFor(final String queryGroupId) {
        QueryGroupState queryGroupState = queryGroupStateMap.get(queryGroupId);
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
    public QueryGroupStats nodeStats() {
        final Map<String, QueryGroupStatsHolder> statsHolderMap = new HashMap<>();
        for (Map.Entry<String, QueryGroupState> queryGroupsState : queryGroupStateMap.entrySet()) {
            final String queryGroupId = queryGroupsState.getKey();
            final QueryGroupState currentState = queryGroupsState.getValue();

            statsHolderMap.put(queryGroupId, QueryGroupStatsHolder.from(currentState));
        }

        return new QueryGroupStats(statsHolderMap);
    }

    /**
     * @param queryGroupId query group identifier
     */
    public void rejectIfNeeded(String queryGroupId) {
        if (queryGroupId == null) return;
        QueryGroupState queryGroupState = queryGroupStateMap.get(queryGroupId);

        // This can happen if the request failed for a deleted query group
        // or new queryGroup is being created and has not been acknowledged yet
        if (queryGroupState == null) {
            return;
        }

        boolean reject = false;
        final StringBuilder reason = new StringBuilder();

        // rejections will not happen for SOFT mode QueryGroups
        QueryGroup queryGroup = activeQueryGroups.stream().filter(x -> x.get_id().equals(queryGroupId)).findFirst().get();

        if (queryGroup.getResiliencyMode() == MutableQueryGroupFragment.ResiliencyMode.SOFT) return;

        for (ResourceType resourceType : TRACKED_RESOURCES) {
            if (queryGroup.getResourceLimits().containsKey(resourceType)) {
                final double threshold = queryGroup.getResourceLimits().get(resourceType);
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
            throw new OpenSearchRejectedExecutionException("QueryGroup " + queryGroupId + " is already contended. " + reason.toString());
        }
    }

    public Set<QueryGroup> getActiveQueryGroups() {
        return activeQueryGroups;
    }

    public Set<QueryGroup> getDeletedQueryGroups() {
        return deletedQueryGroups;
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (!(task instanceof QueryGroupTask)) {
            return;
        }
        final QueryGroupTask queryGroupTask = (QueryGroupTask) task;
        final String queryGroupId = queryGroupTask.getQueryGroupId();
        queryGroupStateMap.get(queryGroupId).completions.inc();
    }
}
