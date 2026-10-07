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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
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
import java.util.function.BooleanSupplier;

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

    /**
     * Separator between the segments of a throttle bucket key,
     * {@code <workload_group_id><delimiter><dimension><delimiter><dimension_value>}. Safe as a separator because no
     * segment can contain it: the id is a base64 UUID, the dimension is the internal group scope or one of
     * {@link WorkloadGroupThrottleSettings#ALLOWED_BY_VALUES}, and only the trailing segment is caller-supplied.
     */
    static final String BUCKET_KEY_DELIMITER = ":";

    private final WorkloadGroupTaskCancellationService taskCancellationService;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final WorkloadManagementSettings workloadManagementSettings;
    private Set<WorkloadGroup> activeWorkloadGroups;
    private final Set<WorkloadGroup> deletedWorkloadGroups;
    private final NodeDuressTrackers nodeDuressTrackers;
    private final WorkloadGroupsStateAccessor workloadGroupsStateAccessor;
    // Node-local in-flight counters per throttle bucket.
    private final WorkloadGroupThrottleTracker throttleTracker = new WorkloadGroupThrottleTracker();
    private volatile WorkloadGroupSharedThrottleService sharedThrottleService;

    public WorkloadGroupService(
        WorkloadGroupTaskCancellationService taskCancellationService,
        ClusterService clusterService,
        ThreadPool threadPool,
        WorkloadManagementSettings workloadManagementSettings,
        WorkloadGroupsStateAccessor workloadGroupsStateAccessor
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
            workloadGroupsStateAccessor,
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
        WorkloadGroupsStateAccessor workloadGroupsStateAccessor,
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
        this.workloadGroupsStateAccessor = workloadGroupsStateAccessor;
        activeWorkloadGroups.forEach(workloadGroup -> this.workloadGroupsStateAccessor.addNewWorkloadGroup(workloadGroup.get_id()));
        this.workloadGroupsStateAccessor.addNewWorkloadGroup(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
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
                logger.debug("Exception occurred in Workload Group service", e);
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

        // Extract the workload groups from both the current and previous cluster states
        Map<String, WorkloadGroup> previousWorkloadGroups = previousMetadata.workloadGroups();
        Map<String, WorkloadGroup> currentWorkloadGroups = currentMetadata.workloadGroups();

        // Detect new workload groups added in the current cluster state
        for (String workloadGroupName : currentWorkloadGroups.keySet()) {
            if (!previousWorkloadGroups.containsKey(workloadGroupName)) {
                // New workload group detected
                WorkloadGroup newWorkloadGroup = currentWorkloadGroups.get(workloadGroupName);
                // Perform any necessary actions with the new workload group
                workloadGroupsStateAccessor.addNewWorkloadGroup(newWorkloadGroup.get_id());
            }
        }

        // Detect workload groups deleted in the current cluster state
        for (String workloadGroupName : previousWorkloadGroups.keySet()) {
            if (!currentWorkloadGroups.containsKey(workloadGroupName)) {
                // Workload group deleted
                WorkloadGroup deletedWorkloadGroup = previousWorkloadGroups.get(workloadGroupName);
                // Perform any necessary actions with the deleted workload group
                this.deletedWorkloadGroups.add(deletedWorkloadGroup);
                workloadGroupsStateAccessor.removeWorkloadGroup(deletedWorkloadGroup.get_id());
            }
        }
        this.activeWorkloadGroups = new HashSet<>(currentMetadata.workloadGroups().values());
    }

    /**
     * updates the failure stats for the workload group
     *
     * @param workloadGroupId workload group identifier
     */
    public void incrementFailuresFor(final String workloadGroupId) {
        WorkloadGroupState workloadGroupState = workloadGroupsStateAccessor.getWorkloadGroupState(workloadGroupId);
        // This can happen if the request failed for a deleted workload group
        // or new workloadGroup is being created and has not been acknowledged yet
        if (workloadGroupState == null) {
            return;
        }
        workloadGroupState.failures.inc();
    }

    /**
     * @return node level workload group stats
     */
    public WorkloadGroupStats nodeStats(Set<String> workloadGroupIds, Boolean requestedBreached) {
        final Map<String, WorkloadGroupStatsHolder> statsHolderMap = new HashMap<>();
        Map<String, WorkloadGroupState> existingStateMap = workloadGroupsStateAccessor.getWorkloadGroupStateMap();
        if (!workloadGroupIds.contains("_all")) {
            for (String id : workloadGroupIds) {
                if (!existingStateMap.containsKey(id)) {
                    throw new ResourceNotFoundException("WorkloadGroup with id " + id + " does not exist");
                }
            }
        }
        if (existingStateMap != null) {
            existingStateMap.forEach((workloadGroupId, currentState) -> {
                boolean shouldInclude = workloadGroupIds.contains("_all") || workloadGroupIds.contains(workloadGroupId);
                if (shouldInclude) {
                    if (requestedBreached == null || requestedBreached == resourceLimitBreached(workloadGroupId, currentState)) {
                        statsHolderMap.put(workloadGroupId, WorkloadGroupStatsHolder.from(currentState));
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
        WorkloadGroup workloadGroup = clusterService.state().metadata().workloadGroups().get(id);
        if (workloadGroup == null) {
            throw new ResourceNotFoundException("WorkloadGroup with id " + id + " does not exist");
        }

        for (ResourceType resourceType : TRACKED_RESOURCES) {
            if (workloadGroup.getResourceLimits().containsKey(resourceType)) {
                final double threshold = getNormalisedRejectionThreshold(workloadGroup.getResourceLimits().get(resourceType), resourceType);
                final double lastRecordedUsage = currentState.getResourceState().get(resourceType).getLastRecordedUsage();
                if (threshold < lastRecordedUsage) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param workloadGroupId workload group identifier
     */
    public void rejectIfNeeded(String workloadGroupId) {
        if (workloadManagementSettings.getWlmMode() != WlmMode.ENABLED) {
            return;
        }

        if (workloadGroupId == null || workloadGroupId.equals(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())) return;
        WorkloadGroupState workloadGroupState = workloadGroupsStateAccessor.getWorkloadGroupState(workloadGroupId);

        // This can happen if the request failed for a deleted workload group
        // or new workloadGroup is being created and has not been acknowledged yet or invalid workload group id
        if (workloadGroupState == null) {
            return;
        }

        // rejections will not happen for SOFT mode WorkloadGroups unless node is in duress
        Optional<WorkloadGroup> optionalWorkloadGroup = activeWorkloadGroups.stream()
            .filter(x -> x.get_id().equals(workloadGroupId))
            .findFirst();

        if (optionalWorkloadGroup.isPresent()
            && (optionalWorkloadGroup.get().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.SOFT
                && !nodeDuressTrackers.isNodeInDuress())) return;

        optionalWorkloadGroup.ifPresent(workloadGroup -> {
            boolean reject = false;
            final StringBuilder reason = new StringBuilder();
            for (ResourceType resourceType : TRACKED_RESOURCES) {
                if (workloadGroup.getResourceLimits().containsKey(resourceType)) {
                    final double threshold = getNormalisedRejectionThreshold(
                        workloadGroup.getResourceLimits().get(resourceType),
                        resourceType
                    );
                    final double lastRecordedUsage = workloadGroupState.getResourceState().get(resourceType).getLastRecordedUsage();
                    if (threshold < lastRecordedUsage) {
                        reject = true;
                        reason.append(resourceType)
                            .append(" limit is breaching for workload group ")
                            .append(workloadGroup.get_id())
                            .append(", ")
                            .append(threshold)
                            .append(" < ")
                            .append(lastRecordedUsage)
                            .append(", wlm mode is ")
                            .append(workloadGroup.getResiliencyMode())
                            .append(". ");
                        workloadGroupState.getResourceState().get(resourceType).rejections.inc();
                        // should not double count even if both the resource limits are breaching
                        break;
                    }
                }
            }
            if (reject) {
                workloadGroupState.totalRejections.inc();
                throw new OpenSearchRejectedExecutionException(
                    "WorkloadGroup " + workloadGroupId + " is already contended. " + reason.toString()
                );
            }
        });
    }

    /** Late-binds the cluster-level ({@code shared_limit}) tier, which needs the transport service built after this one. */
    public void setSharedThrottleService(WorkloadGroupSharedThrottleService sharedThrottleService) {
        this.sharedThrottleService = sharedThrottleService;
    }

    /**
     * Admits a search against the node-local allowance ({@code node_limit}), then the shared allowance
     * ({@code shared_limit}, a cluster-wide overflow pool on top of every node's local allowance) if the local bucket is
     * full. Only shared admission can complete asynchronously. A counted parent exempts nested searches at both tiers.
     * MONITOR records would-be throttling without rejecting; SOFT and ENFORCED reject at the effective limit.
     *
     * @param task the search task to mark as counted when admitted against a throttle
     * @param parentAlreadyCounted supplies whether this task inherits an existing throttle charge
     * @param listener receives a permit to release on completion, {@code null} when no permit is needed, or a 429
     */
    public void acquireThrottlePermit(WorkloadGroupTask task, BooleanSupplier parentAlreadyCounted, ActionListener<Releasable> listener) {
        final ThrottlePlan plan;
        try {
            plan = resolveThrottlePlan(task.getWorkloadGroupId(), task.getThrottlePrincipal());
        } catch (Exception e) {
            logger.debug(() -> "Skipping throttle for workload group [" + task.getWorkloadGroupId() + "] due to an error", e);
            listener.onResponse(null);
            return;
        }
        if (plan == null) {
            listener.onResponse(null);
            return;
        }
        final boolean inherited;
        try {
            inherited = parentAlreadyCounted.getAsBoolean();
        } catch (Exception e) {
            logger.debug(() -> "Skipping throttle for workload group [" + task.getWorkloadGroupId() + "] due to an error", e);
            listener.onResponse(null);
            return;
        }
        if (inherited) {
            task.setThrottleCounted(true);
            listener.onResponse(null);
            return;
        }

        if (plan.nodeLimit() > 0) {
            final Releasable localPermit;
            try {
                localPermit = throttleTracker.tryAcquire(plan.bucketKey(), plan.nodeLimit());
            } catch (Exception e) {
                logger.debug(
                    () -> "Skipping node-level throttle for workload group [" + task.getWorkloadGroupId() + "] due to an error",
                    e
                );
                listener.onResponse(null);
                return;
            }
            if (localPermit != null) {
                task.setThrottleCounted(true);
                listener.onResponse(localPermit);
                return;
            }
        }

        if (plan.sharedLimit() < 1) {
            deliverThrottleBreach(plan, false, task, listener);
            return;
        }
        WorkloadGroupSharedThrottleService sharedService = sharedThrottleService;
        if (sharedService == null) {
            // Defensive (only test-built instances lack it): fail closed like any unanswerable shared acquire.
            deliverThrottleBreach(plan, true, task, listener);
            return;
        }
        final boolean monitor = plan.workloadGroup().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR;
        // MONITOR proceeds on every outcome, so the shared tier never delivers a search-pool rejection for it.
        sharedService.acquireAsync(plan.bucketKey(), plan.sharedLimit(), monitor, ActionListener.wrap(permit -> {
            task.setThrottleCounted(true);
            listener.onResponse(permit);
        }, e -> {
            if (WorkloadGroupSharedThrottleService.isDenial(e)) {
                deliverThrottleBreach(plan, false, task, listener);
            } else if (e instanceof OpenSearchRejectedExecutionException && monitor == false) {
                // The search pool rejected the hand-off (permit already released): a real 429, not a throttle breach.
                listener.onFailure(e);
            } else {
                // The shared tier could not answer: fail closed so shared_limit is never silently exceeded (MONITOR admits).
                if (WorkloadGroupSharedThrottleService.isUnavailable(e) == false) {
                    logger.debug(() -> "Shared throttle for workload group [" + task.getWorkloadGroupId() + "] failed", e);
                }
                deliverThrottleBreach(plan, true, task, listener);
            }
        }));
    }

    /**
     * Wraps {@code listener} so the request's throttle permit is released <em>before</em> the listener is notified: a
     * completion listener may synchronously start new work in the same bucket (an {@code _msearch} dispatches its next
     * sub-search from the previous one's response handler), so releasing after would spuriously 429 a request the
     * coordinator deliberately serialized. Accepted: for a remote shared permit this only sends the fire-and-forget
     * RELEASE, so a follow-up acquire can reach the owner before that release and see the bucket full, a rare false
     * 429 under saturation. The close is guarded so a failed release can never turn a successful search into an error.
     *
     * @param listener       the listener to notify once the permit has been released (or, if remote, its release sent)
     * @param throttlePermit the permit acquired by
     *                       {@link #acquireThrottlePermit(WorkloadGroupTask, BooleanSupplier, ActionListener)}
     */
    public static <T> ActionListener<T> releaseThrottlePermitBeforeCompletion(
        final ActionListener<T> listener,
        final Releasable throttlePermit
    ) {
        return ActionListener.runBefore(listener, () -> {
            try {
                throttlePermit.close();
            } catch (Exception e) {
                logger.warn("Failed to release WLM throttle permit", e);
            }
        });
    }

    private void deliverThrottleBreach(
        ThrottlePlan plan,
        boolean sharedUnavailable,
        WorkloadGroupTask task,
        ActionListener<Releasable> listener
    ) {
        try {
            onThrottleBreach(plan, sharedUnavailable, task);
        } catch (OpenSearchRejectedExecutionException e) {
            listener.onFailure(e);
            return;
        }
        listener.onResponse(null);
    }

    private void onThrottleBreach(ThrottlePlan plan, boolean sharedUnavailable, WorkloadGroupTask task) {
        if (plan.workloadGroup().getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR) {
            if (logger.isDebugEnabled()) {
                logger.debug("Request would be throttled (monitor mode, not rejected): {}.", throttleDescription(plan, sharedUnavailable));
            }
            recordThrottleStat(plan.workloadGroup().get_id(), true);
            task.setThrottleCounted(true);
            return;
        }
        recordThrottleStat(plan.workloadGroup().get_id(), false);
        throw new OpenSearchRejectedExecutionException("Request throttled: " + throttleDescription(plan, sharedUnavailable) + ".");
    }

    private static String throttleDescription(ThrottlePlan plan, boolean sharedUnavailable) {
        String target = "workload group [" + plan.workloadGroup().getName() + "]";
        if (WorkloadGroupThrottleSettings.GROUP_SCOPE.equals(plan.by()) == false) {
            target += " for " + plan.by() + " [" + plan.byValue() + "]";
        }
        // A breach means every configured tier is exhausted, so name each one; an unavailable shared tier was not checked.
        if (sharedUnavailable) {
            String sharedUnchecked = "could not check its shared limit of "
                + plan.sharedLimit()
                + " concurrent requests (cluster-wide throttle unavailable)";
            return plan.nodeLimit() > 0
                ? target + " reached its per-node limit of " + plan.nodeLimit() + " concurrent requests and " + sharedUnchecked
                : target + " " + sharedUnchecked;
        } else if (plan.nodeLimit() > 0 && plan.sharedLimit() > 0) {
            return target
                + " reached its per-node limit of "
                + plan.nodeLimit()
                + " and shared limit of "
                + plan.sharedLimit()
                + " concurrent requests";
        } else if (plan.nodeLimit() > 0) {
            return target + " reached its per-node limit of " + plan.nodeLimit() + " concurrent requests";
        }
        return target + " reached its shared limit of " + plan.sharedLimit() + " concurrent requests";
    }

    private ThrottlePlan resolveThrottlePlan(String workloadGroupId, String principal) {
        if (workloadManagementSettings.getWlmMode() != WlmMode.ENABLED) {
            return null;
        }
        if (workloadGroupId == null || workloadGroupId.equals(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())) {
            return null;
        }
        WorkloadGroup workloadGroup = getWorkloadGroupById(workloadGroupId);
        if (workloadGroup == null) {
            return null;
        }
        Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
        if (throttling == null || throttling.isEmpty()) {
            return null;
        }
        int nodeLimit = WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling);
        int sharedLimit = WorkloadGroupThrottleSettings.SHARED_LIMIT.get(throttling);
        if (nodeLimit < 1 && sharedLimit < 1) {
            return null;
        }
        String by = WorkloadGroupThrottleSettings.getEffectiveBy(throttling);
        String byValue = resolveThrottleByValue(by, principal);
        if (byValue == null) {
            return null;
        }
        String bucketKey = workloadGroupId + BUCKET_KEY_DELIMITER + by + BUCKET_KEY_DELIMITER + byValue;
        return new ThrottlePlan(workloadGroup, bucketKey, by, byValue, nodeLimit, sharedLimit);
    }

    private record ThrottlePlan(WorkloadGroup workloadGroup, String bucketKey, String by, String byValue, int nodeLimit, int sharedLimit) {
    }

    /**
     * Bumps {@code total_would_throttle} ({@code wouldThrottleOnly == true}, MONITOR observed) or {@code total_throttled}
     * (actual rejection). Swallows stats failures so they can't mask the request's outcome, and uses the raw state map so a
     * not-yet-registered group isn't misattributed to DEFAULT.
     */
    private void recordThrottleStat(String workloadGroupId, boolean wouldThrottleOnly) {
        try {
            WorkloadGroupState workloadGroupState = workloadGroupsStateAccessor.getWorkloadGroupStateMap().get(workloadGroupId);
            if (workloadGroupState != null) {
                if (wouldThrottleOnly) {
                    workloadGroupState.totalWouldThrottle.inc();
                } else {
                    workloadGroupState.totalThrottled.inc();
                }
            }
        } catch (Exception statsException) {
            logger.warn("Failed to record throttle stat for workload group [" + workloadGroupId + "]", statsException);
        }
    }

    /**
     * Resolves the bucket key value: {@link WorkloadGroupThrottleSettings#GROUP_SCOPE} for whole-group throttling, else the
     * principal's {@code username}/{@code role} value. When a principal carries several values for the subfield (a user in
     * many roles), the lexicographically smallest is chosen so the same caller lands in a stable bucket.
     *
     * @return the bucket dimension value, or {@code null} to fail open when the principal has no usable value
     */
    private String resolveThrottleByValue(String by, String principal) {
        if (WorkloadGroupThrottleSettings.GROUP_SCOPE.equals(by)) {
            return WorkloadGroupThrottleSettings.GROUP_SCOPE;
        }
        if (principal == null || principal.isEmpty()) {
            return null;
        }
        // Trim the token, not the value: trimming past the delimiter would fold "username|alice " into alice's bucket.
        String subfieldPrefix = by + WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_SUBFIELD_DELIMITER;
        String selected = null;
        for (String token : principal.split(WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER)) {
            String trimmed = token.trim();
            if (trimmed.startsWith(subfieldPrefix)) {
                String value = trimmed.substring(subfieldPrefix.length());
                if (value.isEmpty() == false && (selected == null || value.compareTo(selected) < 0)) {
                    selected = value;
                }
            }
        }
        return selected;
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

    /**
     * Returns the workload group with the given ID, or null if not found.
     * @param workloadGroupId the workload group identifier
     * @return the WorkloadGroup or null
     */
    public WorkloadGroup getWorkloadGroupById(String workloadGroupId) {
        return clusterService.state().metadata().workloadGroups().get(workloadGroupId);
    }

    /**
     * Returns the workload group attached to the calling thread context, or null if the current
     * request does not map to a workload group (no header set, or the referenced group does not
     * exist).
     */
    public WorkloadGroup getCurrentWorkloadGroup() {
        String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        if (workloadGroupId == null) {
            return null;
        }
        return getWorkloadGroupById(workloadGroupId);
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
                .noneMatch(workloadGroup -> workloadGroup.get_id().equals(task.getWorkloadGroupId()));
        }
        return workloadManagementSettings.getWlmMode() != WlmMode.ENABLED || isInvalidWorkloadGroupTask;
    }

    @Override
    public void onTaskCompleted(Task task) {
        if (!(task instanceof WorkloadGroupTask workloadGroupTask) || !workloadGroupTask.isWorkloadGroupSet()) {
            return;
        }
        String workloadGroupId = workloadGroupTask.getWorkloadGroupId();

        // set the default workloadGroupId if not existing in the active workload groups
        String finalWorkloadGroupId = workloadGroupId;
        boolean exists = activeWorkloadGroups.stream().anyMatch(workloadGroup -> workloadGroup.get_id().equals(finalWorkloadGroupId));

        if (!exists) {
            workloadGroupId = WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get();
        }

        workloadGroupsStateAccessor.getWorkloadGroupState(workloadGroupId).totalCompletions.inc();
    }
}
