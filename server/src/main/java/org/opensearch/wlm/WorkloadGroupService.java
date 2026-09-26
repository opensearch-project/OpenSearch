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
import java.util.function.Consumer;

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
    // Node-local in-flight throttle counters, keyed by throttle bucket. No cross-node coordination in this tier.
    private final WorkloadGroupThrottleTracker throttleTracker = new WorkloadGroupThrottleTracker();

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

    /**
     * Test seam over {@link #acquireThrottleOrReject(WorkloadGroupTask, BooleanSupplier)} exercising bucket resolution and
     * the limit directly, without a task or thread context. Package-private: production callers use the task-aware variant
     * so the request is marked counted and re-entrancy is handled.
     *
     * @param workloadGroupId the workload group the request is assigned to
     * @param principal       the caller's joined principal tokens, or {@code null} (see resolver)
     * @return a permit to close on request completion, or {@code null} if not throttled
     * @throws OpenSearchRejectedExecutionException if the bucket is already at its node limit
     */
    Releasable acquireThrottleOrReject(String workloadGroupId, String principal) {
        return acquireThrottleOrReject(workloadGroupId, principal, () -> false, counted -> {});
    }

    /**
     * Acquires one node-level throttle permit for the request, or returns {@code null} (nothing to release) when the
     * request is not throttled: WLM disabled, default/unknown group, no {@code node_limit}, no resolvable bucket (see
     * {@link #resolveThrottleByValue}), or a parent task whose work is already counted. The bucket is group-scoped when
     * {@code throttling.by} is absent, or subdivided by its explicit {@code username}/{@code role} value.
     *
     * @param task                 the request's task; marked as counted in both the acquired and the exempted case, so the
     *                             accounting propagates to its own nested searches
     * @param parentAlreadyCounted supplies whether the parent task is already counted, so a nested coordinator search is
     *                             not charged twice. Supplied lazily so the parent lookup runs only for a throttling group,
     *                             not on every search
     * @return a permit to close on request completion, or {@code null} if not throttled
     * @throws OpenSearchRejectedExecutionException if the bucket is already at its node limit
     */
    public Releasable acquireThrottleOrReject(WorkloadGroupTask task, BooleanSupplier parentAlreadyCounted) {
        return acquireThrottleOrReject(
            task.getWorkloadGroupId(),
            task.getThrottlePrincipal(),
            parentAlreadyCounted,
            task::setThrottleCounted
        );
    }

    /**
     * Wraps {@code listener} so the request's throttle permit is released <em>before</em> the listener is notified: a
     * completion listener may synchronously start new work in the same bucket (an {@code _msearch} dispatches its next
     * sub-search from the previous one's response handler), so releasing after would spuriously 429 a request the
     * coordinator deliberately serialized. The close is guarded so a failed release can never turn a successful search into
     * a client-visible error.
     *
     * @param listener       the listener to notify once the permit has been given back
     * @param throttlePermit the permit acquired by {@link #acquireThrottleOrReject(WorkloadGroupTask, BooleanSupplier)}
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

    private Releasable acquireThrottleOrReject(
        String workloadGroupId,
        String principal,
        BooleanSupplier parentAlreadyCounted,
        Consumer<Boolean> onCounted
    ) {
        if (workloadManagementSettings.getWlmMode() != WlmMode.ENABLED) {
            return null;
        }
        if (workloadGroupId == null || workloadGroupId.equals(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())) {
            return null;
        }
        try {
            WorkloadGroup workloadGroup = getWorkloadGroupById(workloadGroupId);
            if (workloadGroup == null) {
                return null;
            }
            Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
            // Cheap early-out so a group that never configured throttling does not pay for parsing an absent limit on
            // every search request.
            if (throttling == null || throttling.isEmpty()) {
                return null;
            }
            int nodeLimit = WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling);
            if (nodeLimit < 1) {
                return null;
            }
            // Re-entrancy: a nested coordinator search (e.g. a terms lookup's subquery during rewrite) inherits its
            // already-counted parent's charge instead of taking a second permit. Evaluated here, past the early-outs, so the
            // parent-task lookup stays off searches whose group does not throttle. Marked counted (transitively) though no
            // permit is taken; release is tied to the returned Releasable, not this flag.
            if (parentAlreadyCounted.getAsBoolean()) {
                onCounted.accept(true);
                return null;
            }
            String by = WorkloadGroupThrottleSettings.getEffectiveBy(throttling);
            // A null value means the request can't be bucketed (e.g. username/role with no principal) -> fail open.
            String byValue = resolveThrottleByValue(by, principal);
            if (byValue == null) {
                return null;
            }
            String bucketKey = workloadGroupId + BUCKET_KEY_DELIMITER + by + BUCKET_KEY_DELIMITER + byValue;

            Releasable permit = throttleTracker.tryAcquire(bucketKey, nodeLimit);
            if (permit != null) {
                onCounted.accept(true);
                return permit;
            }

            // Over the limit. Name the group and the throttle dimension so both the log line and the 429 identify who
            // was throttled -- the bucket key alone is opaque to an operator.
            String target = "workload group [" + workloadGroup.getName() + "]";
            if (WorkloadGroupThrottleSettings.GROUP_SCOPE.equals(by) == false) {
                target += " for " + by + " [" + byValue + "]";
            }
            if (workloadGroup.getResiliencyMode() == MutableWorkloadGroupFragment.ResiliencyMode.MONITOR) {
                // MONITOR observes only: count the would-be rejection against total_would_throttle (a signal for sizing
                // node_limit before enforcing) and admit, leaving total_throttled meaning "actually rejected". DEBUG since
                // it fires once per would-be-throttled request.
                logger.debug(
                    "Request would be throttled (monitor mode, not rejected): {} reached its per-node limit of {} concurrent requests.",
                    target,
                    nodeLimit
                );
                recordThrottleStat(workloadGroupId, true);
                return null;
            }
            // Record the rejection without ever letting a stats failure swallow the 429.
            recordThrottleStat(workloadGroupId, false);
            throw new OpenSearchRejectedExecutionException(
                "Request throttled: " + target + " reached its per-node limit of " + nodeLimit + " concurrent requests."
            );
        } catch (OpenSearchRejectedExecutionException e) {
            throw e; // the intended 429
        } catch (Exception e) {
            // A bug in the throttle path must never fail an otherwise-valid search, so fail open. DEBUG, not WARN: a
            // deterministic failure in here would otherwise emit a stack trace at the full query rate.
            logger.debug(() -> "Skipping node-level throttle for workload group [" + workloadGroupId + "] due to an error", e);
            return null;
        }
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
