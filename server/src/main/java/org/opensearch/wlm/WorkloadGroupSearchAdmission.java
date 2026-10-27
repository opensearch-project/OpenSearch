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
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.SearchAdmission;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import java.util.function.BooleanSupplier;

/**
 * Admits searches against workload group resource limits and throttles, and scroll pages against throttles, so the search
 * actions depend only on the core {@link SearchAdmission}. A search is first rejected if its group breaches a resource
 * limit, then throttled; creating a point in time is admitted unchecked. Active only while
 * {@link WorkloadGroupService#isSearchAdmissionActive()}.
 *
 * @opensearch.internal
 */
public final class WorkloadGroupSearchAdmission implements SearchAdmission {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSearchAdmission.class);

    private final WorkloadGroupService workloadGroupService;
    private final ClusterService clusterService;
    private final TaskManager taskManager;

    public WorkloadGroupSearchAdmission(WorkloadGroupService workloadGroupService, ClusterService clusterService, TaskManager taskManager) {
        this.workloadGroupService = workloadGroupService;
        this.clusterService = clusterService;
        this.taskManager = taskManager;
    }

    @Override
    public boolean isActive() {
        return workloadGroupService.isSearchAdmissionActive();
    }

    @Override
    public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
        // Creating a point in time is admitted unchecked: it only opens a reader context on each shard and runs no query.
        // The searches that later use the point in time are checked and throttled.
        if (task instanceof WorkloadGroupTask == false || CreatePitAction.NAME.equals(task.getAction())) {
            listener.onResponse(NOOP_PERMIT);
            return;
        }
        final WorkloadGroupTask workloadGroupTask = (WorkloadGroupTask) task;
        // A scroll page belongs to a search checked when the scroll began, so only the throttle applies here; its shard
        // requests are still checked against resource limits by WorkloadManagementTransportInterceptor.
        if (request instanceof SearchScrollRequest == false) {
            try {
                workloadGroupService.rejectIfNeeded(workloadGroupTask.getWorkloadGroupId());
            } catch (OpenSearchRejectedExecutionException e) {
                workloadGroupTask.markAdmissionRejected();
                listener.onFailure(e);
                return;
            } catch (Exception e) {
                // Fail open, like the throttle: an error evaluating resource limits must not fail the search.
                logger.debug(
                    () -> "Skipping resource limit check for workload group ["
                        + workloadGroupTask.getWorkloadGroupId()
                        + "] due to an error",
                    e
                );
            }
        }
        // The service reports "nothing to charge" as a null permit; the admission contract wants a non-null one. Not
        // ActionListener.wrap: an exception from an admitted request's listener must not mark the task as rejected.
        final ActionListener<Releasable> permitListener = new ActionListener<>() {
            @Override
            public void onResponse(Releasable permit) {
                listener.onResponse(permit == null ? NOOP_PERMIT : permit);
            }

            @Override
            public void onFailure(Exception e) {
                workloadGroupTask.markAdmissionRejected();
                listener.onFailure(e);
            }
        };
        // A scroll page is never a nested search, so it never inherits a parent's charge.
        final BooleanSupplier parentAlreadyCounted = request instanceof SearchScrollRequest
            ? () -> false
            : () -> parentAlreadyCounted(task);
        workloadGroupService.acquireThrottlePermit(workloadGroupTask, parentAlreadyCounted, permitListener);
    }

    /**
     * Whether this request's parent task is already counted against a throttle bucket, so admission charges it
     * nothing (see {@link WorkloadGroupService#acquireThrottlePermit}); a nested coordinator search issued during rewrite
     * would otherwise make the request compete with itself. Only the immediate, local parent is inspected: the rewrite
     * client is the only thing that parents a counted coordinator search, and {@code TransportSearchAction} parents rewrite
     * searches on a task only when that task is counted, so a counted ancestor is always the immediate parent. Non-search
     * parents (an {@code _msearch} task, a reindex task) never carry the flag, so their child searches are each charged, as
     * intended. Remote parents are skipped because {@code TaskManager#getTask} is node-local.
     */
    private boolean parentAlreadyCounted(final Task task) {
        TaskId parentTaskId = task.getParentTaskId();
        if (parentTaskId == null || parentTaskId.isSet() == false) {
            return false;
        }
        if (clusterService.localNode().getId().equals(parentTaskId.getNodeId()) == false) {
            return false;
        }
        return taskManager.getTask(parentTaskId.getId()) instanceof WorkloadGroupTask parent && parent.isThrottleCounted();
    }
}
