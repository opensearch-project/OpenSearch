/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchAdmission;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import java.util.function.BooleanSupplier;

/**
 * Admits searches and scroll pages against workload group throttles, so the search actions depend only on the core
 * {@link SearchAdmission}. Active only while {@link WorkloadGroupService#isThrottlingActive()}.
 *
 * @opensearch.internal
 */
public final class WorkloadGroupSearchAdmission implements SearchAdmission {

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
        return workloadGroupService.isThrottlingActive();
    }

    @Override
    public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
        if (task instanceof WorkloadGroupTask == false) {
            listener.onResponse(NOOP_PERMIT);
            return;
        }
        // The service reports "nothing to charge" as a null permit; the admission contract wants a non-null one.
        final ActionListener<Releasable> permitListener = ActionListener.delegateFailure(
            listener,
            (l, permit) -> l.onResponse(permit == null ? NOOP_PERMIT : permit)
        );
        // A scroll page is never a nested search, so it never inherits a parent's charge.
        final BooleanSupplier parentAlreadyCounted = request instanceof SearchScrollRequest
            ? () -> false
            : () -> parentAlreadyCounted(task);
        workloadGroupService.acquireThrottlePermit((WorkloadGroupTask) task, parentAlreadyCounted, permitListener);
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
