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
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import static org.opensearch.action.search.SearchAdmission.NOOP_PERMIT;

/**
 * Rejects a search whose workload group breaches a resource limit ({@link WorkloadGroupService#rejectIfNeeded}).
 *
 * @opensearch.internal
 */
final class ResourceLimitAdmissionPolicy implements WorkloadGroupAdmissionPolicy {

    private static final Logger logger = LogManager.getLogger(ResourceLimitAdmissionPolicy.class);

    private final WorkloadGroupService workloadGroupService;

    ResourceLimitAdmissionPolicy(WorkloadGroupService workloadGroupService) {
        this.workloadGroupService = workloadGroupService;
    }

    @Override
    public boolean isActive() {
        return workloadGroupService.isResourceLimitCheckActive();
    }

    /**
     * Applies to searches in a group whose resource limits can reject them. A scroll page belongs to a search checked when
     * the scroll began, so it is not checked here; its shard requests are still checked by
     * {@link WorkloadManagementTransportInterceptor}.
     */
    @Override
    public boolean appliesTo(WorkloadGroupTask task, ActionRequest request) {
        return request instanceof SearchScrollRequest == false && workloadGroupService.enforcesResourceLimits(task.getWorkloadGroupId());
    }

    @Override
    public void admit(WorkloadGroupTask task, ActionRequest request, ActionListener<Releasable> listener) {
        try {
            workloadGroupService.rejectIfNeeded(task.getWorkloadGroupId());
        } catch (OpenSearchRejectedExecutionException e) {
            listener.onFailure(e);
            return;
        } catch (Exception e) {
            // Fail open, like the throttle: an error evaluating resource limits must not fail the search.
            logger.debug(() -> "Skipping resource limit check for workload group [" + task.getWorkloadGroupId() + "] due to an error", e);
        }
        listener.onResponse(NOOP_PERMIT);
    }
}
