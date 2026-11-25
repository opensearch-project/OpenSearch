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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Admits searches and scroll pages through the workload group {@linkplain WorkloadGroupAdmissionPolicy policies} (resource
 * limits, then throttle); the first refusal releases the permits already granted and fails the request. Creating a point in
 * time is admitted unchecked.
 *
 * @opensearch.internal
 */
public final class WorkloadGroupSearchAdmission implements SearchAdmission {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSearchAdmission.class);

    private final List<WorkloadGroupAdmissionPolicy> policies;

    public WorkloadGroupSearchAdmission(WorkloadGroupService workloadGroupService, ClusterService clusterService, TaskManager taskManager) {
        this(
            List.of(
                new ResourceLimitAdmissionPolicy(workloadGroupService),
                new ThrottleAdmissionPolicy(workloadGroupService, clusterService, taskManager)
            )
        );
    }

    WorkloadGroupSearchAdmission(List<WorkloadGroupAdmissionPolicy> policies) {
        this.policies = List.copyOf(policies);
    }

    @Override
    public boolean isActive() {
        for (WorkloadGroupAdmissionPolicy policy : policies) {
            if (policy.isActive()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void admit(Task task, ActionRequest request, ActionListener<Releasable> listener) {
        // Creating a point in time is admitted unchecked: it only opens a reader context on each shard and runs no query.
        // The searches that later use the point in time are checked and throttled.
        if (task instanceof WorkloadGroupTask == false || CreatePitAction.NAME.equals(task.getAction())) {
            listener.onResponse(NOOP_PERMIT);
            return;
        }
        admitFrom(0, (WorkloadGroupTask) task, request, new ArrayList<>(), listener);
    }

    /** Runs the policies from {@code index} on, each after the previous one admits, which may be asynchronous. */
    private void admitFrom(
        int index,
        WorkloadGroupTask task,
        ActionRequest request,
        List<Releasable> permits,
        ActionListener<Releasable> listener
    ) {
        for (int i = index; i < policies.size(); i++) {
            final WorkloadGroupAdmissionPolicy policy = policies.get(i);
            boolean applies;
            try {
                applies = policy.isActive() && policy.appliesTo(task, request);
            } catch (Exception e) {
                // Fail open, like the policies themselves: an error deciding whether a policy applies skips it.
                logger.debug("Skipping a workload group admission policy due to an error", e);
                applies = false;
            }
            if (applies == false) {
                continue;
            }
            final int next = i + 1;
            // Only the policy's first outcome counts: a policy may report a failure after responding.
            final AtomicBoolean completed = new AtomicBoolean();
            // Not ActionListener.wrap: a throw from the admitted request's listener is not a refusal.
            final ActionListener<Releasable> policyListener = new ActionListener<>() {
                @Override
                public void onResponse(Releasable permit) {
                    if (completed.compareAndSet(false, true)) {
                        if (permit != NOOP_PERMIT) {
                            permits.add(permit);
                        }
                        admitFrom(next, task, request, permits, listener);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (completed.compareAndSet(false, true)) {
                        Releasables.closeWhileHandlingException(permits);
                        task.markAdmissionRejected();
                        listener.onFailure(e);
                    } else {
                        logger.debug("Ignoring a failure reported after the admission policy completed", e);
                    }
                }
            };
            try {
                policy.admit(task, request, policyListener);
            } catch (Exception e) {
                // A throw before the policy completes is a refusal. After it completes, the throw came from a later policy or
                // the caller's listener, so it is not a refusal.
                if (completed.get()) {
                    throw e;
                }
                policyListener.onFailure(e);
            }
            return;
        }
        if (permits.isEmpty()) {
            listener.onResponse(NOOP_PERMIT);
        } else if (permits.size() == 1) {
            listener.onResponse(permits.get(0));
        } else {
            listener.onResponse(Releasables.wrap(permits));
        }
    }
}
