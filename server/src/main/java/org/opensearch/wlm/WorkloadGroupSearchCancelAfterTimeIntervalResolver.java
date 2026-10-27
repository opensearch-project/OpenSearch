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
import org.opensearch.action.search.SearchCancelAfterTimeIntervalResolver;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.listeners.WorkloadGroupRequestOperationListener;

/**
 * Applies the {@code search.cancel_after_time_interval} of a search's workload group to the coordinator's cancellation timer,
 * so the search actions depend only on the core {@link SearchCancelAfterTimeIntervalResolver}. The timer is armed before
 * admission, before {@link WorkloadGroupRequestOperationListener} writes the group's search settings onto the request, so the
 * group is looked up here the same way, in every WLM mode, and with the same precedence
 * ({@link #effectiveCancelAfterTimeInterval}). For a cancellation timeout that came from the request, both therefore choose the
 * same interval, with two exceptions: when the listener's coordinator rejection check throws, the listener writes none of the
 * group's settings while the timer keeps the group's interval; and the listener reads the group after admission, so a change to
 * the group in between reaches only the request.
 *
 * @opensearch.internal
 */
public final class WorkloadGroupSearchCancelAfterTimeIntervalResolver implements SearchCancelAfterTimeIntervalResolver {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSearchCancelAfterTimeIntervalResolver.class);

    private final WorkloadGroupService workloadGroupService;
    private final ThreadPool threadPool;

    public WorkloadGroupSearchCancelAfterTimeIntervalResolver(WorkloadGroupService workloadGroupService, ThreadPool threadPool) {
        this.workloadGroupService = workloadGroupService;
        this.threadPool = threadPool;
    }

    @Override
    public TimeValue resolve(Task task, SearchRequest request, @Nullable TimeValue requested) {
        if (task instanceof WorkloadGroupTask workloadGroupTask) {
            // Idempotent: TransportSearchAction#executeRequest assigns the group again, from the same thread context.
            workloadGroupTask.setWorkloadGroupId(threadPool.getThreadContext());
        }
        try {
            WorkloadGroup workloadGroup = workloadGroupService.getCurrentWorkloadGroup();
            if (workloadGroup == null) {
                return requested;
            }
            Settings wlmSettings = workloadGroup.getSettings();
            // override_request_values only matters for a group that sets an interval, so it is not read for any other group.
            if (wlmSettings == null || wlmSettings.hasValue(WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey()) == false) {
                return requested;
            }
            boolean overrideRequestValues = WorkloadGroupSearchSettings.WLM_OVERRIDE_REQUEST_VALUES.get(wlmSettings);
            return effectiveCancelAfterTimeInterval(wlmSettings, overrideRequestValues, requested);
        } catch (Exception e) {
            // The listener then leaves the request's interval unchanged as well.
            logger.error("Failed to apply workload group setting [search.cancel_after_time_interval]", e);
            return requested;
        }
    }

    /**
     * The {@code search.cancel_after_time_interval} a search runs with in a workload group whose search settings are
     * {@code wlmSettings}. By default the request's own interval wins and the group's applies only when the request set none;
     * with {@code override_request_values} the group's wins. A group without the setting leaves the request's interval.
     *
     * @param wlmSettings           the workload group's search settings
     * @param overrideRequestValues the group's {@code override_request_values}
     * @param requested             the request's interval, or {@code null} if it set none
     * @return the group's interval, or {@code requested} when the group's does not apply
     * @throws IllegalArgumentException if the group's interval cannot be parsed
     */
    @Nullable
    public static TimeValue effectiveCancelAfterTimeInterval(
        Settings wlmSettings,
        boolean overrideRequestValues,
        @Nullable TimeValue requested
    ) {
        if (wlmSettings.hasValue(WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey()) == false) {
            return requested;
        }
        TimeValue cancelAfter = WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.get(wlmSettings);
        return overrideRequestValues || requested == null ? cancelAfter : requested;
    }
}
