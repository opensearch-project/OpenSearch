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

/**
 * Applies the {@code search.cancel_after_time_interval} of a search's workload group, in every WLM mode and with the precedence
 * of the group's other search settings ({@link #effectiveCancelAfterTimeInterval}), so the search actions depend only on the core
 * {@link SearchCancelAfterTimeIntervalResolver}. This is the only place the group's interval is applied: the coordinator arms the
 * search's cancellation timer before admission, before {@code WorkloadGroupRequestOperationListener} applies the group's other
 * search settings, so {@code TransportSearchAction} writes this interval onto the request itself.
 *
 * @opensearch.internal
 */
public final class WorkloadGroupSearchCancelAfterTimeIntervalResolver implements SearchCancelAfterTimeIntervalResolver {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupSearchCancelAfterTimeIntervalResolver.class);

    private final WorkloadGroupService workloadGroupService;

    public WorkloadGroupSearchCancelAfterTimeIntervalResolver(WorkloadGroupService workloadGroupService) {
        this.workloadGroupService = workloadGroupService;
    }

    @Override
    public TimeValue resolve(Task task, SearchRequest request, @Nullable TimeValue requested) {
        try {
            // The group named in the search's thread context.
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
    static TimeValue effectiveCancelAfterTimeInterval(Settings wlmSettings, boolean overrideRequestValues, @Nullable TimeValue requested) {
        if (wlmSettings.hasValue(WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey()) == false) {
            return requested;
        }
        TimeValue cancelAfter = WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.get(wlmSettings);
        return overrideRequestValues || requested == null ? cancelAfter : requested;
    }
}
