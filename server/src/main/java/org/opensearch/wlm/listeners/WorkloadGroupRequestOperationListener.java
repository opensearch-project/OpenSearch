/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WorkloadGroupSearchSettings;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;

/**
 * This listener is used to listen for request lifecycle events for a workloadGroup
 */
public class WorkloadGroupRequestOperationListener extends SearchRequestOperationsListener {

    private static final Logger logger = LogManager.getLogger(WorkloadGroupRequestOperationListener.class);
    private final WorkloadGroupService workloadGroupService;
    private final ThreadPool threadPool;

    public WorkloadGroupRequestOperationListener(WorkloadGroupService workloadGroupService, ThreadPool threadPool) {
        this.workloadGroupService = workloadGroupService;
        this.threadPool = threadPool;
    }

    /**
     * This method assumes that the workloadGroupId is already populated in the thread context
     * @param searchRequestContext SearchRequestContext instance
     */
    @Override
    protected void onRequestStart(SearchRequestContext searchRequestContext) {
        final String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        workloadGroupService.rejectIfNeeded(workloadGroupId);
        applyWorkloadGroupSearchSettings(workloadGroupId, searchRequestContext.getRequest());
    }

    @Override
    protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        final String workloadGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        workloadGroupService.incrementFailuresFor(workloadGroupId);
    }

    /**
     * Applies workload group-specific search settings to the search request.
     * Settings are only applied for workload groups that exist in cluster state.
     * <p>
     * When {@code override_request_values} is {@code false} (default), WLM settings are only
     * applied when the request does not already have an explicit value set.
     * When {@code true}, WLM settings always take precedence over request-level values.
     *
     * @param workloadGroupId the workload group identifier from thread context
     * @param searchRequest the search request to modify
     */
    private void applyWorkloadGroupSearchSettings(String workloadGroupId, SearchRequest searchRequest) {
        if (workloadGroupId == null) {
            return;
        }

        WorkloadGroup workloadGroup = workloadGroupService.getWorkloadGroupById(workloadGroupId);
        if (workloadGroup == null) {
            return;
        }

        Settings wlmSettings = workloadGroup.getSettings();
        if (wlmSettings == null || wlmSettings.isEmpty()) {
            return;
        }

        boolean overrideRequestValues = WorkloadGroupSearchSettings.WLM_OVERRIDE_REQUEST_VALUES.get(wlmSettings);

        applyTimeout(wlmSettings, searchRequest, overrideRequestValues);
        applyCancelAfterTimeInterval(wlmSettings, searchRequest, overrideRequestValues);
        applyMaxConcurrentShardRequests(wlmSettings, searchRequest, overrideRequestValues);
        applyBatchedReduceSize(wlmSettings, searchRequest, overrideRequestValues);
    }

    private void applyTimeout(Settings wlmSettings, SearchRequest searchRequest, boolean overrideRequestValues) {
        if (wlmSettings.hasValue(WorkloadGroupSearchSettings.WLM_SEARCH_TIMEOUT.getKey()) == false) {
            return;
        }
        try {
            TimeValue timeout = WorkloadGroupSearchSettings.WLM_SEARCH_TIMEOUT.get(wlmSettings);
            if (searchRequest.source() == null) {
                return;
            }
            if (overrideRequestValues || searchRequest.source().timeout() == null) {
                searchRequest.source().timeout(timeout);
            }
        } catch (Exception e) {
            logger.error("Failed to apply workload group setting [search.default_search_timeout]", e);
        }
    }

    private void applyCancelAfterTimeInterval(Settings wlmSettings, SearchRequest searchRequest, boolean overrideRequestValues) {
        if (wlmSettings.hasValue(WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.getKey()) == false) {
            return;
        }
        try {
            TimeValue cancelAfter = WorkloadGroupSearchSettings.WLM_CANCEL_AFTER_TIME_INTERVAL.get(wlmSettings);
            if (overrideRequestValues || searchRequest.getCancelAfterTimeInterval() == null) {
                searchRequest.setCancelAfterTimeInterval(cancelAfter);
            }
        } catch (Exception e) {
            logger.error("Failed to apply workload group setting [search.cancel_after_time_interval]", e);
        }
    }

    private void applyMaxConcurrentShardRequests(Settings wlmSettings, SearchRequest searchRequest, boolean overrideRequestValues) {
        if (wlmSettings.hasValue(WorkloadGroupSearchSettings.WLM_MAX_CONCURRENT_SHARD_REQUESTS.getKey()) == false) {
            return;
        }
        try {
            int maxConcurrent = WorkloadGroupSearchSettings.WLM_MAX_CONCURRENT_SHARD_REQUESTS.get(wlmSettings);
            // Raw value 0 means not explicitly set by the user
            if (overrideRequestValues || searchRequest.getMaxConcurrentShardRequestsRaw() == 0) {
                searchRequest.setMaxConcurrentShardRequests(maxConcurrent);
            }
        } catch (Exception e) {
            logger.error("Failed to apply workload group setting [search.max_concurrent_shard_requests]", e);
        }
    }

    private void applyBatchedReduceSize(Settings wlmSettings, SearchRequest searchRequest, boolean overrideRequestValues) {
        if (wlmSettings.hasValue(WorkloadGroupSearchSettings.WLM_BATCHED_REDUCE_SIZE.getKey()) == false) {
            return;
        }
        try {
            int batchedReduceSize = WorkloadGroupSearchSettings.WLM_BATCHED_REDUCE_SIZE.get(wlmSettings);
            // Only apply WLM batched reduce size when the request uses the default value.
            // Note: batchedReduceSize is a primitive int with no sentinel value, so we cannot
            // distinguish between "not set" and "explicitly set to 512 (the default)". If a user
            // explicitly sets batched_reduce_size=512, WLM will still override it when
            // override_request_values is false.
            if (overrideRequestValues || searchRequest.getBatchedReduceSize() == SearchRequest.DEFAULT_BATCHED_REDUCE_SIZE) {
                searchRequest.setBatchedReduceSize(batchedReduceSize);
            }
        } catch (Exception e) {
            logger.error("Failed to apply workload group setting [search.batched_reduce_size]", e);
        }
    }
}
