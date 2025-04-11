/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;

/**
 * This listener is used to listen for request lifecycle events for a queryGroup
 */
public class WorkloadGroupRequestOperationListener extends SearchRequestOperationsListener {

    private final WorkloadGroupService queryGroupService;
    private final ThreadPool threadPool;

    public WorkloadGroupRequestOperationListener(WorkloadGroupService queryGroupService, ThreadPool threadPool) {
        this.queryGroupService = queryGroupService;
        this.threadPool = threadPool;
    }

    /**
     * This method assumes that the queryGroupId is already populated in the thread context
     * @param searchRequestContext SearchRequestContext instance
     */
    @Override
    protected void onRequestStart(SearchRequestContext searchRequestContext) {
        final String queryGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        queryGroupService.rejectIfNeeded(queryGroupId);
    }

    @Override
    protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        final String queryGroupId = threadPool.getThreadContext().getHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER);
        queryGroupService.incrementFailuresFor(queryGroupId);
    }
}
