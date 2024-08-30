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
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.QueryGroupTask;

public class WorkloadManagementRequestFailureListener extends SearchRequestOperationsListener {
    private final QueryGroupService queryGroupService;
    private final ThreadPool threadPool;

    public WorkloadManagementRequestFailureListener(QueryGroupService queryGroupService, ThreadPool threadPool) {
        this.queryGroupService = queryGroupService;
        this.threadPool = threadPool;
    }

    @Override
    protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        final String queryGroupId = threadPool.getThreadContext().getHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER);
        queryGroupService.incrementFailuresFor(queryGroupId);
    }
}
