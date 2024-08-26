/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.QueryGroupTask;

import java.util.Optional;

/**
 * This listener is used to perform the rejections for incoming requests into a queryGroup
 */
public class QueryGroupRequestRejectionOperationListener extends SearchRequestOperationsListener {

    private final QueryGroupService queryGroupService;
    private final ThreadPool threadPool;

    public QueryGroupRequestRejectionOperationListener(QueryGroupService queryGroupService, ThreadPool threadPool) {
        this.queryGroupService = queryGroupService;
        this.threadPool = threadPool;
    }

    /**
     * This method assumes that the queryGroupId is already populated in the {@link ThreadContext}
     * @param searchRequestContext SearchRequestContext instance
     */
    @Override
    protected void onRequestStart(SearchRequestContext searchRequestContext) {
        final String queryGroupId = threadPool.getThreadContext().getHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER);
        Optional<String> reason = queryGroupService.shouldRejectFor(queryGroupId);
        if (reason.isPresent()) {
            throw new OpenSearchRejectedExecutionException("QueryGroup " + queryGroupId + " is already contended." + reason.get());
        }
    }
}
